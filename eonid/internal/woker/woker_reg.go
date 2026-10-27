// Package idgen
// @Author: yinwei
// @File: woker_reg
// @Version: 1.0.0
// @Date: 2024/9/13 14:19

package idgen

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coderyw/goeonid/v2/eonid/internal/log"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type WorkerRegistry interface {
	Register() (uint16, error)
	Stop()
}

type workerRegistry struct {
	// worker区分key
	dataCenter string
	workerId   uint16
	// 最大的workerId
	maxWorkerId int
	// 默认前缀key
	pk string
	// 续约通知停止信号
	stop chan struct{}
	// 当前workerKey
	workerKey string
	// redis的客户端
	redisClient redis.UniversalClient
	// 注册worker的服务名
	serverName string
	// 是否已经关闭
	isClose atomic.Bool
	info    string
	regLua  *redis.Script
	// 出错重试间隔
	retryInterval time.Duration
}

func NewWorkerRegistry(serverName, dataCenter string, maxWorkerId int, redisClient redis.UniversalClient, workerId uint16, disableHashTag bool) WorkerRegistry {
	prefix := "{eonId}"
	if disableHashTag {
		prefix = "eonId"
	}
	wr := &workerRegistry{
		// 默认使用 hash tag；兼容模式保持旧版注册池的 key
		pk:            prefix,
		dataCenter:    dataCenter,
		serverName:    serverName,
		maxWorkerId:   maxWorkerId,
		workerId:      workerId,
		stop:          make(chan struct{}),
		redisClient:   redisClient,
		retryInterval: time.Second * 3,
		// 注册 lua 脚本
		regLua: redis.NewScript(` 
			-- KEYS[1]: 计数器 key (eonIdKey)
            -- ARGV[1]: maxWorkerId
            
            -- 原子性地递增计数器
            local workId = redis.call('INCR', KEYS[1])
            
            -- 如果超过最大 worker id，重置计数器并返回 -1
            if workId > tonumber(ARGV[1]) then
                redis.call('SET', KEYS[1], -1)
                return -1
            end
            
            -- 返回获取到的 workId
            return workId`,
		),
	}

	return wr
}

// Register
//
//	@Description: 注册worker节点，注册id 范围0~maxWorkerId
//	@Author	yw 2024-09-13 14:33:15
//	@param	redisClient
func (wr *workerRegistry) Register() (uint16, error) {
	maxInterCount := wr.maxWorkerId + 1
	currInterCount := 0

	data := strings.Builder{}
	data.WriteString(uuid.NewString())
	data.WriteString("|")
	data.WriteString(wr.serverName)
	ip, err := getIp()
	if err != nil {
		log.Logger.Warn(log.MsgFmt("获取ip地址失败"), zap.Error(err))
	}
	data.WriteString("|")
	data.WriteString(ip)
	data.WriteString("|")
	// 遍历workerId节点，准备去注册
	for currInterCount < maxInterCount {
		b1 := strings.Builder{}
		b1.WriteString(data.String())
		// 获取 workderId 的计数 key
		counterKey := wr.getWorkerParentKey()
		// 通过 lua 脚本获取下一个节点
		result, err := wr.regLua.Run(context.Background(), wr.redisClient, []string{counterKey}, wr.maxWorkerId).Int()
		if err != nil {
			log.Logger.Error(log.MsgFmt("获取 workerId 失败"), zap.Any("key", counterKey), zap.Any("maxWorkerId", wr.maxWorkerId), zap.Error(err))
			time.Sleep(wr.retryInterval)
			continue
		}
		if result == -1 || result > wr.maxWorkerId || result < 0 {
			log.Logger.Error(log.MsgFmt("获取 workerId 失败"), zap.Any("result", result), zap.Any("maxWorkerId", wr.maxWorkerId))
			time.Sleep(wr.retryInterval)
			continue
		}
		// 获取workerkey
		workerKey := wr.getWorkerKey(result)
		// 准备workerInfo
		b1.WriteString(time.Now().Format(time.DateTime))
		// 尝试注册
		ok, err := wr.redisClient.SetNX(context.Background(), workerKey, b1.String(), workerExpireSec*time.Second).Result()
		if err != nil {
			log.Logger.Error(log.MsgFmt("注册雪花id的worker失败"), zap.Any("workerId", workerKey), zap.Any("maxWorkerId", wr.maxWorkerId))
			time.Sleep(wr.retryInterval)
			continue
		}
		if ok { // 注册成功
			wr.workerKey = workerKey
			wr.info = b1.String()
			wr.workerId = uint16(result)
			log.Logger.Info("注册雪花id的worker成功", zap.Any("workerId", workerKey), zap.Any("info", wr.info))
			// 启动协程续约
			wr.keepalive()
			return wr.workerId, nil
		}
		// 注册失败，继续重试
		log.Logger.Warn(log.MsgFmt("注册雪花id的worker失败，继续重试"), zap.Any("workerId", workerKey), zap.Any("maxWorkerId", wr.maxWorkerId))
		time.Sleep(wr.retryInterval)
		currInterCount++
		continue
	}
	log.Logger.Error(log.MsgFmt("注册worker重试次数过多"), zap.Any("maxTry", maxInterCount))
	return 0, errors.New("注册worker节点失败")
}

// 续约
func (wr *workerRegistry) keepalive() {
	// 使用续约管理中心去做
	rmg.addToRenewal(wr)
}

// 获取key
func (wr *workerRegistry) getWorkerKey(workerId int) string {
	if len(wr.dataCenter) > 0 {
		return fmt.Sprintf("%v:%v:worker-id:%v", wr.pk, wr.dataCenter, workerId)
	} else {
		return fmt.Sprintf("%v:worker-id:%v", wr.pk, workerId)
	}
}

func (wr *workerRegistry) getWorkerParentKey() string {
	if len(wr.dataCenter) > 0 {
		return fmt.Sprintf("%v:%v:worker-id", wr.pk, wr.dataCenter)
	} else {
		return fmt.Sprintf("%v:worker-id", wr.pk)
	}
}

// 停止worker
func (wr *workerRegistry) Stop() {
	if wr.isClose.CompareAndSwap(false, true) {
		close(wr.stop)
		wr.delete()
	}
}

// 删除worker key
func (wr *workerRegistry) delete() {
	// 删除key
	// 判断workerKey是否存在，如果存在获取json字符串，然后解析后，判断id字段是否与wr.info中保存的一致，如果一致则删除
	lua := `
local key = KEYS[1]
local field_to_check = ARGV[1]
local value_to_check = ARGV[2]

-- 获取 Redis 中的 JSON 字符串
local json_data = redis.call("GET", key)

-- 如果数据为空，直接返回
if not json_data then
    return 0
end

-- 使用 cjson 库解析 JSON 字符串
local data = cjson.decode(json_data)

-- 检查是否包含指定的 key 且该 key 的值是否匹配
if data[field_to_check] == value_to_check then
    -- 删除 Redis 键
    redis.call("DEL", key)
    return 1  -- 返回 1 表示键被删除
end

-- 不匹配，返回 0
return 0
`
	wr.redisClient.Eval(context.Background(), lua, []string{wr.workerKey}, "id", wr.info)
	log.Logger.Info("删除雪花id的worker成功", zap.Any("key", wr.workerKey), zap.Any("info", wr.info))

}

// 获取ip地址
func getIp() (string, error) {
	// 获取本地所有网络接口
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println("无法获取网络接口:", err)
		return "", err
	}

	// 遍历每个接口，找出其中的IP地址
	for _, addr := range addrs {
		// 检查地址类型是否为 IPNet，并且不为空
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			// 如果是 IPv4 地址且不是本地回环地址，则输出
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String(), nil
			}
		}
	}
	return "", errors.New("无法获取网络接口")
}
