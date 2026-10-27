package idgen

import (
	"context"
	"sync"
	"time"

	"github.com/coderyw/goeonid/v2/eonid/internal/log"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	// 数据中心 key 过期时间，单位 s
	workerExpireSec = 10
	renewalTick     = 3 * time.Second
)

var (
	rmg  *renewalMgr
	once sync.Once
)

type RenewalMgr interface {
	Stop()
}

// 续约管理器
type renewalMgr struct {
	redisClient redis.UniversalClient
	wrM         sync.Map
	renewalLua  *redis.Script
	renewalNum  int // 每次续约数量
	stop        chan struct{}
}

func NewRenewalMgr(redisClient redis.UniversalClient) RenewalMgr {
	once.Do(func() {
		rmg = &renewalMgr{
			redisClient: redisClient,
			renewalNum:  100,
			stop:        make(chan struct{}),
			renewalLua: redis.NewScript(`
-- batch_expire.lua
-- KEYS: 需要续约的 key
-- ARGV[1]: ttl 秒

local ttl = tonumber(ARGV[1])
local renewed = 0

for i = 1, #KEYS do
    if redis.call("EXISTS", KEYS[i]) == 1 then
        redis.call("EXPIRE", KEYS[i], ttl)
        renewed = renewed + 1
    end
end

return renewed
`),
		}
		go rmg.keepalive()
	})
	return rmg
}

func (rm *renewalMgr) Stop() {

}

// 添加到续约管理器
func (rm *renewalMgr) addToRenewal(wr *workerRegistry) {
	rm.wrM.Store(wr.workerKey, wr)
	// todo // 移除续约数据
}

func (rm *renewalMgr) rmFromRenewal(wr *workerRegistry) {
	rm.wrM.Delete(wr.workerKey)
	// todo 删除 redis 的 key（要基于原始 key）
}

// 续约
func (rm *renewalMgr) keepalive() {
	t := time.NewTicker(renewalTick)
	var before time.Time
	defer t.Stop()
	for {
		select {
		case <-rm.stop:
			log.Logger.Info("续约中心停止")
			return
		case <-t.C:
			// 续约
			rm.startRenewal()
			// 每隔一小时打印一下
			now := time.Now()
			if now.Sub(before) >= time.Hour {
				before = now
				log.Logger.Info("续约雪花id的worker成功")
			}
		}
	}
}

// 开始续约
func (rm *renewalMgr) startRenewal() {
	keys := make([]string, 0, rm.renewalNum)
	legacyKeys := make([]string, 0, rm.renewalNum)

	// 批量去续约，每次 100个 key。防止阻塞
	rm.wrM.Range(func(key, value any) bool {
		wr := value.(*workerRegistry)
		// 旧版 key 可以跨 slot：使用单 key EXPIRE，通过普通 Pipeline 批量发送。
		if wr.pk != "{eonId}" {
			legacyKeys = append(legacyKeys, wr.workerKey)
			if len(legacyKeys) >= rm.renewalNum {
				rm.doPipelineRenewal(legacyKeys)
				legacyKeys = legacyKeys[:0]
			}
			return true
		}
		keys = append(keys, wr.workerKey)
		if len(keys) >= rm.renewalNum {
			rm.doRenewal(keys)
			keys = keys[:0]
		}
		return true
	})
	// 处理剩余的
	if len(keys) > 0 {
		rm.doRenewal(keys)
	}
	if len(legacyKeys) > 0 {
		rm.doPipelineRenewal(legacyKeys)
	}
}

// 普通 Pipeline 由 ClusterClient 按节点路由，不要求整批 key 属于同一 slot。
func (rm *renewalMgr) doPipelineRenewal(keys []string) {
	if len(keys) == 0 {
		return
	}
	ctx := context.Background()
	pipe := rm.redisClient.Pipeline()
	commands := make([]*redis.BoolCmd, 0, len(keys))
	for _, key := range keys {
		// EXPIRE 原子更新 TTL；key 不存在时返回 false，不会重新创建注册锁。
		commands = append(commands, pipe.Expire(ctx, key, workerExpireSec*time.Second))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		log.Logger.Error("批量续约雪花id的worker失败", zap.Error(err))
	}
	// Exec 只返回首个错误，仍需检查每条命令，包括 key 已过期时返回的 false。
	for i, cmd := range commands {
		renewed, err := cmd.Result()
		if err != nil {
			log.Logger.Error("续约雪花id的worker失败", zap.String("key", keys[i]), zap.Error(err))
		} else if !renewed {
			log.Logger.Error("续约雪花id的worker未找到注册锁", zap.String("key", keys[i]))
		}
	}
}

// 执行续约
func (rm *renewalMgr) doRenewal(keys []string) {
	err := rm.renewalLua.Run(context.Background(), rm.redisClient, keys, workerExpireSec).Err()
	if err != nil {
		log.Logger.Error("续约雪花id的worker失败", zap.Any("info", keys), zap.Error(err))
	}
}
