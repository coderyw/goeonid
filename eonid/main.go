// Package eonid
// @Author: yinwei
// @File: main
// @Version: 1.0.0
// @Date: 2024/9/13 15:42

package eonid

import (
	"time"

	"github.com/coderyw/goeonid/v2/eonid/internal/idgen"
	"github.com/coderyw/goeonid/v2/eonid/option"
	"github.com/redis/go-redis/v9"
)

// NextId 获取雪花id
//
// Parameters:
//   - idCenter: ...string 如果传递了数据中心名称，则从该数据中心获取id，否则从默认数据中心获取（前提是配置了默认数据中心）
//
// Returns:
//   - int64 雪花id
func NextId(idCenter ...string) int64 {
	return idgen.NextId(idCenter...)
}

// InitIdGenerator 初始化id生成中心
//
//	支持生成多个datacenter，根据dataCenter名称区分。同时支持生成一个默认的datacenter，通过配置DefaultCenter控制
//
// Parameters:
//   - options: *option.IdGeneratorOptions 选项配置
//   - redisClient: redis.UniversalClient redis客户端
//
// Returns:
//   - error
func InitIdGenerator(options *option.IdGeneratorOptions, redisClient redis.UniversalClient) error {
	return idgen.InitIdGenerator(options, redisClient)
}

func Stop() {
	idgen.Stop()
}

// GetTimeFromSnowflakeId 获取雪花id的生成时间（精确到毫秒）
//
// Parameters:
//   - id: int64 雪花id
//   - loc: *time.Location 时区，不传时默认为当前机器运行所在时区
//
// Returns:
//   - time.Time 雪花id生成时间
func GetTimeFromSnowflakeId(id int64, loc ...*time.Location) time.Time {
	timestamp := id>>22 + option.DefaultBaseTime
	t := time.UnixMilli(timestamp)

	if len(loc) > 0 {
		t = t.In(loc[0])
	}
	return t
}

// GetIdInTimestamp 获取秒时间戳下的最小或最大雪花 id
//
// Parameters:
//   - timestamp int64 时间戳 秒 注意时间戳不能小于 option.DefaultBaseTime 基础时间戳
//   - maxId bool 是否获取最大 id。true=获取
//
// Returns：
//   - int64
func GetIdInTimestamp(timestampS int64, maxId ...bool) int64 {
	if len(maxId) > 0 && maxId[0] {
		// 获取时间戳下最大的 id
		return (((timestampS+1)*1000 - option.DefaultBaseTime) << 22) - 1
	}
	return (timestampS*1000 - option.DefaultBaseTime) << 22
}

// now- base = tick
// tick<<22
