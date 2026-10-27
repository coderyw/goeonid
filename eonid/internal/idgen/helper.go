// Package idgen
// @Author: yinwei
// @File: helper
// @Version: 1.0.0
// @Date: 2024/9/13 13:57

package idgen

import (
	"fmt"
	"sync"

	"github.com/coderyw/goeonid/v2/eonid/internal/log"
	idgen "github.com/coderyw/goeonid/v2/eonid/internal/woker"
	"github.com/coderyw/goeonid/v2/eonid/option"
	"github.com/redis/go-redis/v9"
)

var s *SnowflakeManager

type SnowflakeManager struct {
	idMap          sync.Map //map[string]*DefaultIdGenerator
	defaultGen     *DefaultIdGenerator
	singletonMutex sync.RWMutex
	rmg            idgen.RenewalMgr
}

func init() {
	s = &SnowflakeManager{
		idMap: sync.Map{}, //make(map[string]*DefaultIdGenerator),
	}
}

//var idGenerator *DefaultIdGenerator

//var idMap = make(map[string]*DefaultIdGenerator)
//
//var singletonMutex sync.RWMutex

// 初始化生成器
func InitIdGenerator(options *option.IdGeneratorOptions, redisClient redis.UniversalClient) error {
	var err error
	log.InitLogger(options.ServerName)

	s.singletonMutex.Lock()
	defer s.singletonMutex.Unlock()
	// 初始化续约个管理器
	s.rmg = idgen.NewRenewalMgr(redisClient)
	if _, ok := s.idMap.Load(options.DataCenter); ok {
		return fmt.Errorf("数据中心重复:%v", options.DataCenter)
	}

	// dataCenter
	ig, err := NewDefaultIdGenerator(options, redisClient)
	if err != nil {
		return err
	}

	s.idMap.Store(options.DataCenter, ig)

	return err
}

// NextId .
func NextId(idCenter ...string) int64 {
	var c string
	if len(idCenter) > 0 {
		c = idCenter[0]
	}
	if val, ok := s.idMap.Load(c); !ok {
		panic("Please initialize Yitter.IdGeneratorOptions first.")
	} else {
		return val.(*DefaultIdGenerator).NewLong()
	}
}

func Stop() {
	s.idMap.Range(func(key, value any) bool {
		value.(*DefaultIdGenerator).SnowWorker.Stop()
		return true
	})
	s.rmg.Stop()

}
