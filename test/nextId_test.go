// Package test
// @Author: yinwei
// @File: nextId_test
// @Version: 1.0.0
// @Date: 2024/9/13 15:52

package test

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/coderyw/goeonid/v2/eonid"
	"github.com/coderyw/goeonid/v2/eonid/option"
	"github.com/redis/go-redis/v9"
)

var redisClient *redis.Client

func Init() {

	redisClient = redis.NewClient(&redis.Options{
		Addr:         envOr("EONID_TEST_REDIS_ADDR", "127.0.0.1:6379"),
		Password:     os.Getenv("EONID_TEST_REDIS_PASSWORD"),
		DB:           0,
		DialTimeout:  time.Second * 3,
		PoolSize:     20,
		MaxIdleConns: 10,
	})

}

func TestGetTimeFromSnowflakeId(t *testing.T) {
	// 指定时区
	loc := time.Now().Location()
	fmt.Println(eonid.GetTimeFromSnowflakeId(201239129030184965, loc))

	fmt.Println(eonid.GetTimeFromSnowflakeId(73015877775208453, loc))
}

func TestStatsNum(t *testing.T) {
	Init()
	eonid.InitIdGenerator(option.NewDefaultIdGeneratorOptions("server1", "b_user", 123), redisClient)
	mp := make(map[int]int)
	for _ = range 1000 {
		id := eonid.NextId("b_user")
		c := int(id % 10)
		if val, ok := mp[c]; ok {
			mp[c] = val + 1
		} else {
			mp[c] = 1
		}
		time.Sleep(1 * time.Millisecond)
	}
	for k, v := range mp {
		fmt.Println(k, " 占比 %.2f", float64(v)/float64(1000)*100)
	}
}

func TestNextId(t *testing.T) {
	Init()
	tests := []struct {
		name     string
		num      int
		workerId uint16
	}{
		{name: "测试获取100个id", num: 100},
		{name: "测试获取1000个id", num: 1000},
		{name: "自定义workerID=1，测试获取1000个id", num: 1000, workerId: 1},
		{name: "测试获取10000000个id", num: 10000000, workerId: 0},
		{name: "测试获取一直获取个id", num: -1},
		{name: "测试获取一个", num: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := eonid.InitIdGenerator(option.NewDefaultIdGeneratorOptions("server1", "b_user", tt.workerId), redisClient)
			if err != nil {
				t.Error(err.Error())
				return
			}
			ch := make(chan struct{}, 1000)
			var idMap = &sync.Map{}
			wg := sync.WaitGroup{}
			for i := 0; tt.num == -1 || i < tt.num; i++ {
				ch <- struct{}{}
				wg.Add(1)
				go func() {
					defer func() {
						<-ch
						wg.Done()
					}()
					got := eonid.NextId()
					fmt.Println(got)
					if _, ok := idMap.Load(got); ok {
						t.Errorf("生产id包含重复: %v", got)
						panic(fmt.Errorf("重复"))
					} else {
						idMap.Store(got, struct{}{})
					}

					//t.Logf("NextId() = %v", got)
				}()

			}
			wg.Wait()
			eonid.Stop()

		})
	}
}

func TestMultNextId(t *testing.T) {
	Init()
	center := []struct {
		dataCenter string
		workerId   uint16
	}{
		{dataCenter: "center1", workerId: 3},
		{dataCenter: "center2", workerId: 0},
		{dataCenter: "center3", workerId: 0},
		{dataCenter: "center4", workerId: 4},
	}
	for _, tt := range center {
		op := option.NewDefaultIdGeneratorOptions("server1", tt.dataCenter, tt.workerId)
		op.DefaultCenter = 2
		err := eonid.InitIdGenerator(op, redisClient)
		if err != nil {
			t.Error(err.Error())
			return
		}
	}
	tests := []struct {
		name       string
		dataCenter string
		num        int
	}{
		{name: "center1：100", dataCenter: "center1", num: 100},
		{name: "center2：10000", dataCenter: "center2", num: 10000},
		{name: "center3：100", dataCenter: "center3", num: 100},
		{name: "center4：100", dataCenter: "center4", num: 100},
	}
	wg := sync.WaitGroup{}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {

			ch := make(chan struct{}, 300)
			var idMap = &sync.Map{}

			for i := 0; tt.num == -1 || i < tt.num; i++ {
				ch <- struct{}{}
				wg.Add(1)
				go func() {
					defer func() {
						<-ch
						wg.Done()
					}()
					got := eonid.NextId(tt.dataCenter)
					if _, ok := idMap.Load(got); ok {
						t.Errorf("生产id包含重复: %v", got)
						panic(fmt.Errorf("重复"))
					} else {
						idMap.Store(got, struct{}{})
					}

					//t.Logf("NextId() = %v", got)
				}()

			}

		})
	}
	wg.Wait()
	eonid.Stop()
}

func TestMulCenter(t *testing.T) {
	Init()
	centerNum := 101
	//wg := sync.WaitGroup{}
	//var idMap = &sync.Map{}

	for i := 0; i < centerNum; i++ {
		op := option.NewDefaultIdGeneratorOptions("server1", strconv.Itoa(i), 0)
		op.DefaultCenter = 2
		err := eonid.InitIdGenerator(op, redisClient)
		if err != nil {
			t.Error(err.Error())
			return
		}
		go func(i int) {
			for {
				got := eonid.NextId(strconv.Itoa(i))
				t.Logf("NextId(%v) = %v", i, got)
				time.Sleep(20 * time.Millisecond)
			}

		}(i)
	}
	sig := []os.Signal{syscall.SIGINT, syscall.SIGTERM}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, sig...)
	<-signalChan
	return

}

// envOr 读取环境变量，未设置时返回默认值；测试用的 Redis 地址和密码不写进代码
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
