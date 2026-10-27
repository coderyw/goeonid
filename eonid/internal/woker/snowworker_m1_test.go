// Package idgen
// @Author: yinwei
// @File: snowworker_m1_test.go
// @Version: 1.0.0
// @Date: 2024/11/25 13:14

package idgen

import (
	"fmt"
	"github.com/coderyw/goeonid/v2/eonid/internal/log"
	"github.com/coderyw/goeonid/v2/eonid/option"
	"github.com/redis/go-redis/v9"
	"math/rand"
	"testing"
	"time"
)

var redisClient *redis.Client

func init() {

	redisClient = redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:6490",
		Password:     "123456",
		DB:           0,
		DialTimeout:  time.Second * 3,
		PoolSize:     20,
		MaxIdleConns: 10,
	})
	log.InitLogger("test")

}
func TestNewSnowWorkerM1(t *testing.T) {

	got, err := NewSnowWorkerM1(option.NewDefaultIdGeneratorOptions("test", "", 1), redisClient)
	if err != nil {
		panic(err)
	}
	for _ = range 10 {
		fmt.Println(got.NextId())
	}

}

func Test234(t *testing.T) {
	got, err := NewSnowWorkerM1(option.NewDefaultIdGeneratorOptions("test", "", 4), redisClient)
	if err != nil {
		panic(err)
	}

	fmt.Println(got.NextId())
}

func TestA(t *testing.T) {
	for i := range 20 {
		fmt.Println(55344050995200000 | (i << 12))
		fmt.Println((i << 12))
		fmt.Println(rand.Int31n(2))
	}
}

func TestD(t *testing.T) {
	fmt.Println(int64(6549838851<<22) | int64(39<<12) | int64(5))
	fmt.Println((6549838851 << 22)) //27472015292104704
	fmt.Println((39 << 12))         //159744
	fmt.Println(int64(6549838851<<22) | int64(23<<12) | int64(5))
}
