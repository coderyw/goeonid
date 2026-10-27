package idgen

import (
	"errors"
	"strconv"
	"time"

	idgen2 "github.com/coderyw/goeonid/v2/eonid/internal/woker"
	"github.com/coderyw/goeonid/v2/eonid/option"
	"github.com/redis/go-redis/v9"
)

type DefaultIdGenerator struct {
	Options    *option.IdGeneratorOptions
	SnowWorker idgen2.ISnowWorker
}

func NewDefaultIdGenerator(options *option.IdGeneratorOptions, redisClient redis.UniversalClient) (*DefaultIdGenerator, error) {
	if options == nil {
		panic("dig.Options error.")
	}

	// 1.BaseTime 配置了一个最小值
	minTime := int64(631123200000) // time.Now().AddDate(-30, 0, 0).UnixNano() / 1e6
	if options.BaseTime < minTime || options.BaseTime > time.Now().UnixNano()/1e6 {
		return nil, errors.New("BaseTime error.")
	}

	// 2.WorkerIdBitLength
	if options.WorkerIdBitLength <= 0 {
		return nil, errors.New("WorkerIdBitLength error.(range:[1, 21])")
	}
	if options.WorkerIdBitLength+options.SeqBitLength > 22 {
		return nil, errors.New("error：WorkerIdBitLength + SeqBitLength <= 22")
	}

	// 3.WorkerId
	maxWorkerIdNumber := uint16(1<<options.WorkerIdBitLength) - 1
	if maxWorkerIdNumber == 0 {
		maxWorkerIdNumber = 63
	}
	if options.WorkerId < 0 || options.WorkerId > maxWorkerIdNumber {
		panic("WorkerId error. (range:[0, " + strconv.FormatUint(uint64(maxWorkerIdNumber), 10) + "]")
	}

	options.SeqBitLength = 22 - options.WorkerIdBitLength

	// 4.SeqBitLength
	if options.SeqBitLength < 2 || options.SeqBitLength > 21 {
		return nil, errors.New("SeqBitLength error. (range:[2, 21])")
	}

	// 5.MaxSeqNumber
	maxSeqNumber := uint32(1<<options.SeqBitLength) - 1
	if maxSeqNumber == 0 {
		maxSeqNumber = 63
	}
	if options.MaxSeqNumber < 0 || options.MaxSeqNumber > maxSeqNumber {
		return nil, errors.New("MaxSeqNumber error. (range:[1, " + strconv.FormatUint(uint64(maxSeqNumber), 10) + "]")
	}

	// 6.MinSeqNumber
	if options.MinSeqNumber < 5 || options.MinSeqNumber > maxSeqNumber {
		return nil, errors.New("MinSeqNumber error. (range:[5, " + strconv.FormatUint(uint64(maxSeqNumber), 10) + "]")
	}

	// 7.TopOverCostCount
	if options.TopOverCostCount < 0 || options.TopOverCostCount > 10000 {
		return nil, errors.New("TopOverCostCount error. (range:[0, 10000]")
	}

	if options.DefaultCenter == 0 {
		options.DefaultCenter = 1
	}

	snowWorker, err := idgen2.NewSnowWorkerM1(options, redisClient)
	if err != nil {
		return nil, err
	}

	return &DefaultIdGenerator{
		Options:    options,
		SnowWorker: snowWorker,
	}, nil
}

func (dig DefaultIdGenerator) NewLong() int64 {
	return dig.SnowWorker.NextId()
}

func (dig DefaultIdGenerator) ExtractTime(id int64) time.Time {
	return time.UnixMilli(id>>(dig.Options.WorkerIdBitLength+dig.Options.SeqBitLength) + dig.Options.BaseTime)
}
