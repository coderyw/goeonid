package idgen

import (
	"math/rand"
	"sync"
	"time"

	"github.com/coderyw/goeonid/v2/eonid/internal/log"
	"github.com/coderyw/goeonid/v2/eonid/option"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// SnowWorkerM1 .
type SnowWorkerM1 struct {
	BaseTime          int64  // 基础时间
	WorkerId          int64  // 机器码
	WorkerIdBitLength byte   // 机器码位长
	SeqBitLength      byte   // 自增序列数位长
	MaxSeqNumber      uint32 // 最大序列数（含）
	MinSeqNumber      uint32 // 最小序列数（含）
	TopOverCostCount  uint32 // 最大漂移次数
	_TimestampShift   byte
	_CurrentSeqNumber uint32

	_LastTimeTick           int64
	_TurnBackTimeTick       int64
	_TurnBackIndex          byte
	_IsOverCost             bool
	_OverCostCountInOneTerm uint32
	// _GenCountInOneTerm      uint32
	// _TermIndex              uint32
	reg WorkerRegistry
	sync.Mutex
}

// NewSnowWorkerM1 .
func NewSnowWorkerM1(options *option.IdGeneratorOptions, redisClient redis.UniversalClient) (ISnowWorker, error) {
	var workerIdBitLength byte
	var seqBitLength byte
	var maxSeqNumber uint32

	// 1.BaseTime
	var baseTime int64
	if options.BaseTime != 0 {
		baseTime = options.BaseTime
	} else {
		baseTime = 1725960863000
	}

	// 2.WorkerIdBitLength
	if options.WorkerIdBitLength == 0 {
		workerIdBitLength = 10
	} else {
		workerIdBitLength = options.WorkerIdBitLength
	}

	// 3.WorkerId
	var workerId = options.WorkerId

	// 4.SeqBitLength
	if options.SeqBitLength == 0 {
		seqBitLength = 12
	} else {
		seqBitLength = options.SeqBitLength
	}

	// 5.MaxSeqNumber
	if options.MaxSeqNumber <= 0 {
		maxSeqNumber = (1 << seqBitLength) - 1
	} else {
		maxSeqNumber = options.MaxSeqNumber
	}

	// 6.MinSeqNumber
	var minSeqNumber = options.MinSeqNumber

	// 7.TopOverCostCount
	var topOverCostCount = options.TopOverCostCount

	// 8.Others
	timestampShift := (byte)(workerIdBitLength + seqBitLength)
	currentSeqNumber := minSeqNumber

	// 注册worker
	reg := NewWorkerRegistry(options.ServerName, options.DataCenter, (1<<workerIdBitLength)-1, redisClient, workerId, options.DisableHashTag)
	wkid, err := reg.Register()
	if err != nil {
		return nil, err
	}
	options.WorkerId = wkid
	log.Logger.Info("创建SnowWorkerM1成功", zap.Any("option", options))
	return &SnowWorkerM1{
		BaseTime:                baseTime,
		WorkerIdBitLength:       workerIdBitLength,
		WorkerId:                int64(wkid),
		SeqBitLength:            seqBitLength,
		MaxSeqNumber:            maxSeqNumber,
		MinSeqNumber:            minSeqNumber,
		TopOverCostCount:        topOverCostCount,
		_TimestampShift:         timestampShift,
		_CurrentSeqNumber:       currentSeqNumber,
		reg:                     reg,
		_LastTimeTick:           0,
		_TurnBackTimeTick:       0,
		_TurnBackIndex:          0,
		_IsOverCost:             false,
		_OverCostCountInOneTerm: 0,
		// _GenCountInOneTerm:      0,
		// _TermIndex:              0,
	}, nil
}

// DoGenIdAction .
// todo 实现
func (m1 *SnowWorkerM1) DoGenIdAction(arg *OverCostActionArg) {

}

func (m1 *SnowWorkerM1) BeginOverCostAction(useTimeTick int64) {

}

func (m1 *SnowWorkerM1) EndOverCostAction(useTimeTick int64) {
	// if m1._TermIndex > 10000 {
	// 	m1._TermIndex = 0
	// }
}

func (m1 *SnowWorkerM1) BeginTurnBackAction(useTimeTick int64) {

}

func (m1 *SnowWorkerM1) EndTurnBackAction(useTimeTick int64) {

}

func (m1 *SnowWorkerM1) Stop() {
	m1.reg.Stop()
}

func (m1 *SnowWorkerM1) NextOverCostId(millis int64) int64 {
	log.Logger.Info("NextOverCostId", zap.Any("currentTimeTick", millis))
	currentTimeTick := m1.GetCurrentTimeTick(millis)
	if currentTimeTick > m1._LastTimeTick {
		// m1.EndOverCostAction(currentTimeTick)
		log.Logger.Info("")
		m1._LastTimeTick = currentTimeTick
		m1._CurrentSeqNumber = m1.MinSeqNumber
		m1._IsOverCost = false
		m1._OverCostCountInOneTerm = 0
		// m1._GenCountInOneTerm = 0
		return m1.CalcId(m1._LastTimeTick)
	}
	if m1._OverCostCountInOneTerm >= m1.TopOverCostCount {
		// m1.EndOverCostAction(currentTimeTick)
		m1._LastTimeTick = m1.GetNextTimeTick(millis)
		m1._CurrentSeqNumber = m1.MinSeqNumber
		m1._IsOverCost = false
		m1._OverCostCountInOneTerm = 0
		// m1._GenCountInOneTerm = 0
		return m1.CalcId(m1._LastTimeTick)
	}
	if m1._CurrentSeqNumber > m1.MaxSeqNumber {
		m1._LastTimeTick++
		m1._CurrentSeqNumber = m1.MinSeqNumber
		m1._IsOverCost = true
		m1._OverCostCountInOneTerm++
		// m1._GenCountInOneTerm++

		return m1.CalcId(m1._LastTimeTick)
	}

	// m1._GenCountInOneTerm++
	return m1.CalcId(m1._LastTimeTick)
}

// NextNormalID .
func (m1 *SnowWorkerM1) NextNormalId(cur int64) int64 {
	currentTimeTick := m1.GetCurrentTimeTick(cur)
	if currentTimeTick < m1._LastTimeTick {
		log.Logger.Warn("时间回拨", zap.Any("currentTimeTick", currentTimeTick), zap.Any("last", m1._LastTimeTick))
		if m1._TurnBackTimeTick < 1 {
			m1._TurnBackTimeTick = m1._LastTimeTick - 1
			m1._TurnBackIndex++
			// 每毫秒序列数的前5位是预留位，0用于手工新值，1-4是时间回拨次序
			// 支持4次回拨次序（避免回拨重叠导致ID重复），可无限次回拨（次序循环使用）。
			if m1._TurnBackIndex > 4 {
				m1._TurnBackIndex = 1
			}
			m1.BeginTurnBackAction(m1._TurnBackTimeTick)
		}

		// time.Sleep(time.Duration(1) * time.Millisecond)
		return m1.CalcTurnBackId(m1._TurnBackTimeTick)
	}

	// 时间追平时，_TurnBackTimeTick清零
	if m1._TurnBackTimeTick > 0 {
		m1.EndTurnBackAction(m1._TurnBackTimeTick)
		m1._TurnBackTimeTick = 0
	}

	if currentTimeTick > m1._LastTimeTick {
		//log.Logger.Info("当前tick大于last", zap.Any("currentTimeTick", currentTimeTick), zap.Any("last", m1._LastTimeTick))
		m1._LastTimeTick = currentTimeTick
		// 做随机 seq
		m1._CurrentSeqNumber = uint32(rand.Int31n(2)) + m1.MinSeqNumber

		return m1.CalcId(m1._LastTimeTick)
	}

	if m1._CurrentSeqNumber > m1.MaxSeqNumber {
		log.Logger.Info("雪花id序列号消耗完", zap.Any("currentTimeTick", currentTimeTick))
		m1.BeginOverCostAction(currentTimeTick)
		// m1._TermIndex++

		m1._LastTimeTick++
		m1._CurrentSeqNumber = m1.MinSeqNumber
		m1._IsOverCost = true
		m1._OverCostCountInOneTerm = 1
		// m1._GenCountInOneTerm = 1

		return m1.CalcId(m1._LastTimeTick)
	}

	return m1.CalcId(m1._LastTimeTick)
}

// CalcID .
func (m1 *SnowWorkerM1) CalcId(useTimeTick int64) int64 {
	result := useTimeTick<<m1._TimestampShift | m1.WorkerId<<m1.SeqBitLength | int64(m1._CurrentSeqNumber)
	m1._CurrentSeqNumber++
	return result
}

// CalcTurnBackID .
func (m1 *SnowWorkerM1) CalcTurnBackId(useTimeTick int64) int64 {
	result := useTimeTick<<m1._TimestampShift | m1.WorkerId<<m1.SeqBitLength | int64(m1._TurnBackIndex)
	m1._TurnBackTimeTick--
	return result
}

// GetCurrentTimeTick .
func (m1 *SnowWorkerM1) GetCurrentTimeTick(millis int64) int64 {
	//var millis = time.Now().UnixMilli()
	return millis - m1.BaseTime
}

// GetNextTimeTick .
func (m1 *SnowWorkerM1) GetNextTimeTick(millis int64) int64 {
	tempTimeTicker := m1.GetCurrentTimeTick(millis)
	for tempTimeTicker <= m1._LastTimeTick {
		time.Sleep(time.Duration(1) * time.Millisecond)
		tempTimeTicker = m1.GetCurrentTimeTick(time.Now().UnixMilli())
	}
	return tempTimeTicker
}

// NextId .
func (m1 *SnowWorkerM1) NextId() int64 {

	m1.Lock()
	defer m1.Unlock()
	cur := time.Now().UnixMilli()
	if m1._IsOverCost {
		return m1.NextOverCostId(cur)
	} else {
		return m1.NextNormalId(cur)
	}
}
