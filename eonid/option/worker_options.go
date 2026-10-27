package option

const DefaultBaseTime = 1725960863000

type IdGeneratorOptions struct {
	DisableHashTag    bool   // true 使用旧版 eonId 前缀；默认 false 使用 {eonId}
	ServerName        string // 服务名
	DataCenter        string // 数据中心，隔离worker使用
	BaseTime          int64  // 基础时间（ms单位），不能超过当前系统时间 41bit
	WorkerId          uint16 // 机器码，必须由外部设定，最大值 2^WorkerIdBitLength-1
	WorkerIdBitLength byte   // 机器码位长，默认值10，取值范围 [1, 15]（要求：序列数位长+机器码位长不超过22）
	SeqBitLength      byte   // 序列数位长，默认值12，取值范围 [3, 21]（要求：序列数位长+机器码位长不超过22）
	MaxSeqNumber      uint32 // 最大序列数（含），设置范围 [MinSeqNumber, 2^SeqBitLength-1]，默认值0，表示最大序列数取最大值（2^SeqBitLength-1]）
	MinSeqNumber      uint32 // 最小序列数（含），默认值5，取值范围 [5, MaxSeqNumber]，每毫秒的前5个序列数对应编号0-4是保留位，其中1-4是时间回拨相应预留位，0是手工新值预留位
	TopOverCostCount  uint32 // 最大漂移次数（含），默认2000，推荐范围500-10000（与计算能力有关）

	DefaultCenter uint8 //默认数据中心 1是 2否，默认1
}

// NewDefaultIdGeneratorOptions
//
//	 @Description: 获取默认雪花id配置
//			配置会指导生成默认的数据中心，如果需要配置其他的数据中心，请设置DefaultCenter=2
//	 @Author	yw 2024-09-14 10:00:06
//	 @param	ServerName
//	 @param	dataCenter
//	 @param	workerId
//	 @return	*IdGeneratorOptions
func NewDefaultIdGeneratorOptions(ServerName, dataCenter string, workerId uint16) *IdGeneratorOptions {
	return &IdGeneratorOptions{
		ServerName:        ServerName,
		DataCenter:        dataCenter,
		WorkerId:          workerId,
		BaseTime:          DefaultBaseTime,
		WorkerIdBitLength: 10,
		SeqBitLength:      12,
		MaxSeqNumber:      0,
		MinSeqNumber:      5,
		TopOverCostCount:  2000,
		DefaultCenter:     1,
	}
}
