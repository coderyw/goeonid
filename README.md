# goeonid

雪花id，golang语言实现

## 安装

```sh
go get github.com/coderyw/goeonid/v2@v2.1.0
```

由 eonid-go `v2.1.0-rc.2` 复制而来，代码一致，只改了模块路径。

集成测试（`test/`、`eonid/internal/woker` 中的部分用例）需要 Redis，通过环境变量配置：
`EONID_TEST_REDIS_ADDR`（默认 `127.0.0.1:6379`）、`EONID_TEST_REDIS_PASSWORD`。

## Redis hash tag 兼容开关

`IdGeneratorOptions.DisableHashTag` 默认 `false`，使用 `{eonId}` 前缀。
NS 与旧版服务混合部署时设置为 `true`，使用旧版 `eonId` 前缀：

```go
op := option.NewDefaultIdGeneratorOptions("serverName", "b_user", 0)
op.DisableHashTag = true
err := eonid.InitIdGenerator(op, redisClient)
```

SDK 不直接读取环境变量。接入层（例如 gomicro-util）应将
`DISABLE_EONID_HASHTAG=0`（默认）映射为 `false`，`1` 映射为 `true`。

- 开启：`{eonId}:b_user:worker-id`、`{eonId}:b_user:worker-id:123`。
- 关闭：`eonId:b_user:worker-id`、`eonId:b_user:worker-id:123`。
- 空数据中心省略 `b_user:` 段。
- 关闭时使用普通 Pipeline，每批最多 100 条单 key EXPIRE，避免 Redis Cluster 跨 slot 和逐 key 串行网络往返；省去 Lua 与 EXISTS 开销，逐条检查错误和 key 不存在的返回值。开启时保留批量 Lua 续租。

配置在注册时确定，不支持运行中切换。共享注册池的新旧服务必须使用相同的
Redis、DB、DataCenter 和兼容的 ID 参数；NS 升级前应统一设置关闭模式。
不要通过复制或删除在线注册锁迁移注册池。

回归测试（不连接 Redis）：

```sh
go test ./eonid/... -run '^TestHashTag' -count=1
```

## 版本说明

### v1.1.2 待发布

- 增加从雪花id获取生成雪花id时间的方法`eonid.GetTimeFromSnowflakeId`

### v1.1.1

- 修复雪花id在不同毫秒获取时候都是奇数开始

## 参数介绍- options

```
type IdGeneratorOptions struct {
	ServerName        string // 服务名
	DataCenter        string // 数据中心，隔离worker使用
	BaseTime          int64  // 基础时间（ms单位），不能超过当前系统时间 41bit
	WorkerId          uint16 // 机器码，必须由外部设定，最大值 2^WorkerIdBitLength-1
	WorkerIdBitLength byte   // 机器码位长，默认值10，取值范围 [1, 15]（要求：序列数位长+机器码位长不超过22）
	SeqBitLength      byte   // 序列数位长，默认值12，取值范围 [3, 21]（要求：序列数位长+机器码位长不超过22）
	MaxSeqNumber      uint32 // 最大序列数（含），设置范围 [MinSeqNumber, 2^SeqBitLength-1]，默认值0，表示最大序列数取最大值（2^SeqBitLength-1]）
	MinSeqNumber      uint32 // 最小序列数（含），默认值5，取值范围 [5, MaxSeqNumber]，每毫秒的前5个序列数对应编号0-4是保留位，其中1-4是时间回拨相应预留位，0是手工新值预留位
	TopOverCostCount  uint32 // 最大漂移次数（含），默认2000，推荐范围500-10000（与计算能力有关）

	RedisClient *redis.Client // redis客户端
}
```

## 调用

调用方式：

1. 先初始化worker

   - 使用默认配置初始化

     ```
     // serverName: 服务名
     // dataCenter: 用于区分workerId，比如可以使用表名为区分
     // redisClient: redis客户端
     // workerId: 自定义workerId，如果当前已经被占用，会向后寻找可用的。默认为0，从0开始向后寻找
     err := eonid.InitIdGenerator(option.NewDefaultIdGeneratorOptions("serverName", "dataCenter", redisClient, workerId))
     ```

   - 自定义配置初始化

     ```
     // 直接实现 IdGeneratorOptions
     var op := IdGeneratorOptions{}
     
     //初始化
     err := eonid.InitIdGenerator(&op)
     ```

2. 获取下一个id

   ```
   // 请注意如果不初始化workder，这直接调用会panic
   eonid.NextId()
   ```

3. 注销

   ```
   eonid.Stop()
   ```

   
