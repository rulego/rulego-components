package redis

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

// 注册节点
func init() {
	_ = rulego.Registry.Register(&PublisherNode{})
}

// KeyResult 接收到消息的订阅者数量
const KeyResult = "result"

// PublisherNodeConfiguration 节点配置
type PublisherNodeConfiguration struct {
	// Server redis服务器地址
	Server string
	// Password 密码
	Password string
	// PoolSize 连接池大小
	PoolSize int
	// Db 数据库index
	Db int
	// Channel 发布频道
	// 支持${metadata.key}占位符读取metadata元数据
	Channel string
}

// PublisherNode redis发布节点
// 成功：转向Success链，通过msg.metadata.result获取接收到消息的订阅者数量
// 失败：转向Failure链
type PublisherNode struct {
	base.SharedNode[*redis.Client]
	//节点配置
	Config          PublisherNodeConfiguration
	client          *redis.Client
	channelTemplate str.Template
}

// Type 返回组件类型
func (x *PublisherNode) Type() string {
	return "x/redisPub"
}

func (x *PublisherNode) New() types.Node {
	return &PublisherNode{Config: PublisherNodeConfiguration{
		Server:  "127.0.0.1:6379",
		Channel: "default",
		Db:      0,
	}}
}

// Init 初始化组件
func (x *PublisherNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		//初始化客户端
		_ = x.SharedNode.Init(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*redis.Client, error) {
			return x.initClient()
		})
		x.channelTemplate = str.NewTemplate(strings.TrimSpace(x.Config.Channel))
	}
	return err
}

// OnMsg 处理消息
func (x *PublisherNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	var channel = x.channelTemplate.Execute(evn)
	client, err := x.SharedNode.Get()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// 发布消息到Redis
	result, err := client.Publish(ctx.GetContext(), channel, msg.Data).Result()
	if err != nil {
		ctx.TellFailure(msg, err)
	} else {
		msg.Metadata.PutValue(KeyResult, str.ToString(result))
		ctx.TellSuccess(msg)
	}
}

// Destroy 销毁组件
func (x *PublisherNode) Destroy() {
	if x.client != nil {
		_ = x.client.Close()
	}
}

func (x *PublisherNode) initClient() (*redis.Client, error) {
	if x.client != nil {
		return x.client, nil
	} else {
		x.Locker.Lock()
		defer x.Locker.Unlock()
		if x.client != nil {
			return x.client, nil
		}
		x.client = redis.NewClient(&redis.Options{
			Addr:     x.Config.Server,
			PoolSize: x.Config.PoolSize,
			DB:       x.Config.Db,
			Password: x.Config.Password,
		})
		return x.client, x.client.Ping(context.Background()).Err()
	}
}
