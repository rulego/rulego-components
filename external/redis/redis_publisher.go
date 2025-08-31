package redis

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
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
	Server string `json:"server"`
	// Password 密码
	Password string `json:"password"`
	// PoolSize 连接池大小
	PoolSize int `json:"poolSize"`
	// Db 数据库index
	Db int `json:"db"`
	// Channel 发布频道
	// 支持${metadata.key}占位符读取metadata元数据
	Channel string `json:"channel"`
}

// PublisherNode redis发布节点
// 成功：转向Success链，通过msg.metadata.result获取接收到消息的订阅者数量
// 失败：转向Failure链
type PublisherNode struct {
	base.SharedNode[*redis.Client]
	//节点配置
	Config          PublisherNodeConfiguration
	channelTemplate el.Template
	// hasVar 标识模板是否包含变量
	hasVar bool
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
		_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*redis.Client, error) {
			return x.initClient()
		}, func(client *redis.Client) error {
			// 清理回调函数
			return client.Close()
		})
		x.channelTemplate, err = el.NewTemplate(strings.TrimSpace(x.Config.Channel))
		if err != nil {
			return err
		}
		// 检查模板是否包含变量
		x.hasVar = x.channelTemplate.HasVar()
	}
	return err
}

// OnMsg 处理消息
func (x *PublisherNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	var channel string
	if x.hasVar {
		evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
		channel = x.channelTemplate.ExecuteAsString(evn)
	} else {
		channel = x.Config.Channel
	}
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// 发布消息到Redis
	result, err := client.Publish(ctx.GetContext(), channel, msg.GetData()).Result()
	if err != nil {
		ctx.TellFailure(msg, err)
	} else {
		msg.Metadata.PutValue(KeyResult, str.ToString(result))
		ctx.TellSuccess(msg)
	}
}

func (x *PublisherNode) Destroy() {
	_ = x.SharedNode.Close()
}

func (x *PublisherNode) initClient() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     x.Config.Server,
		PoolSize: x.Config.PoolSize,
		DB:       x.Config.Db,
		Password: x.Config.Password,
	})
	err := client.Ping(context.Background()).Err()
	return client, err
}
