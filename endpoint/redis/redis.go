/*
 * Copyright 2024 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package redis

import (
	"context"
	"errors"
	"fmt"
	"net/textproto"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rulego/rulego-components/pkg/statusprobe"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
)

// Type 组件类型
const Type = types.EndpointTypePrefix + "redis"

const (
	// KeyResponseTopic 响应主题metadataKey
	KeyResponseTopic = "responseTopic"
	// KeyResponseChannel 响应主题metadataKey
	KeyResponseChannel = "responseChannel"
)

// Endpoint 别名
type Endpoint = Redis

var _ endpointApi.Endpoint = (*Endpoint)(nil)

// 注册组件
func init() {
	_ = endpoint.Registry.Register(&Endpoint{})
}

// RequestMessage 请求消息
type RequestMessage struct {
	redisClient *redis.Client
	topic       string
	body        []byte
	msg         *types.RuleMsg
	err         error
}

func (r *RequestMessage) Body() []byte {
	return r.body
}

func (r *RequestMessage) Headers() textproto.MIMEHeader {
	header := make(textproto.MIMEHeader)
	header.Set("topic", r.topic)
	header.Set("channel", r.topic)
	return header
}

func (r *RequestMessage) From() string {
	return r.topic
}

func (r *RequestMessage) GetParam(key string) string {
	return ""
}

func (r *RequestMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

func (r *RequestMessage) GetMsg() *types.RuleMsg {
	if r.msg == nil {
		//默认指定是JSON格式，如果不是该类型，请在process函数中修改
		ruleMsg := types.NewMsg(0, r.From(), types.JSON, types.NewMetadata(), string(r.Body()))

		ruleMsg.Metadata.PutValue("topic", r.From())
		ruleMsg.Metadata.PutValue("channel", r.From())

		r.msg = &ruleMsg
	}
	return r.msg
}

func (r *RequestMessage) SetStatusCode(statusCode int) {
}

func (r *RequestMessage) SetBody(body []byte) {
	r.body = body
}

func (r *RequestMessage) SetError(err error) {
	r.err = err
}

func (r *RequestMessage) GetError() error {
	return r.err
}

// ResponseMessage http响应消息
type ResponseMessage struct {
	redisClient *redis.Client
	topic       string
	body        []byte
	msg         *types.RuleMsg
	headers     textproto.MIMEHeader
	err         error
	log         func(format string, v ...interface{})
}

func (r *ResponseMessage) Body() []byte {
	return r.body
}

func (r *ResponseMessage) Headers() textproto.MIMEHeader {
	if r.headers == nil {
		r.headers = make(map[string][]string)
	}
	return r.headers
}

func (r *ResponseMessage) From() string {
	return r.topic
}

func (r *ResponseMessage) GetParam(key string) string {
	return ""
}

func (r *ResponseMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}
func (r *ResponseMessage) GetMsg() *types.RuleMsg {
	return r.msg
}

func (r *ResponseMessage) SetStatusCode(statusCode int) {
}

// 从msg.Metadata或者响应头获取
func (r *ResponseMessage) getMetadataValue(metadataName, headerName string) string {
	var v string
	if r.GetMsg() != nil {
		metadata := r.GetMsg().Metadata
		v = metadata.GetValue(metadataName)
	}
	if v == "" {
		return r.Headers().Get(headerName)
	} else {
		return v
	}
}

func (r *ResponseMessage) SetBody(body []byte) {
	r.body = body
	topic := r.getMetadataValue(KeyResponseTopic, KeyResponseTopic)
	if topic == "" {
		topic = r.getMetadataValue(KeyResponseChannel, KeyResponseChannel)
	}
	if topic != "" {
		if err := r.redisClient.Publish(context.Background(), topic, string(r.body)).Err(); err != nil {
			r.log("redis publish error:%v", err)
		}
	}
}

func (r *ResponseMessage) SetError(err error) {
	r.err = err
}

func (r *ResponseMessage) GetError() error {
	return r.err
}

type Config struct {
	Server   string `json:"server" label:"Server" desc:"Redis server address, format: host:port" required:"true" ref:"primary"`
	Password string `json:"password" label:"Password" desc:"Redis authentication password" ref:"shared"`
	Db       int    `json:"db" label:"DB Index" desc:"Redis database index, default is 0"`
}

// Redis Redis接收端端点
type Redis struct {
	impl.BaseEndpoint
	base.SharedNode[*redis.Client]
	// GracefulShutdown provides graceful shutdown capabilities
	// GracefulShutdown 提供优雅停机功能
	base.GracefulShutdown
	RuleConfig types.Config
	//Config 配置
	Config           Config
	pubSub           *redis.PubSub
	channelRouterMap map[string][]endpointApi.Router
	// demoted 降级标记：订阅重建与降级并发时，安装前复核避免 standby 带订阅消费
	demoted          bool
	// probe 限频 Ping 探测
	probe *statusprobe.Throttled
	// chainId 所属规则链 ID，取自 Init 注入的链定义，参与选主锁键
	chainId string
	// instanceKey 实例标识：配置内容的散列，多副本部署同名端点生成相同键
	instanceKey string
	// guard 多副本选主：leader 订阅消费，待命副本只登记路由不订阅。
	// Pub/Sub 不落盘，主备切换窗口内发布的消息会丢失，无 at-least-once 保证
	guard *types.ActiveGuard
	// guardCancel 停掉选主循环
	guardCancel context.CancelFunc
}

// Type 组件类型
func (x *Redis) Type() string {
	return Type
}

func (x *Redis) Id() string {
	return x.Config.Server
}

func (x *Redis) New() types.Node {
	return &Redis{
		Config: Config{
			Server: "127.0.0.1:6379",
			Db:     0,
		},
	}
}

func (x *Redis) Def() types.ComponentForm {
	return types.ComponentForm{
		Desc: "Redis Pub/Sub endpoint for subscribing to channels and processing messages",
		RouterForm: &types.RouterForm{
			From: &types.RouterFormField{
				Path: types.ComponentFormField{
					Name:     "path",
					Type:     "string",
					Label:    "Channel",
					Desc:     "Redis channel pattern to subscribe, supports glob-style patterns, e.g. orders.*",
					Required: true,
				},
			},
		},
	}
}

// Init 初始化
func (x *Redis) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	x.RuleConfig = ruleConfig

	if def := x.GetRuleChainDefinition(configuration); def != nil {
		x.chainId = def.RuleChain.ID
	}
	// 实例标识优先取端点节点 Id（链内唯一、跨副本一致），无 Id 时回退配置散列，
	// 避免同链内同配置的多端点节点锁键互撞
	x.instanceKey = base.NodeIdOf(configuration)
	if x.instanceKey == "" {
		x.instanceKey = base.ConfigKey(x.Config)
	}
	x.guard = types.NewActiveGuard(ruleConfig,
		types.OnceScope(Type, ruleConfig.Owner, x.chainId, x.instanceKey))

	// 初始化优雅停机功能
	x.GracefulShutdown.InitGracefulShutdown(x.RuleConfig.Logger, 0)

	_ = x.SharedNode.InitWithClose(x.RuleConfig, x.Type(), x.Config.Server, true, func() (*redis.Client, error) {
		return x.initClient()
	}, func(client *redis.Client) error {
		if client != nil {
			return client.Close()
		}
		return nil
	})
	// chainCtx is injected when deployed on a chain: enables chain-scoped ref://
	// resolution (borrowing from same-chain nodes or endpoints) and registers
	// this endpoint's connection for same-chain borrowers
	x.SharedNode.BindChain(configuration)
	x.probe = statusprobe.New()
	return err
}

// ConnectionStatus reports the live redis server state.
func (x *Redis) ConnectionStatus() types.StatusInfo {
	if client, ok := x.SharedNode.Instance(); ok {
		return x.probe.Status(func(ctx context.Context) error { return client.Ping(ctx).Err() })
	}
	return x.SharedNode.ConnectionStatus()
}

// Destroy 销毁
func (x *Redis) Destroy() {
	x.GracefulShutdown.GracefulStop(func() {
		_ = x.Close()
	})
}

// GracefulStop provides graceful shutdown for the Redis endpoint
// GracefulStop 为 Redis 端点提供优雅停机
func (x *Redis) GracefulStop() {
	x.GracefulShutdown.GracefulStop(func() {
		_ = x.Close()
	})
}

func (x *Redis) Close() error {
	if x.guardCancel != nil {
		x.guardCancel()
	}
	// 先销毁父组件，它会清理自己的资源，例如通过CheckAndSetRouterId注册的路由
	x.BaseEndpoint.Destroy()
	// SharedNode 会通过 InitWithClose 中的清理函数来管理客户端的关闭
	// SharedNode manages client closure through the cleanup function in InitWithClose
	_ = x.SharedNode.Close()
	x.Lock()
	defer x.Unlock()

	if x.pubSub != nil {
		_ = x.pubSub.Close()
		x.pubSub = nil
	}
	// 清理channel-router的映射关系
	x.channelRouterMap = nil
	return nil
}

func (x *Redis) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router cannot be nil")
	}
	// 获取或者初始化客户端
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		return "", err
	}
	routerId := x.CheckAndSetRouterId(router)
	if x.checkSubByRouterId(routerId) {
		return routerId, fmt.Errorf("routerId:%s already exists", routerId)
	}
	channels := strings.Split(router.GetFrom().ToString(), ",")
	x.addRouter(router, channels...)
	// 待命副本只登记路由，晋升后由守卫回调统一订阅；订阅失败只记录不报错，
	// 路由登记必须保留
	_ = x.applySubscription(client)
	return routerId, nil
}

// currentChannels 返回当前登记的全部 channel
func (x *Redis) currentChannels() []string {
	x.RLock()
	defer x.RUnlock()
	channels := make([]string, 0, len(x.channelRouterMap))
	for channel := range x.channelRouterMap {
		channels = append(channels, channel)
	}
	return channels
}

// applySubscription 持有租约时按当前登记的 channel 全量重建订阅；
// 待命态不订阅。重建是全量幂等的，晋升回调与 AddRouter 并发到达安全。
// 订阅确认失败返回错误：守卫据此释放租约，下个轮询周期重建。
func (x *Redis) applySubscription(client *redis.Client) error {
	if !x.guard.IsActive() {
		return nil
	}
	x.Lock()
	x.demoted = false
	x.Unlock()
	return x.pSubscribe(client, x.currentChannels()...)
}

// stopSubscription 降级时关闭订阅；路由登记保留，重新晋升后统一重订。
func (x *Redis) stopSubscription() {
	x.Lock()
	x.demoted = true
	if x.pubSub != nil {
		_ = x.pubSub.Close()
		x.pubSub = nil
	}
	x.Unlock()
}

func (x *Redis) pSubscribe(client *redis.Client, channels ...string) error {
	x.Lock()
	if x.pubSub != nil {
		_ = x.pubSub.Close()
		x.pubSub = nil
	}
	if len(channels) == 0 || x.demoted {
		// 快照后已被降级：不安装订阅，避免 standby 带订阅消费
		x.Unlock()
		return nil
	}
	// 使用本地变量，避免数据竞争
	pubSub := client.PSubscribe(context.Background(), channels...)
	x.pubSub = pubSub
	x.Unlock()

	// 等待订阅确认（PSubscribe 只写命令不等待确认，生效前发布的消息会丢失）
	deadline := time.Now().Add(3 * time.Second)
	for i := 0; i < len(channels); i++ {
		remain := time.Until(deadline)
		if remain <= 0 {
			break
		}
		if _, err := pubSub.ReceiveTimeout(context.Background(), remain); err != nil {
			x.Printf("redis endpoint psubscribe confirm err: %v", err)
			// 未生效的订阅关闭置空并返回错误，交守卫释放租约下轮重建，
			// 避免 leader 持着租约带死订阅空转
			x.Lock()
			if x.pubSub == pubSub {
				_ = pubSub.Close()
				x.pubSub = nil
			}
			x.Unlock()
			return err
		}
	}

	go func() {
		// 遍历接收消息
		for msg := range pubSub.Channel() {
			// 处理消息逻辑
			if x.RuleConfig.Pool != nil {
				err := x.RuleConfig.Pool.Submit(func() {
					x.handlerMsg(client, msg)
				})
				if err != nil {
					x.Printf("redis consumer handler err :%v", err)
				}
			} else {
				go x.handlerMsg(client, msg)
			}
		}
	}()
	return nil
}

func (x *Redis) RemoveRouter(routerId string, params ...interface{}) error {
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		return err
	}
	x.removeSubByRouterId(routerId)
	_ = x.applySubscription(client)
	return nil
}

func (x *Redis) Start() error {
	if !x.SharedNode.IsInit() {
		if err := x.SharedNode.InitWithClose(x.RuleConfig, x.Type(), x.Config.Server, true, func() (*redis.Client, error) {
			return x.initClient()
		}, func(client *redis.Client) error {
			if client != nil {
				return client.Close()
			}
			return nil
		}); err != nil {
			return err
		}
	}
	// 选主循环：无 Locker 时恒为活跃态，行为与未接入守卫前一致
	ctx, cancel := context.WithCancel(context.Background())
	x.guardCancel = cancel
	go x.guard.Run(ctx, func() error {
		client, err := x.SharedNode.GetSafely()
		if err != nil {
			x.Printf("redis endpoint promotion get client err: %v", err)
			return err
		}
		return x.applySubscription(client)
	}, x.stopSubscription)
	return nil
}

func (x *Redis) initClient() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     x.Config.Server,
		DB:       x.Config.Db,
		Password: x.Config.Password,
	})
	return client, client.Ping(context.Background()).Err()
}

func (x *Redis) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}

func (x *Redis) addRouter(router endpointApi.Router, channels ...string) []string {
	x.Lock()
	defer x.Unlock()
	if x.channelRouterMap == nil {
		x.channelRouterMap = map[string][]endpointApi.Router{}
	}
	for _, channel := range channels {
		if _, ok := x.channelRouterMap[channel]; !ok {
			x.channelRouterMap[channel] = []endpointApi.Router{}
		}
		x.channelRouterMap[channel] = append(x.channelRouterMap[channel], router)
	}

	//获取所有的channels
	var newChannels []string
	for channel := range x.channelRouterMap {
		newChannels = append(newChannels, channel)
	}
	return newChannels
}

// 删除指定routerId，返回新的订阅的channels
func (x *Redis) removeSubByRouterId(routerId string) []string {
	x.Lock()
	defer x.Unlock()
	if x.channelRouterMap == nil {
		return nil
	}
	var newChannels []string
	for channel, routers := range x.channelRouterMap {
		// 创建一个新的切片来存储结果
		var newRouters []endpointApi.Router
		for _, router := range routers {
			if router.GetId() != routerId {
				newRouters = append(newRouters, router)
			}
		}
		if len(newRouters) == 0 {
			delete(x.channelRouterMap, channel)
		} else {
			x.channelRouterMap[channel] = newRouters
			newChannels = append(newChannels, channel)
		}
	}
	return newChannels
}

func (x *Redis) checkSubByRouterId(routerId string) bool {
	x.RLock()
	defer x.RUnlock()
	if x.channelRouterMap == nil {
		return false
	}
	for _, routers := range x.channelRouterMap {
		for _, router := range routers {
			if router.GetId() == routerId {
				return true
			}
		}
	}
	return false
}

func (x *Redis) handlerMsg(client *redis.Client, msg *redis.Message) {
	defer func() {
		if e := recover(); e != nil {
			x.Printf("redis endpoint handler err :\n%v", runtime.Stack())
		}
	}()

	x.RLock()
	routers := x.channelRouterMap[msg.Pattern]
	x.RUnlock()
	for _, router := range routers {
		exchange := &endpointApi.Exchange{
			In: &RequestMessage{
				redisClient: client,
				topic:       msg.Channel,
				body:        []byte(msg.Payload),
			},
			Out: &ResponseMessage{
				redisClient: client,
				topic:       msg.Channel,
				log: func(format string, v ...interface{}) {
					x.Printf(format, v...)
				},
			},
		}
		x.DoProcess(context.Background(), router, exchange)
	}
}
