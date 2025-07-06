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

	"github.com/redis/go-redis/v9"
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
	// Redis服务器地址
	Server string
	// Redis密码
	Password string
	// Redis数据库index
	Db int
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

// Init 初始化
func (x *Redis) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	x.RuleConfig = ruleConfig

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
	return err
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
	// SharedNode 会通过 InitWithClose 中的清理函数来管理客户端的关闭
	// SharedNode manages client closure through the cleanup function in InitWithClose
	_ = x.SharedNode.Close()
	if x.pubSub != nil {
		_ = x.pubSub.Close()
		x.pubSub = nil
	}
	x.BaseEndpoint.Destroy()
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
	newChannels := x.addRouter(router, channels...)
	x.pSubscribe(client, newChannels...)
	return routerId, nil
}

func (x *Redis) pSubscribe(client *redis.Client, channels ...string) {
	if x.pubSub != nil {
		_ = x.pubSub.Close()
	}
	if len(channels) == 0 {
		return
	}
	x.pubSub = client.PSubscribe(context.Background(), channels...)
	go func() {
		// 遍历接收消息
		for msg := range x.pubSub.Channel() {
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
}

func (x *Redis) RemoveRouter(routerId string, params ...interface{}) error {
	channels := x.removeSubByRouterId(routerId)
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		return err
	}
	x.pSubscribe(client, channels...)
	return nil
}

func (x *Redis) Start() error {
	if !x.SharedNode.IsInit() {
		return x.SharedNode.InitWithClose(x.RuleConfig, x.Type(), x.Config.Server, true, func() (*redis.Client, error) {
			return x.initClient()
		}, func(client *redis.Client) error {
			if client != nil {
				return client.Close()
			}
			return nil
		})
	}
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
