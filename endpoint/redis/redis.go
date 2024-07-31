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
	"github.com/redis/go-redis/v9"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
	"net/textproto"
	"strings"
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
		_ = r.redisClient.Publish(context.Background(), topic, string(r.body))
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
	RuleConfig types.Config
	//Config 配置
	Config           Config
	redisClient      *redis.Client
	pubSub           *redis.PubSub
	channelRouterMap map[string][]endpointApi.Router
}

// Type 组件类型
func (n *Redis) Type() string {
	return Type
}

func (n *Redis) Id() string {
	return n.Config.Server
}

func (n *Redis) New() types.Node {
	return &Redis{
		Config: Config{
			Server: "127.0.0.1:6379",
			Db:     0,
		},
	}
}

// Init 初始化
func (n *Redis) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &n.Config)
	n.RuleConfig = ruleConfig
	return err
}

// Destroy 销毁
func (n *Redis) Destroy() {
	_ = n.Close()
}

func (n *Redis) Close() error {
	if nil != n.redisClient {
		_ = n.redisClient.Close()
	}
	n.BaseEndpoint.Destroy()
	if n.pubSub != nil {
		_ = n.pubSub.Close()
	}
	return nil
}

func (n *Redis) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router cannot be nil")
	}
	// 初始化客户端
	if err := n.initRedisClient(); err != nil {
		return "", err
	}
	routerId := n.CheckAndSetRouterId(router)
	if n.checkSubByRouterId(routerId) {
		return routerId, fmt.Errorf("routerId:%s already exists", routerId)
	}
	channels := strings.Split(router.GetFrom().ToString(), ",")
	newChannels := n.addRouter(router, channels...)
	n.pSubscribe(newChannels...)
	return routerId, nil
}

func (n *Redis) pSubscribe(channels ...string) {
	if n.pubSub != nil {
		_ = n.pubSub.Close()
	}
	if len(channels) == 0 {
		return
	}
	n.pubSub = n.redisClient.PSubscribe(context.Background(), channels...)
	go func() {
		// 遍历接收消息
		for msg := range n.pubSub.Channel() {
			// 处理消息逻辑
			if n.RuleConfig.Pool != nil {
				err := n.RuleConfig.Pool.Submit(func() {
					n.handlerMsg(msg)
				})
				if err != nil {
					n.Printf("kafka consumer handler err :%v", err)
				}
			} else {
				go n.handlerMsg(msg)
			}

		}
	}()
}

func (n *Redis) RemoveRouter(routerId string, params ...interface{}) error {
	channels := n.removeSubByRouterId(routerId)
	n.pSubscribe(channels...)
	return nil
}

func (n *Redis) Start() error {
	return n.initRedisClient()
}

// initRedisClient 初始化Redis客户端
func (n *Redis) initRedisClient() error {
	if n.redisClient == nil {
		n.redisClient = redis.NewClient(&redis.Options{
			Addr: n.Config.Server,
			DB:   n.Config.Db,
		})
		return n.redisClient.Ping(context.Background()).Err()
	}
	return nil
}

func (n *Redis) Printf(format string, v ...interface{}) {
	if n.RuleConfig.Logger != nil {
		n.RuleConfig.Logger.Printf(format, v...)
	}
}

func (n *Redis) addRouter(router endpointApi.Router, channels ...string) []string {
	n.Lock()
	defer n.Unlock()
	if n.channelRouterMap == nil {
		n.channelRouterMap = map[string][]endpointApi.Router{}
	}
	for _, channel := range channels {
		if _, ok := n.channelRouterMap[channel]; !ok {
			n.channelRouterMap[channel] = []endpointApi.Router{}
		}
		n.channelRouterMap[channel] = append(n.channelRouterMap[channel], router)
	}

	//获取所有的channels
	var newChannels []string
	for channel := range n.channelRouterMap {
		newChannels = append(newChannels, channel)
	}
	return newChannels
}

// 删除指定routerId，返回新的订阅的channels
func (n *Redis) removeSubByRouterId(routerId string) []string {
	n.Lock()
	defer n.Unlock()
	if n.channelRouterMap == nil {
		return nil
	}
	var newChannels []string
	for channel, routers := range n.channelRouterMap {
		// 创建一个新的切片来存储结果
		var newRouters []endpointApi.Router
		for _, router := range routers {
			if router.GetId() != routerId {
				newRouters = append(newRouters, router)
			}
		}
		if len(newRouters) == 0 {
			delete(n.channelRouterMap, channel)
		} else {
			n.channelRouterMap[channel] = newRouters
			newChannels = append(newChannels, channel)
		}
	}
	return newChannels
}

func (n *Redis) checkSubByRouterId(routerId string) bool {
	n.RLock()
	defer n.RUnlock()
	if n.channelRouterMap == nil {
		return false
	}
	for _, routers := range n.channelRouterMap {
		for _, router := range routers {
			if router.GetId() == routerId {
				return true
			}
		}
	}
	return false
}

func (n *Redis) handlerMsg(msg *redis.Message) {
	defer func() {
		if e := recover(); e != nil {
			n.Printf("redis endpoint handler err :\n%v", runtime.Stack())
		}
	}()
	n.RLock()
	routers := n.channelRouterMap[msg.Pattern]
	n.RUnlock()
	for _, router := range routers {
		exchange := &endpointApi.Exchange{
			In: &RequestMessage{
				redisClient: n.redisClient,
				topic:       msg.Channel,
				body:        []byte(msg.Payload),
			},
			Out: &ResponseMessage{
				redisClient: n.redisClient,
				topic:       msg.Channel,
				log: func(format string, v ...interface{}) {
					n.Printf(format, v...)
				},
			},
		}
		n.DoProcess(context.Background(), router, exchange)
	}
}
