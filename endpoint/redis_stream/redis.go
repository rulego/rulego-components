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
	"github.com/rulego/rulego/utils/json"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
	"github.com/rulego/rulego/utils/str"
	"net/textproto"
	"strings"
)

// Type 组件类型
const Type = types.EndpointTypePrefix + "redis/stream"

// KeyResponseTopic 响应主题metadataKey
const KeyResponseTopic = "responseTopic"

// KeyResponseStream 响应流metadataKey
const KeyResponseStream = "responseStream"

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
		topic = r.getMetadataValue(KeyResponseStream, KeyResponseStream)
	}
	if topic != "" {
		var values interface{}
		var message = make(map[string]interface{})
		if err := json.Unmarshal(body, &message); err == nil {
			values = message
		} else {
			message["value"] = string(body)
		}
		values = message
		// 向 Stream 写入数据
		_, err := r.redisClient.XAdd(context.Background(), &redis.XAddArgs{
			Stream: topic,
			ID:     "*", // 使用 "*" 表示使用 Redis 自动生成的 ID
			Values: values,
		}).Result()
		if err != nil {
			r.log("redis stream write error:%v", err)
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
	// Redis消费者组ID
	GroupId string
}

// Redis Redis接收端端点
type Redis struct {
	impl.BaseEndpoint
	RuleConfig types.Config
	//Config 配置
	Config                   Config
	redisClient              *redis.Client
	pubSub                   *redis.PubSub
	routerIdAndStreamNameMap map[string]string
	streamNameAndRouterIdMap map[string]string
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
			Server:  "127.0.0.1:6379",
			Db:      0,
			GroupId: "rulego",
		},
	}
}

// Init 初始化
func (n *Redis) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &n.Config)
	if err == nil {
		n.Config.GroupId = strings.TrimSpace(n.Config.GroupId)
		if n.Config.GroupId == "" {
			n.Config.GroupId = "rulego"
		}
	}
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
	if err := n.addRouter(router); err != nil {
		return routerId, err
	}
	return routerId, n.createConsumerGroup(router)
}

func (n *Redis) parseStreams(streamNames string) []string {
	// 分割 Stream 名称
	streamList := strings.Split(streamNames, ",")

	// 准备 Streams 参数，每个 Stream 都需要一个起始消息 ID
	// 这里我们使用 ">" 作为起始消息 ID，表示从每个 Stream 的最新消息开始读取
	var streams []string
	streams = append(streams, streamList...)
	for range streamList {
		//消息ID
		streams = append(streams, ">")
	}
	return streams
}

func (n *Redis) createConsumerGroup(router endpointApi.Router) error {
	for _, streamName := range strings.Split(router.FromToString(), ",") {
		// 创建消费者组
		if err := n.redisClient.XGroupCreateMkStream(context.Background(), streamName, n.Config.GroupId, "$").Err(); err != nil {
			if err.Error() != "BUSYGROUP Consumer Group name already exists" {
				return err
			}
		}
	}
	var args = &redis.XReadGroupArgs{
		Group:    n.Config.GroupId,
		Consumer: router.GetId(),
		Streams:  n.parseStreams(router.FromToString()),
		Block:    0, // 阻塞等待新消息
		Count:    1, // 一次处理一条消息
	}
	go func() {
		// 消费消息
		for {
			messages, err := n.redisClient.XReadGroup(context.Background(), args).Result()
			if err != nil {
				n.Printf("XReadGroup err:%v", err)
				break
			}
			for _, message := range messages {
				for _, msg := range message.Messages {
					go n.handlerMsg(message.Stream, msg, router)
				}
			}
		}
	}()
	return nil
}

func (n *Redis) RemoveRouter(routerId string, params ...interface{}) error {
	streamName := n.deleteRouter(routerId)
	if streamName == "" {
		return nil
	}
	for _, item := range strings.Split(streamName, ",") {
		if err := n.redisClient.XGroupDestroy(context.Background(), item, n.Config.GroupId).Err(); err != nil {
			return err
		}
	}
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

func (n *Redis) addRouter(router endpointApi.Router) error {
	n.Lock()
	defer n.Unlock()

	if n.routerIdAndStreamNameMap == nil {
		n.routerIdAndStreamNameMap = map[string]string{}
	}
	if n.streamNameAndRouterIdMap == nil {
		n.streamNameAndRouterIdMap = map[string]string{}
	}
	if _, ok := n.routerIdAndStreamNameMap[router.GetId()]; ok {
		return fmt.Errorf("routerId:%s already exists", router.GetId())
	}
	if _, ok := n.streamNameAndRouterIdMap[router.FromToString()]; ok {
		return fmt.Errorf("steam:%s already exists", router.FromToString())
	}
	n.routerIdAndStreamNameMap[router.GetId()] = router.FromToString()
	n.streamNameAndRouterIdMap[router.FromToString()] = router.GetId()
	return nil
}

// 从存储器中删除路由
func (n *Redis) deleteRouter(id string) string {
	n.Lock()
	defer n.Unlock()
	if n.streamNameAndRouterIdMap != nil {
		if streamName, ok := n.routerIdAndStreamNameMap[id]; ok {
			delete(n.routerIdAndStreamNameMap, id)
			delete(n.streamNameAndRouterIdMap, streamName)
			return streamName
		}
	}
	return ""
}

func (n *Redis) handlerMsg(stream string, msg redis.XMessage, router endpointApi.Router) {
	defer func() {
		if e := recover(); e != nil {
			n.Printf("redis stream endpoint handler err :\n%v", runtime.Stack())
		}
	}()
	exchange := &endpointApi.Exchange{
		In: &RequestMessage{
			redisClient: n.redisClient,
			topic:       stream,
			body:        []byte(str.ToString(msg.Values)),
		},
		Out: &ResponseMessage{
			redisClient: n.redisClient,
			topic:       stream,
			log: func(format string, v ...interface{}) {
				n.Printf(format, v...)
			},
		},
	}
	n.DoProcess(context.Background(), router, exchange)
	// 确认消息处理完成
	_ = n.redisClient.XAck(context.Background(), router.GetFrom().ToString(), n.Config.GroupId, msg.ID).Err()
}
