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

package pulsar

import (
	"context"
	"errors"
	"fmt"
	"net/textproto"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
)

// Type 组件类型
const Type = types.EndpointTypePrefix + "pulsar"

// KeyResponseTopic 响应主题metadataKey
const KeyResponseTopic = "responseTopic"

// Endpoint 别名
type Endpoint = Pulsar

var _ endpointApi.Endpoint = (*Endpoint)(nil)

// 注册组件
func init() {
	_ = endpoint.Registry.Register(&Endpoint{})
}

// RequestMessage 请求消息
type RequestMessage struct {
	message pulsar.Message
	msg     *types.RuleMsg
	err     error
}

// Body 获取消息体
func (r *RequestMessage) Body() []byte {
	return r.message.Payload()
}

// Headers 获取消息头
func (r *RequestMessage) Headers() textproto.MIMEHeader {
	header := make(textproto.MIMEHeader)
	header.Set("topic", r.message.Topic())
	header.Set("messageId", r.message.ID().String())
	header.Set("publishTime", r.message.PublishTime().Format(time.RFC3339))
	header.Set("eventTime", r.message.EventTime().Format(time.RFC3339))
	if r.message.Key() != "" {
		header.Set("key", r.message.Key())
	}
	// 添加自定义属性
	for k, v := range r.message.Properties() {
		header.Set(k, v)
	}
	return header
}

// From 获取消息来源
func (r *RequestMessage) From() string {
	return r.message.Topic()
}

// GetParam 获取参数
func (r *RequestMessage) GetParam(key string) string {
	return r.message.Properties()[key]
}

// SetMsg 设置规则消息
func (r *RequestMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

// GetMsg 获取规则消息
func (r *RequestMessage) GetMsg() *types.RuleMsg {
	if r.msg == nil {
		// 默认指定是JSON格式，如果不是该类型，请在process函数中修改
		ruleMsg := types.NewMsg(0, r.From(), types.JSON, types.NewMetadata(), string(r.Body()))
		ruleMsg.Metadata.PutValue("topic", r.message.Topic())
		ruleMsg.Metadata.PutValue("messageId", r.message.ID().String())
		ruleMsg.Metadata.PutValue("publishTime", r.message.PublishTime().Format(time.RFC3339))
		ruleMsg.Metadata.PutValue("eventTime", r.message.EventTime().Format(time.RFC3339))
		if r.message.Key() != "" {
			ruleMsg.Metadata.PutValue("key", r.message.Key())
		}
		// 添加自定义属性
		for k, v := range r.message.Properties() {
			ruleMsg.Metadata.PutValue(k, v)
		}
		r.msg = &ruleMsg
	}
	return r.msg
}

// SetStatusCode 设置状态码
func (r *RequestMessage) SetStatusCode(statusCode int) {
}

// SetBody 设置消息体
func (r *RequestMessage) SetBody(body []byte) {
}

// SetError 设置错误
func (r *RequestMessage) SetError(err error) {
	r.err = err
}

// GetError 获取错误
func (r *RequestMessage) GetError() error {
	return r.err
}

// ResponseMessage 响应消息
type ResponseMessage struct {
	message   pulsar.Message
	producers *sync.Map
	client    *base.SharedNode[pulsar.Client]
	body      []byte
	msg       *types.RuleMsg
	headers   textproto.MIMEHeader
	err       error
}

// Body 获取响应体
func (r *ResponseMessage) Body() []byte {
	return r.body
}

// Headers 获取响应头
func (r *ResponseMessage) Headers() textproto.MIMEHeader {
	if r.headers == nil {
		r.headers = make(map[string][]string)
	}
	return r.headers
}

// From 获取消息来源
func (r *ResponseMessage) From() string {
	return r.message.Topic()
}

// GetParam 获取参数
func (r *ResponseMessage) GetParam(key string) string {
	return r.message.Properties()[key]
}

// SetMsg 设置规则消息
func (r *ResponseMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

// GetMsg 获取规则消息
func (r *ResponseMessage) GetMsg() *types.RuleMsg {
	return r.msg
}

// SetStatusCode 设置状态码
func (r *ResponseMessage) SetStatusCode(statusCode int) {
}

// getMetadataValue 从msg.Metadata或者响应头获取值
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

// SetBody 设置响应体
func (r *ResponseMessage) SetBody(body []byte) {
	r.body = body
	topic := r.getMetadataValue(KeyResponseTopic, KeyResponseTopic)
	if topic != "" && r.producers != nil && r.client != nil {
		// 获取客户端
		client, err := r.client.GetSafely()
		if err != nil {
			r.SetError(err)
			return
		}

		// 获取或创建对应topic的生产者（使用sync.Map的无锁操作）
		var producer pulsar.Producer
		if value, exists := r.producers.Load(topic); exists {
			producer = value.(pulsar.Producer)
		} else {
			// 创建新的生产者
			producerOptions := pulsar.ProducerOptions{
				Topic: topic,
			}

			newProducer, err := client.CreateProducer(producerOptions)
			if err != nil {
				r.SetError(err)
				return
			}

			// 使用LoadOrStore确保只有一个生产者被创建和存储
			if actual, loaded := r.producers.LoadOrStore(topic, newProducer); loaded {
				// 如果已经存在，关闭新创建的生产者，使用已存在的
				newProducer.Close()
				producer = actual.(pulsar.Producer)
			} else {
				// 使用新创建的生产者
				producer = newProducer
			}
		}

		// 构建消息属性
		properties := make(map[string]string)
		for k, v := range r.Headers() {
			if len(v) > 0 {
				properties[k] = v[0]
			}
		}

		// 发送响应消息
		_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload:    r.body,
			Properties: properties,
		})
		if err != nil {
			r.SetError(err)
		}
	}
}

// SetError 设置错误
func (r *ResponseMessage) SetError(err error) {
	r.err = err
}

// GetError 获取错误
func (r *ResponseMessage) GetError() error {
	return r.err
}

// Config Pulsar配置
type Config struct {
	// Pulsar服务器地址
	Server string `json:"server" label:"Pulsar服务器地址" desc:"Pulsar服务器地址，格式为pulsar://host:port" required:"true"`
	// 默认订阅名称
	SubName string `json:"subName" label:"订阅名称" desc:"默认订阅名称，如果AddRouter时未指定则使用此值" required:"true"`
	// 订阅类型
	SubType string `json:"subType" label:"订阅类型" desc:"订阅类型：Exclusive、Shared、Failover、KeyShared" required:"true"`
	// 消息通道缓冲池大小
	PoolSize int `json:"poolSize" label:"消息通道缓冲池大小" desc:"消息通道缓冲区大小，默认为100"`
	// 鉴权令牌
	AuthToken string `json:"authToken" label:"鉴权令牌" desc:"Pulsar JWT鉴权令牌"`
	// TLS证书文件
	CertFile string `json:"certFile" label:"TLS证书文件" desc:"TLS证书文件路径"`
	// TLS私钥文件
	CertKeyFile string `json:"certKeyFile" label:"TLS私钥文件" desc:"TLS私钥文件路径"`
}

// parseSubscriptionType 解析订阅类型字符串为pulsar.SubType（大小写不敏感）
func parseSubscriptionType(subscriptionType string) pulsar.SubscriptionType {
	switch strings.ToLower(subscriptionType) {
	case "exclusive":
		return pulsar.Exclusive
	case "shared":
		return pulsar.Shared
	case "failover":
		return pulsar.Failover
	case "keyshared":
		return pulsar.KeyShared
	default:
		return pulsar.Shared // 默认使用Shared类型
	}
}

// Pulsar Pulsar接收端端点
type Pulsar struct {
	impl.BaseEndpoint
	base.SharedNode[pulsar.Client]
	// GracefulShutdown provides graceful shutdown capabilities
	// GracefulShutdown 提供优雅停机功能
	base.GracefulShutdown
	RuleConfig types.Config
	//Config 配置
	Config Config
	// 消费者映射关系，用于停止消费
	consumers map[string]pulsar.Consumer
	// topic+subscription组合映射，用于检查重复订阅
	subscriptions map[string]string // key: topic+subscription, value: routerId
	// 生产者映射，key为topic，value为对应的生产者
	producers sync.Map
	// 互斥锁
	mu sync.RWMutex
}

// Type 组件类型
func (x *Pulsar) Type() string {
	return Type
}

// Id 获取组件ID
func (x *Pulsar) Id() string {
	return x.Config.Server
}

// New 创建新实例
func (x *Pulsar) New() types.Node {
	return &Pulsar{
		Config: Config{
			Server:   "pulsar://localhost:6650",
			SubName:  "default",
			SubType:  "Shared",
			PoolSize: 100,
		},
	}
}

// Init 初始化
func (x *Pulsar) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	x.RuleConfig = ruleConfig
	x.consumers = make(map[string]pulsar.Consumer)
	x.subscriptions = make(map[string]string)

	// 初始化优雅停机功能
	x.GracefulShutdown.InitGracefulShutdown(x.RuleConfig.Logger, 0)

	// 初始化共享客户端
	_ = x.SharedNode.InitWithClose(x.RuleConfig, x.Type(), x.Config.Server, true, func() (pulsar.Client, error) {
		return x.initClient()
	}, func(client pulsar.Client) error {
		if client != nil {
			client.Close()
		}
		return nil
	})

	return err
}

// Destroy 销毁
func (x *Pulsar) Destroy() {
	x.GracefulShutdown.GracefulStop(func() {
		_ = x.Close()
	})
}

// GracefulStop 优雅停机
func (x *Pulsar) GracefulStop() {
	x.GracefulShutdown.GracefulStop(func() {
		_ = x.Close()
	})
}

// Close 关闭连接
func (x *Pulsar) Close() error {
	x.mu.Lock()
	defer x.mu.Unlock()

	// 停止所有消费者
	for _, consumer := range x.consumers {
		consumer.Close()
	}
	x.consumers = make(map[string]pulsar.Consumer)
	x.subscriptions = make(map[string]string)

	// 停止所有生产者（使用sync.Map的无锁操作）
	x.producers.Range(func(key, value interface{}) bool {
		if producer, ok := value.(pulsar.Producer); ok && producer != nil {
			producer.Close()
		}
		return true
	})
	// 清空sync.Map
	x.producers = sync.Map{}

	// 关闭共享客户端
	_ = x.SharedNode.Close()
	x.BaseEndpoint.Destroy()
	return nil
}

// AddRouter 添加路由
func (x *Pulsar) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router cannot be nil")
	}

	client, err := x.SharedNode.GetSafely()
	if err != nil {
		return "", err
	}

	routerId := router.GetId()
	if routerId == "" {
		routerId = router.GetFrom().ToString()
		router.SetId(routerId)
	}

	x.mu.Lock()
	defer x.mu.Unlock()

	if _, ok := x.consumers[routerId]; ok {
		return routerId, fmt.Errorf("routerId %s already exists", routerId)
	}

	// 解析topic和subscription
	from := router.FromToString()
	topic := from
	subscription := x.Config.SubName
	if subscription == "" {
		subscription = "default"
	}

	// 如果有参数，第一个参数作为subscription
	if len(params) > 0 {
		if sub, ok := params[0].(string); ok {
			subscription = sub
		}
	}

	// 检查topic+subscription组合是否已存在
	subscriptionKey := topic + "|" + subscription
	if existingRouterId, exists := x.subscriptions[subscriptionKey]; exists {
		return routerId, fmt.Errorf("topic '%s' with subscription '%s' already exists for routerId '%s'", topic, subscription, existingRouterId)
	}

	// 解析订阅类型
	subscriptionType := parseSubscriptionType(x.Config.SubType)

	// 创建消费者配置
	consumerOptions := pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscription,
		Type:             subscriptionType,
	}

	// 使用简化的配置
	consumerOptions.Topic = topic
	consumerOptions.SubscriptionName = subscription

	// 设置消息处理器
	poolSize := x.Config.PoolSize
	if poolSize <= 0 {
		poolSize = 100 // 默认值
	}
	consumerOptions.MessageChannel = make(chan pulsar.ConsumerMessage, poolSize)

	// 创建消费者
	consumer, err := client.Subscribe(consumerOptions)
	if err != nil {
		return "", err
	}

	// 启动消息处理协程
	go func() {
		for {
			select {
			case msg, ok := <-consumer.Chan():
				if !ok {
					return
				}
				// 使用线程池或协程处理消息，避免阻塞消息接收循环
				if x.RuleConfig.Pool != nil {
					_ = x.RuleConfig.Pool.Submit(func() {
						x.handleMessage(msg, router)
					})
				} else {
					// 开启协程处理消息，确保不阻塞消息接收
					go x.handleMessage(msg, router)
				}
			}
		}
	}()

	x.consumers[routerId] = consumer
	x.subscriptions[subscriptionKey] = routerId
	return routerId, nil
}

// handleMessage 处理单个消息
// 处理Pulsar消息，创建Exchange并执行规则链处理
func (x *Pulsar) handleMessage(msg pulsar.ConsumerMessage, router endpointApi.Router) {
	defer func() {
		if e := recover(); e != nil {
			x.Printf("pulsar endpoint handler err :\n%v", runtime.Stack())
		}
	}()

	exchange := &endpointApi.Exchange{
		In: &RequestMessage{
			message: msg.Message,
		},
		Out: &ResponseMessage{
			message:   msg.Message,
			producers: &x.producers,
			client:    &x.SharedNode,
		},
	}
	x.DoProcess(context.Background(), router, exchange)
	// 确认消息
	_ = msg.Ack(msg.Message)
}

// RemoveRouter 移除路由
func (x *Pulsar) RemoveRouter(routerId string, params ...interface{}) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	if consumer, ok := x.consumers[routerId]; ok {
		consumer.Close()
		delete(x.consumers, routerId)
		
		// 删除对应的subscription映射记录
		for key, value := range x.subscriptions {
			if value == routerId {
				delete(x.subscriptions, key)
				break
			}
		}
		return nil
	}
	return errors.New("router not found")
}

// Start 启动服务
func (x *Pulsar) Start() error {
	if !x.SharedNode.IsInit() {
		return x.SharedNode.InitWithClose(x.RuleConfig, x.Type(), x.Config.Server, true, func() (pulsar.Client, error) {
			return x.initClient()
		}, func(client pulsar.Client) error {
			if client != nil {
				client.Close()
			}
			return nil
		})
	}

	// 生产者将在需要时动态创建
	return nil
}

// Printf 打印日志
func (x *Pulsar) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}

// initClient 初始化Pulsar客户端
func (x *Pulsar) initClient() (pulsar.Client, error) {
	clientOptions := pulsar.ClientOptions{
		URL: x.Config.Server,
	}

	// 设置JWT Token鉴权
	if x.Config.AuthToken != "" {
		clientOptions.Authentication = pulsar.NewAuthenticationToken(x.Config.AuthToken)
	}

	// 设置TLS配置
	if x.Config.CertFile != "" {
		clientOptions.TLSCertificateFile = x.Config.CertFile
	}
	if x.Config.CertKeyFile != "" {
		clientOptions.TLSKeyFilePath = x.Config.CertKeyFile
	}

	client, err := pulsar.NewClient(clientOptions)
	return client, err
}
