/*
 * Copyright 2023 The RuleGo Authors.
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

package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
	"net/textproto"
	"strconv"
	"strings"
	"time"
)

// Type 组件类型
const Type = types.EndpointTypePrefix + "kafka"
const (
	//Topic 消息主题
	Topic = "topic"
	//Key 消息key
	Key = "key"
	//Partition 消费分区
	Partition = "partition"
)
const (
	// KeyResponseTopic 响应主题metadataKey
	KeyResponseTopic = "responseTopic"
	// KeyResponseKey 响应key metadataKey
	KeyResponseKey = "key"
	// KeyResponsePartition 响应 消费分区metadataKey
	KeyResponsePartition = "partition"
)

// Endpoint 别名
type Endpoint = Kafka

var _ endpointApi.Endpoint = (*Endpoint)(nil)

// 注册组件
func init() {
	_ = endpoint.Registry.Register(&Endpoint{})
}

// RequestMessage http请求消息
type RequestMessage struct {
	request *sarama.ConsumerMessage
	msg     *types.RuleMsg
	err     error
}

func (r *RequestMessage) Body() []byte {
	return r.request.Value
}

func (r *RequestMessage) Headers() textproto.MIMEHeader {
	header := make(textproto.MIMEHeader)
	header.Set(Topic, r.request.Topic)
	return header
}

func (r *RequestMessage) From() string {
	return r.request.Topic
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

		ruleMsg.Metadata.PutValue(Topic, r.From())

		r.msg = &ruleMsg
	}
	return r.msg
}

func (r *RequestMessage) SetStatusCode(statusCode int) {
}

func (r *RequestMessage) SetBody(body []byte) {
}

func (r *RequestMessage) SetError(err error) {
	r.err = err
}

func (r *RequestMessage) GetError() error {
	return r.err
}

// ResponseMessage http响应消息
type ResponseMessage struct {
	request  *sarama.ConsumerMessage
	response sarama.SyncProducer
	body     []byte
	msg      *types.RuleMsg
	headers  textproto.MIMEHeader
	err      error
	log      func(format string, v ...interface{})
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
	return r.request.Topic
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
	if topic != "" {
		key := r.getMetadataValue(KeyResponseKey, KeyResponseKey)
		partitionStr := r.getMetadataValue(KeyResponsePartition, KeyResponsePartition)
		var partition = int32(0)
		if partitionStr != "" {
			if num, err := strconv.ParseInt(partitionStr, 10, 32); err == nil {
				partition = int32(num)
			}
		}
		message := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: partition,
			Key:       sarama.StringEncoder(key),
			Value:     sarama.StringEncoder(r.body),
		}
		_, _, err := r.response.SendMessage(message)
		if err != nil {

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
	// kafka服务器地址列表，多个与逗号隔开
	Server string `json:"server"`
	// GroupId 消费者组Id
	GroupId string `json:"groupId"`
	// SASL认证配置
	SASL SASLConfig `json:"sasl"`
	// TLS配置
	TLS TLSConfig `json:"tls"`
}

// SASLConfig SASL认证配置
type SASLConfig struct {
	// Enable 是否启用SASL认证
	Enable bool `json:"enable"`
	// Mechanism 认证机制，支持 PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	Mechanism string `json:"mechanism"`
	// Username 用户名
	Username string `json:"username"`
	// Password 密码
	Password string `json:"password"`
}

// TLSConfig TLS配置
type TLSConfig struct {
	// Enable 是否启用TLS
	Enable bool `json:"enable"`
	// InsecureSkipVerify 是否跳过证书验证
	InsecureSkipVerify bool `json:"insecureSkipVerify"`
}

// Kafka Kafka 接收端端点
type Kafka struct {
	impl.BaseEndpoint
	RuleConfig types.Config
	//Config 配置
	Config Config
	// brokers kafka服务器地址列表
	brokers []string
	//消息生产者，用于响应
	producer sarama.SyncProducer
	// 主题和主题消费者映射关系，用于取消订阅
	handlers map[string]sarama.ConsumerGroup
	closed   bool
}

// Type 组件类型
func (x *Kafka) Type() string {
	return Type
}

func (x *Kafka) New() types.Node {
	return &Kafka{
		Config: Config{
			Server:  "127.0.0.1:9092",
			GroupId: "rulego",
			SASL: SASLConfig{
				Mechanism: "PLAIN",
			},
		},
	}
}

func (x *Kafka) getBrokerFromOldVersion(configuration types.Configuration) []string {
	if v, ok := configuration["brokers"]; ok {
		return v.([]string)
	} else {
		return nil
	}
}

// Init 初始化
func (x *Kafka) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	x.Config.GroupId = strings.TrimSpace(x.Config.GroupId)
	if x.Config.GroupId == "" {
		x.Config.GroupId = "rulego"
	}
	x.brokers = x.getBrokerFromOldVersion(configuration)
	if len(x.brokers) == 0 && x.Config.Server != "" {
		x.brokers = strings.Split(x.Config.Server, ",")
	}
	if len(x.brokers) == 0 {
		return errors.New("brokers is empty")
	}
	x.RuleConfig = ruleConfig
	return err
}

// Destroy 销毁
func (x *Kafka) Destroy() {
	_ = x.Close()
}

func (x *Kafka) Close() error {
	for _, v := range x.handlers {
		_ = v.Close()
	}

	x.handlers = nil
	x.closed = true
	if nil != x.producer {
		return x.producer.Close()
	}
	x.BaseEndpoint.Destroy()
	return nil
}

func (x *Kafka) Id() string {
	if len(x.brokers) > 0 {
		return x.brokers[0]
	} else {
		return ""
	}
}

func (x *Kafka) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router can not nil")
	}
	//初始化kafka客户端
	if err := x.initKafkaProducer(); err != nil {
		return "", err
	}

	if id := router.GetId(); id == "" {
		router.SetId(router.GetFrom().ToString())
	}
	if err := x.createTopicConsumer(router); err != nil {
		return "", err
	}
	return router.GetId(), nil
}

func (x *Kafka) RemoveRouter(routerId string, params ...interface{}) error {
	x.Lock()
	defer x.Unlock()
	//删除订阅
	if v, ok := x.handlers[routerId]; ok {
		delete(x.handlers, routerId)
		return v.Close()
	}
	return nil
}

func (x *Kafka) Start() error {
	return x.initKafkaProducer()
}

// initKafkaProducer 初始化kafka生产者，用于响应
func (x *Kafka) initKafkaProducer() error {
	if x.producer == nil {
		config := sarama.NewConfig()
		config.Producer.Return.Successes = true // 同步模式需要设置这个参数为true

		// 配置SASL认证
		if x.Config.SASL.Enable {
			config.Net.SASL.Enable = true
			config.Net.SASL.User = x.Config.SASL.Username
			config.Net.SASL.Password = x.Config.SASL.Password

			switch strings.ToUpper(x.Config.SASL.Mechanism) {
			case "PLAIN":
				config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			case "SCRAM-SHA-256":
				config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			case "SCRAM-SHA-512":
				config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			default:
				config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			}
		}

		// 配置TLS
		if x.Config.TLS.Enable {
			config.Net.TLS.Enable = true
			if x.Config.TLS.InsecureSkipVerify {
				config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}
			}
		}

		producer, err := sarama.NewSyncProducer(x.brokers, config)
		if err != nil {
			return err
		}
		x.producer = producer
	}

	return nil
}

// 创建kafka消费者
func (x *Kafka) createTopicConsumer(router endpointApi.Router) error {
	if form := router.GetFrom(); form != nil {
		routerId := router.GetId()
		if routerId == "" {
			routerId = router.GetFrom().ToString()
			router.SetId(routerId)
		}
		x.Lock()
		defer x.Unlock()
		if x.handlers == nil {
			x.handlers = make(map[string]sarama.ConsumerGroup)
		}
		if _, ok := x.handlers[routerId]; ok {
			return fmt.Errorf("routerId %s already exists", routerId)
		}
		config := sarama.NewConfig()
		// 设置重连相关配置
		config.Consumer.Return.Errors = true
		config.Metadata.Retry.Max = 3
		config.Metadata.Retry.Backoff = 250 * 1000000 // 250ms
		config.Consumer.Offsets.Initial = sarama.OffsetNewest

		// 配置SASL认证
		if x.Config.SASL.Enable {
			config.Net.SASL.Enable = true
			config.Net.SASL.User = x.Config.SASL.Username
			config.Net.SASL.Password = x.Config.SASL.Password

			switch strings.ToUpper(x.Config.SASL.Mechanism) {
			case "PLAIN":
				config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			case "SCRAM-SHA-256":
				config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			case "SCRAM-SHA-512":
				config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			default:
				config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			}
		}

		// 配置TLS
		if x.Config.TLS.Enable {
			config.Net.TLS.Enable = true
			if x.Config.TLS.InsecureSkipVerify {
				config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}
			}
		}

		consumer, err := sarama.NewConsumerGroup(x.brokers, x.Config.GroupId, config)
		if err != nil {
			return err
		}
		x.handlers[routerId] = consumer

		topics := []string{form.ToString()}                // 订阅的主题列表
		handler := &consumerHandler{router: router, ep: x} // 自定义的消费者处理程序

		// 启动消费者goroutine，带重连机制
		go x.startConsumerWithRetry(consumer, topics, handler, routerId)

	}
	return nil
}

// 带重连机制的消费者启动函数
func (x *Kafka) startConsumerWithRetry(consumer sarama.ConsumerGroup, topics []string, handler *consumerHandler, routerId string) {
	defer func() {
		if consumer != nil {
			_ = consumer.Close()
		}
		// 从handlers中移除已关闭的消费者
		x.Lock()
		if x.handlers != nil {
			delete(x.handlers, routerId)
		}
		x.Unlock()
	}()

	ctx := context.Background()
	for {
		// 检查消费者是否已关闭
		select {
		case <-ctx.Done():
			return
		default:
		}

		// 检查消费者组是否仍在handlers中（用于判断是否被手动移除）
		x.Lock()
		_, exists := x.handlers[routerId]
		x.Unlock()
		if !exists {
			x.Printf("Consumer for routerId %s topic %s has been removed, stopping", routerId, topics[0])
			return
		}
		if x.closed {
			return
		}
		err := consumer.Consume(ctx, topics, handler)
		if err != nil {
			x.Printf("Consumer error for topic %s: %v, will retry...", topics[0], err)
			// 如果是致命错误，重新创建消费者
			if err == sarama.ErrClosedConsumerGroup {
				x.Printf("Consumer group closed, recreating consumer for topic %s", topics[0])
				// 重新创建消费者
				config := sarama.NewConfig()
				config.Consumer.Return.Errors = true
				config.Metadata.Retry.Max = 3
				config.Metadata.Retry.Backoff = 250 * 1000000 // 250ms
				config.Consumer.Offsets.Initial = sarama.OffsetNewest
				newConsumer, createErr := sarama.NewConsumerGroup(x.brokers, x.Config.GroupId, config)
				if createErr != nil {
					x.Printf("Failed to recreate consumer for topic %s: %v", topics[0], createErr)
					return
				}
				// 更新handlers中的消费者引用
				x.Lock()
				if x.handlers != nil {
					_ = consumer.Close() // 关闭旧的消费者
					x.handlers[routerId] = newConsumer
					consumer = newConsumer
				}
				x.Unlock()
				x.Printf("Successfully recreated consumer for topic %s", topics[0])
			}
		} else {
			x.Printf("Consumer for topic %s stopped normally, retry after 5s", topics[0])
			time.Sleep(5 * time.Second)
		}
	}
}

func (x *Kafka) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}

// 自定义消费者处理程序
type consumerHandler struct {
	ep         *Endpoint
	router     endpointApi.Router
	ruleConfig types.Config
}

func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		// 处理消息逻辑
		if h.ruleConfig.Pool != nil {
			err := h.ruleConfig.Pool.Submit(func() {
				h.handlerMsg(session, msg)
			})
			if err != nil {
				h.ep.Printf("kafka consumer handler err :%v", err)
			}
			return err
		} else {
			go h.handlerMsg(session, msg)
		}
	}
	return nil
}
func (h *consumerHandler) handlerMsg(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
	defer func() {
		if e := recover(); e != nil {
			h.ep.Printf("kafka endpoint handler err :\n%v", runtime.Stack())
		}
	}()
	exchange := &endpointApi.Exchange{
		In: &RequestMessage{
			request: msg,
		},
		Out: &ResponseMessage{
			request:  msg,
			response: h.ep.producer,
			log: func(format string, v ...interface{}) {
				h.ep.Printf(format, v...)
			},
		},
	}
	metadata := exchange.In.GetMsg().Metadata
	metadata.PutValue(Key, string(msg.Key))
	metadata.PutValue(Partition, strconv.Itoa(int(msg.Partition)))

	h.ep.DoProcess(context.Background(), h.router, exchange)
	session.MarkMessage(msg, "") // 标记消息已处理
}
