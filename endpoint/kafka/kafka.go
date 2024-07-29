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

func (r *ResponseMessage) SetBody(body []byte) {
	r.body = body
	topic := r.Headers().Get(Topic)
	if topic != "" {
		key := r.Headers().Get(Key)
		partitionStr := r.Headers().Get(Partition)
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
	// Brokers kafka服务器地址列表
	Brokers []string
	// GroupId 消费者组Id
	GroupId string
}

// Kafka Kafka 接收端端点
type Kafka struct {
	impl.BaseEndpoint
	RuleConfig types.Config
	//Config 配置
	Config Config
	//消息生产者，用于响应
	producer sarama.SyncProducer
	// 主题和主题消费者映射关系，用于取消订阅
	handlers map[string]sarama.ConsumerGroup
}

// Type 组件类型
func (k *Kafka) Type() string {
	return Type
}

func (k *Kafka) New() types.Node {
	return &Kafka{
		Config: Config{
			Brokers: []string{"127.0.0.1:9092"},
			GroupId: "rulego",
		},
	}
}

// Init 初始化
func (k *Kafka) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &k.Config)
	k.Config.GroupId = strings.TrimSpace(k.Config.GroupId)
	if k.Config.GroupId == "" {
		k.Config.GroupId = "rulego"
	}
	k.RuleConfig = ruleConfig
	return err
}

// Destroy 销毁
func (k *Kafka) Destroy() {
	_ = k.Close()
}

func (k *Kafka) Close() error {
	for _, v := range k.handlers {
		v.Close()
	}

	k.handlers = nil

	if nil != k.producer {
		return k.producer.Close()
	}
	k.BaseEndpoint.Destroy()
	return nil
}

func (k *Kafka) Id() string {
	if len(k.Config.Brokers) > 0 {
		return k.Config.Brokers[0]
	} else {
		return ""
	}
}

func (k *Kafka) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router can not nil")
	}
	//初始化kafka客户端
	if err := k.initKafkaClient(); err != nil {
		return "", err
	}

	if id := router.GetId(); id == "" {
		router.SetId(router.GetFrom().ToString())
	}
	if err := k.createTopicConsumer(router); err != nil {
		return "", err
	}
	return router.GetId(), nil
}

func (k *Kafka) RemoveRouter(routerId string, params ...interface{}) error {
	k.Lock()
	defer k.Unlock()
	//删除订阅
	if v, ok := k.handlers[routerId]; ok {
		delete(k.handlers, routerId)
		return v.Close()
	}
	return nil
}

func (k *Kafka) Start() error {
	return k.initKafkaClient()
}

// initKafkaClient 初始化kafka客户端
func (k *Kafka) initKafkaClient() error {
	if k.producer == nil {
		config := sarama.NewConfig()
		config.Producer.Return.Successes = true // 同步模式需要设置这个参数为true
		producer, err := sarama.NewSyncProducer(k.Config.Brokers, config)
		if err != nil {
			return err
		}
		k.producer = producer
	}

	return nil
}

// 创建kafka消费者
func (k *Kafka) createTopicConsumer(router endpointApi.Router) error {
	if form := router.GetFrom(); form != nil {
		routerId := router.GetId()
		if routerId == "" {
			routerId = router.GetFrom().ToString()
			router.SetId(routerId)
		}
		k.Lock()
		if k.handlers == nil {
			k.handlers = make(map[string]sarama.ConsumerGroup)
		}
		if _, ok := k.handlers[routerId]; ok {
			return fmt.Errorf("routerId %s already exists", routerId)
		}
		config := sarama.NewConfig()
		consumer, err := sarama.NewConsumerGroup(k.Config.Brokers, k.Config.GroupId, config)
		if err != nil {
			return err
		}
		k.handlers[routerId] = consumer
		defer k.Unlock()

		topics := []string{form.ToString()}                // 订阅的主题列表
		handler := &consumerHandler{router: router, ep: k} // 自定义的消费者处理程序

		go func() {
			defer consumer.Close()
			if err := consumer.Consume(context.Background(), topics, handler); err != nil {
				k.Printf("failed to start consumer for topic %s: %v", form.ToString(), err)
			}
		}()

	}
	return nil
}

func (k *Kafka) Printf(format string, v ...interface{}) {
	if k.RuleConfig.Logger != nil {
		k.RuleConfig.Logger.Printf(format, v...)
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
