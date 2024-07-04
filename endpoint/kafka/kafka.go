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
	"github.com/IBM/sarama"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"net/textproto"
	"strconv"
)

// Type 组件类型
const Type = types.EndpointTypePrefix + "kafka"

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
	header := make(map[string][]string)
	header["topic"] = []string{r.request.Topic}
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

		ruleMsg.Metadata.PutValue("topic", r.From())

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
	topic := r.Headers().Get("topic")
	if topic != "" {
		key := r.Headers().Get("key")
		partitionStr := r.Headers().Get("partition")
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
}

// Kafka Kafka 接收端端点
type Kafka struct {
	impl.BaseEndpoint
	RuleConfig types.Config
	//Config 配置
	Config Config
	//消息消费者
	consumer sarama.Consumer
	//消息生产者，用于响应
	producer sarama.SyncProducer
	// 主题和主题消费者映射关系，用于取消订阅
	topics map[string]sarama.PartitionConsumer
}

// Type 组件类型
func (k *Kafka) Type() string {
	return Type
}

func (k *Kafka) New() types.Node {
	return &Kafka{
		Config: Config{
			Brokers: []string{"localhost:9092"},
		},
	}
}

// Init 初始化
func (k *Kafka) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &k.Config)
	k.RuleConfig = ruleConfig
	return err
}

// Destroy 销毁
func (k *Kafka) Destroy() {
	_ = k.Close()
}

func (k *Kafka) Close() error {
	if nil != k.consumer {
		return k.consumer.Close()
	}
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
	var partition = int32(0)
	//指定分区
	if len(params) > 0 {
		if v1, ok1 := params[0].(int32); ok1 {
			partition = v1
		} else if v2, ok2 := params[0].(int); ok2 {
			partition = int32(v2)
		}
	}
	if err := k.createTopicConsumer(router, partition); err != nil {
		return "", err
	}
	return router.GetId(), nil
}

func (k *Kafka) RemoveRouter(routerId string, params ...interface{}) error {
	k.Lock()
	defer k.Unlock()
	//删除订阅
	if v, ok := k.topics[routerId]; ok {
		delete(k.topics, routerId)
		return v.Close()
	}
	return nil
}

func (k *Kafka) Start() error {
	return k.initKafkaClient()
}

// initKafkaClient 初始化kafka客户端
func (k *Kafka) initKafkaClient() error {
	if k.consumer == nil {
		config := sarama.NewConfig()
		consumer, err := sarama.NewConsumer(k.Config.Brokers, config)
		if err != nil {
			return err
		}
		k.consumer = consumer
	}
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
func (k *Kafka) createTopicConsumer(router endpointApi.Router, partition int32) error {
	if form := router.GetFrom(); form != nil {
		partitionConsumer, err := k.consumer.ConsumePartition(form.ToString(), partition, sarama.OffsetNewest)
		if err != nil {
			k.Printf("failed to start consumer for topic %s: %v", form.ToString(), err)
			return err
		}
		k.Lock()
		if k.topics == nil {
			k.topics = make(map[string]sarama.PartitionConsumer)
		}
		k.topics[router.FromToString()] = partitionConsumer
		defer k.Unlock()
		go k.handleMessages(partitionConsumer, router)
	}
	return nil
}

// 处理订阅消息
func (k *Kafka) handleMessages(partitionConsumer sarama.PartitionConsumer, router endpointApi.Router) {
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			k.Printf("failed to close partition consumer: %v", err)
		}
	}()
	for msg := range partitionConsumer.Messages() { // loop until partition consumer is closed or context is canceled
		exchange := &endpointApi.Exchange{
			In: &RequestMessage{
				request: msg,
			},
			Out: &ResponseMessage{
				request:  msg,
				response: k.producer,
				log: func(format string, v ...interface{}) {
					k.Printf(format, v...)
				},
			}}

		k.DoProcess(context.Background(), router, exchange)
	}
}

func (k *Kafka) Printf(format string, v ...interface{}) {
	if k.RuleConfig.Logger != nil {
		k.RuleConfig.Logger.Printf(format, v...)
	}
}
