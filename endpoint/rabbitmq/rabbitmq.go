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

package rabbitmq

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
	"net/textproto"
)

const (
	Type               = types.EndpointTypePrefix + "rabbitmq"
	KeyRequestTopic    = "topic"
	KeyRequestKey      = "key"
	KeyRequestExchange = "exchange"

	// KeyResponseExchange 响应交换机metadataKey
	KeyResponseExchange = "responseExchange"
	// KeyResponseTopic 响应主题metadataKey
	KeyResponseTopic = "responseTopic"
	// KeyResponseKey 响应主题metadataKey
	KeyResponseKey = "responseKey"

	JsonContextType = "application/json"
)

const (
	ContentTypeJson = "application/json"
	ContentTypeText = "text/plain"

	KeyContentType = "Content-Type"
	KeyUTF8        = "utf-8"
)

var _ endpointApi.Endpoint = (*Endpoint)(nil)

type Endpoint = RabbitMQ

func init() {
	_ = endpoint.Registry.Register(&Endpoint{})
}

type RequestMessage struct {
	exchange string
	delivery amqp.Delivery
	headers  textproto.MIMEHeader
	body     []byte
	msg      *types.RuleMsg
	err      error
}

func (r *RequestMessage) Body() []byte {
	return r.body
}

func (r *RequestMessage) Headers() textproto.MIMEHeader {
	if r.headers == nil {
		r.headers = make(map[string][]string)
	}
	return r.headers
}

func (r *RequestMessage) From() string {
	return r.delivery.RoutingKey
}

func (r *RequestMessage) GetParam(key string) string {
	return ""
}

func (r *RequestMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

func (r *RequestMessage) GetMsg() *types.RuleMsg {
	if r.msg == nil {
		dataType := types.TEXT
		if r.delivery.ContentType == "" || r.delivery.ContentType == JsonContextType {
			dataType = types.JSON
		}
		ruleMsg := types.NewMsg(0, r.From(), dataType, types.NewMetadata(), string(r.Body()))
		ruleMsg.Metadata.PutValue(KeyRequestTopic, r.From())
		ruleMsg.Metadata.PutValue(KeyRequestKey, r.From())
		ruleMsg.Metadata.PutValue(KeyRequestExchange, r.exchange)
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

type ResponseMessage struct {
	channel    *amqp.Channel
	exchange   string
	routingKey string
	body       []byte
	headers    textproto.MIMEHeader
	msg        *types.RuleMsg
	log        func(format string, v ...interface{})
	err        error
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
	return r.routingKey
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
	topic := r.getMetadataValue(KeyResponseTopic, KeyResponseTopic)
	if topic == "" {
		topic = r.getMetadataValue(KeyResponseKey, KeyResponseKey)
	}
	if topic != "" {
		exchange := r.getMetadataValue(KeyResponseExchange, KeyResponseExchange)
		if exchange == "" {
			exchange = r.exchange
		}
		err := r.channel.Publish(exchange, topic, false, false,
			amqp.Publishing{
				ContentType:     getContentType(r.GetMsg()),
				ContentEncoding: KeyUTF8,
				Body:            body,
			})
		if err != nil {
			r.log(fmt.Sprintf("Failed to publish message to key %s: %v", topic, err))
		}
	}
}

func (r *ResponseMessage) SetError(err error) {
	r.err = err
}

func (r *ResponseMessage) GetError() error {
	return r.err
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

type Config struct {
	Server string
	// 交换机名称
	Exchange string
	// 交换机类型 如果交换机不存在，则会使用该类型创建，默认为direct
	ExchangeType string
	//表示交换器是否持久化。如果设置为 true，即使消息服务器重启，交换器也会被保留。
	Durable bool
	//表示交换器是否自动删除。如果设置为 true，则当没有绑定的队列时，交换器会被自动删除。
	AutoDelete bool
}

type RabbitMQ struct {
	impl.BaseEndpoint
	RuleConfig types.Config
	Config     Config
	conn       *amqp.Connection
	channels   map[string]*amqp.Channel
}

func (e *RabbitMQ) Type() string {
	return Type
}

func (e *RabbitMQ) Id() string {
	return e.Config.Server
}

func (e *RabbitMQ) New() types.Node {
	return &RabbitMQ{
		Config: Config{
			Server:       "amqp://guest:guest@127.0.0.1:5672/",
			Exchange:     "rulego",
			ExchangeType: "topic",
			Durable:      true,
			AutoDelete:   true,
		},
		channels: make(map[string]*amqp.Channel),
	}
}

func (e *RabbitMQ) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &e.Config)
	e.RuleConfig = ruleConfig
	if e.Config.ExchangeType == "" {
		e.Config.ExchangeType = "direct"
	}
	return err
}

func (e *RabbitMQ) Destroy() {
	_ = e.Close()
}

func (e *RabbitMQ) Close() error {
	if e.conn != nil {
		return e.conn.Close()
	}
	e.Lock()
	defer e.Unlock()
	for _, ch := range e.channels {
		_ = ch.Close()
	}
	e.channels = map[string]*amqp.Channel{}
	return nil
}

func (e *RabbitMQ) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	routerId := e.CheckAndSetRouterId(router)

	e.RLock()
	_, ok := e.channels[routerId]
	e.RUnlock()
	if ok {
		return routerId, fmt.Errorf("routerId %s already exists", routerId)
	}

	if ch, q, err := e.queueBind(router.FromToString()); err != nil {
		return "", err
	} else {
		e.Lock()
		defer e.Unlock()
		if e.channels == nil {
			e.channels = make(map[string]*amqp.Channel)
		}
		e.channels[routerId] = ch
		msgs, err := ch.Consume(
			q.Name, // Queue name
			"",     // Consumer tag
			true,   // Auto-ack (acknowledgment of message receipt)
			false,  // Exclusive
			false,  // No-local
			false,  // No-wait
			nil,    // Arguments
		)
		if err != nil {
			return "", err
		}
		go func(router endpointApi.Router, ch *amqp.Channel) {
			for msg := range msgs {
				// 处理消息逻辑
				if e.RuleConfig.Pool != nil {
					submitErr := e.RuleConfig.Pool.Submit(func() {
						e.handlerMsg(router, ch, msg)
					})
					if submitErr != nil {
						e.Printf("rabbitmq consumer handler err :%v", submitErr)
					}
				} else {
					go e.handlerMsg(router, ch, msg)
				}

			}
		}(router, ch)
		return routerId, nil
	}
}

func (e *RabbitMQ) RemoveRouter(routerId string, params ...interface{}) error {
	if e.channels == nil {
		return nil
	}
	e.Lock()
	defer e.Unlock()
	if ch, ok := e.channels[routerId]; ok {
		delete(e.channels, routerId)
		return ch.Close()
	}
	return nil
}

func (e *RabbitMQ) Start() error {
	return e.initClient()
}

func (e *RabbitMQ) Printf(format string, v ...interface{}) {
	if e.RuleConfig.Logger != nil {
		e.RuleConfig.Logger.Printf(format, v...)
	}
}

func (e *RabbitMQ) initClient() error {
	conn, err := amqp.Dial(e.Config.Server)
	e.conn = conn
	return err
}

func (e *RabbitMQ) queueBind(key string) (*amqp.Channel, *amqp.Queue, error) {
	if e.conn == nil {
		if err := e.initClient(); err != nil {
			return nil, nil, err
		}
	}
	ch, err := e.conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	q, err := ch.QueueDeclare("", e.Config.Durable, e.Config.AutoDelete, true, false, nil)
	if err != nil {
		return nil, nil, err
	}

	err = ch.QueueBind(q.Name, key, e.Config.Exchange, false, nil)
	if err != nil && err.(*amqp.Error).Code == 404 {
		ch, err = e.conn.Channel()
		err = ch.ExchangeDeclare(e.Config.Exchange, e.Config.ExchangeType, e.Config.Durable, e.Config.AutoDelete, false, false, nil)
		if err != nil {
			ch, err = e.conn.Channel()
		}
		err = ch.QueueBind(q.Name, key, e.Config.Exchange, false, nil)
	}
	return ch, &q, err
}

func (e *RabbitMQ) handlerMsg(router endpointApi.Router, ch *amqp.Channel, msg amqp.Delivery) {
	defer func() {
		if err := recover(); err != nil {
			e.Printf("rabbitmq endpoint handler err :\n%v", runtime.Stack())
		}
	}()
	exchange := &endpointApi.Exchange{
		In: &RequestMessage{
			delivery: msg,
			exchange: e.Config.Exchange,
			body:     msg.Body,
		},
		Out: &ResponseMessage{
			channel:    ch,
			exchange:   e.Config.Exchange,
			routingKey: msg.RoutingKey,
			log: func(format string, v ...interface{}) {
				e.Printf(format, v...)
			},
		},
	}
	e.DoProcess(context.Background(), router, exchange)
}

func getContentType(msg *types.RuleMsg) string {
	if msg == nil {
		return ContentTypeText
	}
	contentType := msg.Metadata.GetValue(KeyContentType)
	if contentType != "" {
		return contentType
	} else if msg.DataType == types.JSON {
		return ContentTypeJson
	} else {
		return ContentTypeText
	}
}
