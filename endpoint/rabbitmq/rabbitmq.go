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
	"net/textproto"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
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
	base.SharedNode[*amqp.Connection]
	// GracefulShutdown provides graceful shutdown capabilities
	// GracefulShutdown 提供优雅停机功能
	base.GracefulShutdown
	RuleConfig types.Config
	Config     Config
	channels   map[string]*amqp.Channel
}

func (x *RabbitMQ) Type() string {
	return Type
}

func (x *RabbitMQ) Id() string {
	return x.Config.Server
}

func (x *RabbitMQ) New() types.Node {
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

func (x *RabbitMQ) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	x.RuleConfig = ruleConfig

	// 初始化优雅停机功能
	x.GracefulShutdown.InitGracefulShutdown(x.RuleConfig.Logger, 0)

	if x.Config.ExchangeType == "" {
		x.Config.ExchangeType = "direct"
	}
	_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, true, func() (*amqp.Connection, error) {
		return x.initClient()
	}, func(conn *amqp.Connection) error {
		if conn != nil && !conn.IsClosed() {
			return conn.Close()
		}
		return nil
	})
	return err
}

func (x *RabbitMQ) Destroy() {
	x.GracefulShutdown.GracefulStop(func() {
		_ = x.Close()
	})
}

// GracefulStop provides graceful shutdown for the RabbitMQ endpoint
// GracefulStop 为 RabbitMQ 端点提供优雅停机
func (x *RabbitMQ) GracefulStop() {
	x.GracefulShutdown.GracefulStop(func() {
		_ = x.Close()
	})
}

func (x *RabbitMQ) Close() error {
	// SharedNode 会通过 InitWithClose 中的清理函数来管理客户端的关闭
	// SharedNode manages client closure through the cleanup function in InitWithClose
	_ = x.SharedNode.Close()
	x.Lock()
	defer x.Unlock()
	for _, ch := range x.channels {
		_ = ch.Close()
	}
	x.channels = map[string]*amqp.Channel{}
	return nil
}

func (x *RabbitMQ) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	routerId := x.CheckAndSetRouterId(router)

	x.RLock()
	_, ok := x.channels[routerId]
	x.RUnlock()
	if ok {
		return routerId, fmt.Errorf("routerId %s already exists", routerId)
	}

	if ch, q, err := x.queueBind(router.FromToString()); err != nil {
		return "", err
	} else {
		x.Lock()
		defer x.Unlock()
		if x.channels == nil {
			x.channels = make(map[string]*amqp.Channel)
		}
		x.channels[routerId] = ch
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
				if x.RuleConfig.Pool != nil {
					submitErr := x.RuleConfig.Pool.Submit(func() {
						x.handlerMsg(router, ch, msg)
					})
					if submitErr != nil {
						x.Printf("rabbitmq consumer handler err :%v", submitErr)
					}
				} else {
					go x.handlerMsg(router, ch, msg)
				}

			}
		}(router, ch)
		return routerId, nil
	}
}

func (x *RabbitMQ) RemoveRouter(routerId string, params ...interface{}) error {
	if x.channels == nil {
		return nil
	}
	x.Lock()
	defer x.Unlock()
	if ch, ok := x.channels[routerId]; ok {
		delete(x.channels, routerId)
		return ch.Close()
	}
	return nil
}

func (x *RabbitMQ) Start() error {
	if !x.SharedNode.IsInit() {
		return x.SharedNode.InitWithClose(x.RuleConfig, x.Type(), x.Config.Server, true, func() (*amqp.Connection, error) {
			return x.initClient()
		}, func(conn *amqp.Connection) error {
			if conn != nil && !conn.IsClosed() {
				return conn.Close()
			}
			return nil
		})
	}
	return nil
}

func (x *RabbitMQ) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}

func (x *RabbitMQ) initClient() (*amqp.Connection, error) {
	conn, err := amqp.Dial(x.Config.Server)
	return conn, err
}

func (x *RabbitMQ) queueBind(key string) (*amqp.Channel, *amqp.Queue, error) {
	conn, err := x.SharedNode.GetSafely()
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	q, err := ch.QueueDeclare("", x.Config.Durable, x.Config.AutoDelete, true, false, nil)
	if err != nil {
		return nil, nil, err
	}

	err = ch.QueueBind(q.Name, key, x.Config.Exchange, false, nil)
	if err != nil && err.(*amqp.Error).Code == 404 {
		ch, err = conn.Channel()
		err = ch.ExchangeDeclare(x.Config.Exchange, x.Config.ExchangeType, x.Config.Durable, x.Config.AutoDelete, false, false, nil)
		if err != nil {
			ch, err = conn.Channel()
		}
		err = ch.QueueBind(q.Name, key, x.Config.Exchange, false, nil)
	}
	return ch, &q, err
}

func (x *RabbitMQ) handlerMsg(router endpointApi.Router, ch *amqp.Channel, msg amqp.Delivery) {
	defer func() {
		if err := recover(); err != nil {
			x.Printf("rabbitmq endpoint handler err :\n%v", runtime.Stack())
		}
	}()

	exchange := &endpointApi.Exchange{
		In: &RequestMessage{
			delivery: msg,
			exchange: x.Config.Exchange,
			body:     msg.Body,
		},
		Out: &ResponseMessage{
			channel:    ch,
			exchange:   x.Config.Exchange,
			routingKey: msg.RoutingKey,
			log: func(format string, v ...interface{}) {
				x.Printf(format, v...)
			},
		},
	}
	x.DoProcess(context.Background(), router, exchange)
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
