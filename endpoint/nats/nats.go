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

package nats

import (
	"context"
	"errors"
	"fmt"
	"net/textproto"

	"github.com/nats-io/nats.go"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
)

// Type 组件类型
const Type = types.EndpointTypePrefix + "nats"

// KeyResponseTopic 响应主题metadataKey
const KeyResponseTopic = "responseTopic"

// Endpoint 别名
type Endpoint = Nats

var _ endpointApi.Endpoint = (*Endpoint)(nil)

// 注册组件
func init() {
	_ = endpoint.Registry.Register(&Endpoint{})
}

// RequestMessage 请求消息
type RequestMessage struct {
	request *nats.Msg
	msg     *types.RuleMsg
	err     error
}

func (r *RequestMessage) Body() []byte {
	return r.request.Data
}

func (r *RequestMessage) Headers() textproto.MIMEHeader {
	header := make(textproto.MIMEHeader)
	header.Set("topic", r.request.Subject)
	return header
}

func (r *RequestMessage) From() string {
	return r.request.Subject
}

func (r *RequestMessage) GetParam(key string) string {
	return ""
}

func (r *RequestMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

func (r *RequestMessage) GetMsg() *types.RuleMsg {
	if r.msg == nil {
		// 默认指定是JSON格式，如果不是该类型，请在process函数中修改
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

// ResponseMessage 响应消息
type ResponseMessage struct {
	request  *nats.Msg
	response *nats.Conn
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
	return r.request.Subject
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
		err := r.response.Publish(topic, r.body)
		if err != nil {
			r.SetError(err)
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
	// NATS服务器地址
	Server string `json:"server"`
	// NATS用户名
	Username string `json:"username"`
	// NATS密码
	Password string `json:"password"`
	// GroupId 消费者组Id
	// 组ID不为空，则使用 QueueSubscribe 模式，
	// 即多个消费者共享订阅主题，消息在组内以 负载均衡 方式分发，确保每条消息仅被组内一个消费者处理
	GroupId string `json:"groupId"`
}

// Nats NATS接收端端点
type Nats struct {
	impl.BaseEndpoint
	base.SharedNode[*nats.Conn]
	// GracefulShutdown provides graceful shutdown capabilities
	// GracefulShutdown 提供优雅停机功能
	base.GracefulShutdown
	RuleConfig types.Config
	//Config 配置
	Config Config
	// 订阅映射关系，用于取消订阅
	subscriptions map[string]*nats.Subscription
}

// Type 组件类型
func (x *Nats) Type() string {
	return Type
}

func (x *Nats) Id() string {
	return x.Config.Server
}

func (x *Nats) New() types.Node {
	return &Nats{
		Config: Config{
			Server: "nats://127.0.0.1:4222",
		},
	}
}

// Init 初始化
func (x *Nats) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	x.RuleConfig = ruleConfig

	// 初始化优雅停机功能
	x.GracefulShutdown.InitGracefulShutdown(x.RuleConfig.Logger, 0)

	_ = x.SharedNode.InitWithClose(x.RuleConfig, x.Type(), x.Config.Server, true, func() (*nats.Conn, error) {
		return x.initClient()
	}, func(client *nats.Conn) error {
		if client != nil {
			client.Close()
		}
		return nil
	})
	return err
}

// Destroy 销毁
func (x *Nats) Destroy() {
	x.GracefulShutdown.GracefulStop(func() {
		_ = x.Close()
	})
}

// GracefulStop provides graceful shutdown for the NATS endpoint
// GracefulStop 为 NATS 端点提供优雅停机
func (x *Nats) GracefulStop() {
	x.GracefulShutdown.GracefulStop(func() {
		_ = x.Close()
	})
}

func (x *Nats) Close() error {
	// SharedNode 会通过 InitWithClose 中的清理函数来管理客户端的关闭
	// SharedNode manages client closure through the cleanup function in InitWithClose
	_ = x.SharedNode.Close()
	x.BaseEndpoint.Destroy()
	return nil
}

func (x *Nats) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
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
	x.Lock()
	defer x.Unlock()
	if x.subscriptions == nil {
		x.subscriptions = make(map[string]*nats.Subscription)
	}
	if _, ok := x.subscriptions[routerId]; ok {
		return routerId, fmt.Errorf("routerId %s already exists", routerId)
	}

	var subscription *nats.Subscription
	if x.Config.GroupId != "" {
		subscription, err = client.QueueSubscribe(router.FromToString(), x.Config.GroupId, x.createMsgHandler(client, router))
	} else {
		subscription, err = client.Subscribe(router.FromToString(), x.createMsgHandler(client, router))
	}
	if err != nil {
		return "", err
	}
	x.subscriptions[routerId] = subscription
	return routerId, nil
}

func (x *Nats) RemoveRouter(routerId string, params ...interface{}) error {
	x.Lock()
	defer x.Unlock()
	if subscription, ok := x.subscriptions[routerId]; ok {
		delete(x.subscriptions, routerId)
		return subscription.Unsubscribe()
	}
	return errors.New("router not found")
}

func (x *Nats) Start() error {
	if !x.SharedNode.IsInit() {
		return x.SharedNode.InitWithClose(x.RuleConfig, x.Type(), x.Config.Server, true, func() (*nats.Conn, error) {
			return x.initClient()
		}, func(client *nats.Conn) error {
			if client != nil {
				client.Close()
			}
			return nil
		})
	}
	return nil
}

// createMsgHandler 创建NATS消息处理器
// 该函数返回一个处理NATS消息的回调函数，用于处理订阅的消息
func (x *Nats) createMsgHandler(client *nats.Conn, router endpointApi.Router) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		defer func() {
			if e := recover(); e != nil {
				x.Printf("nats endpoint handler err :\n%v", runtime.Stack())
			}
		}()

		exchange := &endpointApi.Exchange{
			In: &RequestMessage{
				request: msg,
			},
			Out: &ResponseMessage{
				request:  msg,
				response: client,
				log: func(format string, v ...interface{}) {
					x.Printf(format, v...)
				},
			},
		}
		x.DoProcess(context.Background(), router, exchange)
	}
}

func (x *Nats) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}

func (x *Nats) initClient() (*nats.Conn, error) {
	conn, err := nats.Connect(x.Config.Server, nats.UserInfo(x.Config.Username, x.Config.Password))
	return conn, err
}
