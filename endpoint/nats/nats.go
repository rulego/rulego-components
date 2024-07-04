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
	"github.com/nats-io/nats.go"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"net/textproto"
)

// Type 组件类型
const Type = types.EndpointTypePrefix + "nats"

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
	header := make(map[string][]string)
	header["topic"] = []string{r.request.Subject}
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

func (r *ResponseMessage) SetBody(body []byte) {
	r.body = body
	topic := r.Headers().Get("topic")
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
	Server string
	// NATS用户名
	Username string
	// NATS密码
	Password string
}

// Nats NATS接收端端点
type Nats struct {
	impl.BaseEndpoint
	RuleConfig types.Config
	//Config 配置
	Config Config
	// NATS连接
	conn *nats.Conn
	// 订阅映射关系，用于取消订阅
	subscriptions map[string]*nats.Subscription
}

// Type 组件类型
func (n *Nats) Type() string {
	return Type
}

func (n *Nats) Id() string {
	return n.Config.Server
}

func (n *Nats) New() types.Node {
	return &Nats{
		Config: Config{
			Server: "nats://127.0.0.1:4222",
		},
	}
}

// Init 初始化
func (n *Nats) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &n.Config)
	n.RuleConfig = ruleConfig
	return err
}

// Destroy 销毁
func (n *Nats) Destroy() {
	_ = n.Close()
}

func (n *Nats) Close() error {
	if nil != n.conn {
		n.conn.Close()
	}
	n.BaseEndpoint.Destroy()
	return nil
}

func (n *Nats) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router cannot be nil")
	}
	// 初始化NATS客户端
	if err := n.initNatsClient(); err != nil {
		return "", err
	}

	routerId := router.GetId()
	if routerId == "" {
		routerId = router.GetFrom().ToString()
		router.SetId(routerId)
	}

	subscription, err := n.conn.Subscribe(router.GetFrom().ToString(), func(msg *nats.Msg) {
		exchange := &endpointApi.Exchange{
			In: &RequestMessage{
				request: msg,
			},
			Out: &ResponseMessage{
				request:  msg,
				response: n.conn,
				log: func(format string, v ...interface{}) {
					n.Printf(format, v...)
				},
			},
		}
		n.DoProcess(context.Background(), router, exchange)
	})
	if err != nil {
		return "", err
	}

	n.subscriptions[routerId] = subscription
	return routerId, nil
}

func (n *Nats) RemoveRouter(routerId string, params ...interface{}) error {
	if subscription, ok := n.subscriptions[routerId]; ok {
		delete(n.subscriptions, routerId)
		return subscription.Unsubscribe()
	}
	return errors.New("router not found")
}

func (n *Nats) Start() error {
	return n.initNatsClient()
}

// initNatsClient 初始化NATS客户端
func (n *Nats) initNatsClient() error {
	if n.conn == nil {
		conn, err := nats.Connect(n.Config.Server, nats.UserInfo(n.Config.Username, n.Config.Password))
		if err != nil {
			return err
		}
		n.conn = conn
		n.subscriptions = make(map[string]*nats.Subscription)
	}
	return nil
}

func (n *Nats) Printf(format string, v ...interface{}) {
	if n.RuleConfig.Logger != nil {
		n.RuleConfig.Logger.Printf(format, v...)
	}
}
