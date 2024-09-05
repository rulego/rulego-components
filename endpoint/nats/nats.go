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
	"github.com/nats-io/nats.go"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
	"net/textproto"
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
	Server string
	// NATS用户名
	Username string
	// NATS密码
	Password string
}

// Nats NATS接收端端点
type Nats struct {
	impl.BaseEndpoint
	base.SharedNode[*nats.Conn]
	RuleConfig types.Config
	//Config 配置
	Config Config
	// NATS连接
	conn *nats.Conn
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
	_ = x.SharedNode.Init(x.RuleConfig, x.Type(), x.Config.Server, true, func() (*nats.Conn, error) {
		return x.initClient()
	})
	return err
}

// Destroy 销毁
func (x *Nats) Destroy() {
	_ = x.Close()
}

func (x *Nats) Close() error {
	if nil != x.conn {
		x.conn.Close()
	}
	x.BaseEndpoint.Destroy()
	return nil
}

func (x *Nats) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router cannot be nil")
	}
	client, err := x.SharedNode.Get()
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
	subscription, err := client.Subscribe(router.FromToString(), func(msg *nats.Msg) {
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
	})
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
		return x.SharedNode.Init(x.RuleConfig, x.Type(), x.Config.Server, true, func() (*nats.Conn, error) {
			return x.initClient()
		})
	}
	return nil
}

func (x *Nats) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}

func (x *Nats) initClient() (*nats.Conn, error) {
	if x.conn != nil {
		return x.conn, nil
	} else {
		x.Locker.Lock()
		defer x.Locker.Unlock()
		if x.conn != nil {
			return x.conn, nil
		}
		var err error
		x.conn, err = nats.Connect(x.Config.Server, nats.UserInfo(x.Config.Username, x.Config.Password))
		return x.conn, err
	}
}
