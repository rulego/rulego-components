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
package wukongim

import (
	"context"
	"errors"
	"net/textproto"
	"time"

	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/WuKongIM/WuKongIMGoSDK/pkg/wksdk"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
)

// Type 组件类型
const Type = types.EndpointTypePrefix + "wukongim"

// Endpoint 别名
type Endpoint = Wukongim

var _ endpointApi.Endpoint = (*Endpoint)(nil)

// 注册组件
func init() {
	_ = endpoint.Registry.Register(&Endpoint{})
}

// RequestMessage http请求消息
type RequestMessage struct {
	headers textproto.MIMEHeader
	body    []byte
	msg     *types.RuleMsg
	err     error
}

func (r *RequestMessage) Body() []byte {
	return r.body
}

func (r *RequestMessage) Headers() textproto.MIMEHeader {
	header := make(textproto.MIMEHeader)
	return header
}

func (r *RequestMessage) From() string {
	return ""
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
	headers    textproto.MIMEHeader
	body       []byte
	msg        *types.RuleMsg
	statusCode int
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
	return ""
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
}

func (r *ResponseMessage) SetError(err error) {
	r.err = err
}

func (r *ResponseMessage) GetError() error {
	return r.err
}
func (r *ResponseMessage) SetStatusCode(statusCode int) {
	r.statusCode = statusCode
}

type Config struct {
	// Wukongim服务器地址列表
	Server string
	// 用户UID
	UID string
	// 登录密码
	Token string
	// 连接超时
	ConnectTimeout int64
	// Proto版本
	ProtoVersion int
	// 心跳间隔
	PingInterval int64
	// 是否自动重连
	Reconnect bool
}

// Wukongim 接收端端点
type Wukongim struct {
	impl.BaseEndpoint
	base.SharedNode[*wksdk.Client]
	RuleConfig types.Config
	//Config 配置
	Config Config
	client *wksdk.Client
	Router endpointApi.Router
}

// Type 组件类型
func (x *Wukongim) Type() string {
	return Type
}

func (x *Wukongim) New() types.Node {
	return &Wukongim{
		Config: Config{
			Server:         "tcp://127.0.0.1:5100",
			UID:            "test1",
			Token:          "test1",
			ConnectTimeout: 5,
			ProtoVersion:   wkproto.LatestVersion,
			PingInterval:   30,
			Reconnect:      true,
		},
	}
}

// Init 初始化
func (x *Wukongim) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	x.RuleConfig = ruleConfig
	_ = x.SharedNode.Init(x.RuleConfig, x.Type(), x.Config.Server, true, func() (*wksdk.Client, error) {
		return x.initClient()
	})
	return err
}

// Destroy 销毁组件
func (x *Wukongim) Destroy() {
	if x.client != nil {
		_ = x.client.Disconnect()
	}
}

func (x *Wukongim) Close() error {
	x.BaseEndpoint.Destroy()
	return nil
}

func (x *Wukongim) Id() string {
	return x.Config.Server
}

func (x *Wukongim) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router cannot be nil")
	}
	if x.Router != nil {
		return "", errors.New("duplicate router")
	}
	x.Router = router
	return router.GetId(), nil
}

func (x *Wukongim) RemoveRouter(routerId string, params ...interface{}) error {
	x.Lock()
	defer x.Unlock()
	x.Router = nil
	return nil
}

func (x *Wukongim) Start() error {
	var err error
	if !x.SharedNode.IsInit() {
		err = x.SharedNode.Init(x.RuleConfig, x.Type(), x.Config.Server, true, func() (*wksdk.Client, error) {
			return x.initClient()
		})
	}
	x.client.OnMessage(func(msg *wksdk.Message) {
		exchange := &endpoint.Exchange{
			In: &RequestMessage{body: []byte(string(msg.Payload))},
			Out: &ResponseMessage{
				body: []byte(string(msg.Payload)),
			}}
		x.DoProcess(context.Background(), x.Router, exchange)
	})
	return err
}

func (x *Wukongim) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}

func (x *Wukongim) initClient() (*wksdk.Client, error) {
	if x.client != nil {
		return x.client, nil
	} else {
		x.Locker.Lock()
		defer x.Locker.Unlock()
		if x.client != nil {
			return x.client, nil
		}
		x.client = wksdk.NewClient(x.Config.Server,
			wksdk.WithConnectTimeout(time.Duration(x.Config.ConnectTimeout)*time.Second),
			wksdk.WithProtoVersion(x.Config.ProtoVersion),
			wksdk.WithUID(x.Config.UID),
			wksdk.WithToken(x.Config.Token),
			wksdk.WithPingInterval(time.Duration(x.Config.PingInterval)*time.Second),
			wksdk.WithReconnect(x.Config.Reconnect),
		)
		err := x.client.Connect()
		return x.client, err
	}
}
