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
	"errors"
	"github.com/nats-io/nats.go"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
	"sync/atomic"
)

// ClientNotInitErr 表示NATS客户端未初始化的错误
var ClientNotInitErr = errors.New("nats client not initialized")

func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

type ClientNodeConfiguration struct {
	// 发布主题
	Topic string
	// NATS服务器地址
	Server string
	// NATS用户名
	Username string
	// NATS密码
	Password string
}

type ClientNode struct {
	// 节点配置
	Config     ClientNodeConfiguration
	natsClient *nats.Conn
	// 是否正在连接NATS服务器
	connecting int32
}

// Type 组件类型
func (x *ClientNode) Type() string {
	return "x/natsClient"
}

func (x *ClientNode) New() types.Node {
	return &ClientNode{Config: ClientNodeConfiguration{
		Topic:  "/device/msg",
		Server: "nats://127.0.0.1:4222",
	}}
}

// Init 初始化
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		_ = x.tryInitClient()
	}
	return err
}

// OnMsg 处理消息
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	topic := str.SprintfDict(x.Config.Topic, msg.Metadata.Values())
	if x.natsClient == nil {
		if err := x.tryInitClient(); err != nil {
			ctx.TellFailure(msg, err)
		} else {
			ctx.TellFailure(msg, ClientNotInitErr)
		}
	} else if err := x.natsClient.Publish(topic, []byte(msg.Data)); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		ctx.TellSuccess(msg)
	}
}

// Destroy 销毁
func (x *ClientNode) Destroy() {
	if x.natsClient != nil {
		x.natsClient.Close()
	}
}

func (x *ClientNode) isConnecting() bool {
	return atomic.LoadInt32(&x.connecting) == 1
}

// TryInitClient 尝试初始化NATS客户端
func (x *ClientNode) tryInitClient() error {
	if x.natsClient == nil && atomic.CompareAndSwapInt32(&x.connecting, 0, 1) {
		defer atomic.StoreInt32(&x.connecting, 0)
		var err error
		x.natsClient, err = nats.Connect(x.Config.Server, nats.UserInfo(x.Config.Username, x.Config.Password))
		return err
	}
	return nil
}
