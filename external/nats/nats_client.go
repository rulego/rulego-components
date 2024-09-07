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
	"github.com/nats-io/nats.go"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

type ClientNodeConfiguration struct {
	// NATS服务器地址
	Server string
	// NATS用户名
	Username string
	// NATS密码
	Password string
	// 发布主题
	Topic string
}

type ClientNode struct {
	base.SharedNode[*nats.Conn]
	// 节点配置
	Config ClientNodeConfiguration
	client *nats.Conn
	// 是否正在连接NATS服务器
	connecting int32
	//topic 模板
	topicTemplate str.Template
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
		_ = x.SharedNode.Init(ruleConfig, x.Type(), x.Config.Server, true, func() (*nats.Conn, error) {
			return x.initClient()
		})
		x.topicTemplate = str.NewTemplate(x.Config.Topic)
	}
	return err
}

// OnMsg 处理消息
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	topic := x.topicTemplate.ExecuteFn(func() map[string]any {
		return base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	})
	client, err := x.SharedNode.Get()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	if err := client.Publish(topic, []byte(msg.Data)); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		ctx.TellSuccess(msg)
	}
}

// Destroy 销毁
func (x *ClientNode) Destroy() {
	if x.client != nil {
		x.client.Close()
	}
}

func (x *ClientNode) initClient() (*nats.Conn, error) {
	if x.client != nil {
		return x.client, nil
	} else {
		x.Locker.Lock()
		defer x.Locker.Unlock()
		if x.client != nil {
			return x.client, nil
		}
		var err error
		x.client, err = nats.Connect(x.Config.Server, nats.UserInfo(x.Config.Username, x.Config.Password))
		return x.client, err
	}
}
