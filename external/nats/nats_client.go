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
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
)

func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

type ClientNodeConfiguration struct {
	// NATS服务器地址
	Server string `json:"server"`
	// NATS用户名
	Username string `json:"username"`
	// NATS密码
	Password string `json:"password"`
	// 发布主题
	Topic string `json:"topic"`
}

type ClientNode struct {
	base.SharedNode[*nats.Conn]
	// 节点配置
	Config ClientNodeConfiguration
	// 是否正在连接NATS服务器
	connecting int32
	// topicTemplate 主题模板，用于解析动态主题
	// topicTemplate template for resolving dynamic topic
	topicTemplate el.Template
	// hasVar 标识模板是否包含变量
	hasVar bool
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
		_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*nats.Conn, error) {
			return x.initClient()
		}, func(client *nats.Conn) error {
			// 清理回调函数
			client.Close()
			return nil
		})
		x.topicTemplate, err = el.NewTemplate(x.Config.Topic)
		if err != nil {
			return err
		}
		// 检查模板是否包含变量
		x.hasVar = x.topicTemplate.HasVar()
	}
	return err
}

// OnMsg 处理消息
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	var topic string
	if x.hasVar {
		topic = x.topicTemplate.ExecuteAsString(base.NodeUtils.GetEvnAndMetadata(ctx, msg))
	} else {
		topic = x.Config.Topic
	}
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	if err := client.Publish(topic, []byte(msg.GetData())); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		ctx.TellSuccess(msg)
	}
}

func (x *ClientNode) Destroy() {
	_ = x.SharedNode.Close()
}

func (x *ClientNode) initClient() (*nats.Conn, error) {
	client, err := nats.Connect(x.Config.Server, nats.UserInfo(x.Config.Username, x.Config.Password))
	return client, err
}
