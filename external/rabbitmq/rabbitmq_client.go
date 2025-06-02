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
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

const (
	ContentTypeJson = "application/json"
	ContentTypeText = "text/plain"

	KeyContentType = "Content-Type"
	KeyUTF8        = "utf-8"
)

func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

type ClientNodeConfiguration struct {
	// RabbitMQ服务器地址，格式为"amqp://用户名:密码@服务器地址:端口号"
	Server string
	// 路由键
	Key string
	// 交换机名称
	Exchange string
	// 交换机类型 direct, fanout, topic
	ExchangeType string
	//表示交换器是否持久化。如果设置为 true，即使消息服务器重启，交换器也会被保留。
	Durable bool
	//表示交换器是否自动删除。如果设置为 true，则当没有绑定的队列时，交换器会被自动删除。
	AutoDelete bool
}

type ClientNode struct {
	base.SharedNode[*amqp.Connection]
	// 节点配置
	Config      ClientNodeConfiguration
	amqpConn    *amqp.Connection
	amqpChannel *amqp.Channel
	// 是否正在连接RabbitMQ服务器
	connecting int32
	// 路由键模板
	keyTemplate str.Template
}

// Type 组件类型
func (x *ClientNode) Type() string {
	return "x/rabbitmqClient"
}

func (x *ClientNode) New() types.Node {
	return &ClientNode{Config: ClientNodeConfiguration{
		Server:       "amqp://guest:guest@127.0.0.1:5672/",
		Exchange:     "rulego",
		ExchangeType: "topic",
		Durable:      true,
		AutoDelete:   true,
		Key:          "device.msg.request",
	}}
}

// Init 初始化
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	_ = x.SharedNode.Init(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*amqp.Connection, error) {
		return x.initClient()
	})
	x.keyTemplate = str.NewTemplate(x.Config.Key)
	return nil
}

// OnMsg 处理消息
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	var evn map[string]interface{}
	if !x.keyTemplate.IsNotVar() {
		evn = base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	}
	key := x.keyTemplate.Execute(evn)

	ch, err := x.checkChannel()
	if err == nil {
		err = ch.Publish(x.Config.Exchange, key, false, false,
			amqp.Publishing{
				ContentType:     x.getContentType(msg),
				ContentEncoding: KeyUTF8,
				Body:            []byte(msg.GetData()),
			})
	}

	if err != nil {
		ctx.TellFailure(msg, err)
	} else {
		ctx.TellSuccess(msg)
	}
}

// Destroy 销毁
func (x *ClientNode) Destroy() {
	if x.amqpChannel != nil {
		_ = x.amqpChannel.Close()
	}
	if x.amqpConn != nil {
		_ = x.amqpConn.Close()
	}
}

func (x *ClientNode) getContentType(msg types.RuleMsg) string {
	contentType := msg.Metadata.GetValue(KeyContentType)
	if contentType != "" {
		return contentType
	} else if msg.DataType == types.JSON {
		return ContentTypeJson
	} else {
		return ContentTypeText
	}
}

func (x *ClientNode) initClient() (*amqp.Connection, error) {
	if x.amqpConn != nil && !x.amqpConn.IsClosed() {
		return x.amqpConn, nil
	} else {
		x.Locker.Lock()
		defer x.Locker.Unlock()
		if x.amqpConn != nil && !x.amqpConn.IsClosed() {
			return x.amqpConn, nil
		}
		var err error
		x.amqpConn, err = amqp.Dial(x.Config.Server)
		return x.amqpConn, err
	}
}

func (x *ClientNode) checkChannel() (*amqp.Channel, error) {
	if x.amqpChannel != nil && !x.amqpChannel.IsClosed() {
		return x.amqpChannel, nil
	}
	var err error
	var conn *amqp.Connection
	conn, err = x.SharedNode.Get()
	if err != nil {
		return nil, err
	}
	x.Locker.Lock()
	defer x.Locker.Unlock()
	if x.amqpChannel != nil && !x.amqpChannel.IsClosed() {
		return x.amqpChannel, nil
	}
	x.amqpChannel, err = conn.Channel()
	if err != nil {
		return nil, err
	}
	if x.Config.Exchange != "" {
		//声明交换机
		err = x.amqpChannel.ExchangeDeclare(
			x.Config.Exchange,     // 交换机名称
			x.Config.ExchangeType, // 交换机类型
			x.Config.Durable,      //是否持久化
			x.Config.AutoDelete,   //是否自动删除
			false,
			false,
			nil,
		)
		if err != nil {
			//如果交换机已经存在，则不再声明，重新创建通道
			x.amqpChannel, err = conn.Channel()
			if err != nil {
				return nil, err
			}
		}
	}

	return x.amqpChannel, err
}
