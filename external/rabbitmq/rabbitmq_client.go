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
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
)

const (
	ContentTypeJson = "application/json"
	ContentTypeText = "text/plain"

	KeyContentType = "Content-Type"
	KeyUTF8        = "utf-8"

	// 通道池配置
	DefaultChannelPoolSize = 10
	MaxChannelPoolSize     = 100
)

// channelPool 通道池结构
type channelPool struct {
	mu       sync.RWMutex
	channels chan *amqp.Channel
	factory  func() (*amqp.Channel, error)
	close    func(*amqp.Channel)
	maxSize  int
}

// newChannelPool 创建通道池
func newChannelPool(maxSize int, factory func() (*amqp.Channel, error)) *channelPool {
	if maxSize <= 0 {
		maxSize = DefaultChannelPoolSize
	}
	if maxSize > MaxChannelPoolSize {
		maxSize = MaxChannelPoolSize
	}

	return &channelPool{
		channels: make(chan *amqp.Channel, maxSize),
		factory:  factory,
		maxSize:  maxSize,
		close: func(ch *amqp.Channel) {
			if ch != nil && !ch.IsClosed() {
				ch.Close()
			}
		},
	}
}

// Get 从池中获取通道
func (p *channelPool) Get() (*amqp.Channel, error) {
	select {
	case ch := <-p.channels:
		if ch != nil && !ch.IsClosed() {
			return ch, nil
		}
		// 通道已关闭，创建新的
		return p.factory()
	default:
		// 池为空，创建新通道
		return p.factory()
	}
}

// Put 将通道放回池中
func (p *channelPool) Put(ch *amqp.Channel) {
	if ch == nil || ch.IsClosed() {
		return
	}

	select {
	case p.channels <- ch:
		// 成功放回池中
	default:
		// 池已满，关闭通道
		p.close(ch)
	}
}

// Close 关闭通道池
func (p *channelPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	close(p.channels)
	for ch := range p.channels {
		p.close(ch)
	}
}

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
	Config ClientNodeConfiguration
	// 通道池
	channelPool *channelPool
	// 路由键模板
	keyTemplate el.Template
	// 标识模板是否包含变量，用于性能优化
	hasVar bool
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

	// 初始化SharedNode
	_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*amqp.Connection, error) {
		return x.initClient()
	}, func(conn *amqp.Connection) error {
		// 清理回调函数
		if x.channelPool != nil {
			x.channelPool.Close()
			x.channelPool = nil
		}
		if conn != nil && !conn.IsClosed() {
			return conn.Close()
		}
		return nil
	})

	// 初始化通道池（使用默认大小）
	x.channelPool = newChannelPool(DefaultChannelPoolSize, func() (*amqp.Channel, error) {
		return x.createChannel()
	})

	// 初始化路由键模板
	template, err := el.NewTemplate(x.Config.Key)
	if err != nil {
		return err
	}
	x.keyTemplate = template
	x.hasVar = template.HasVar()
	return nil
}

// OnMsg 处理消息
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	var evn map[string]interface{}
	if x.hasVar {
		evn = base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	}
	key := x.keyTemplate.ExecuteAsString(evn)

	// 使用通道池获取通道
	ch, err := x.channelPool.Get()
	if err == nil {
		defer x.channelPool.Put(ch)

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
	_ = x.SharedNode.Close()
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
	return amqp.Dial(x.Config.Server)
}

// createChannel 创建新通道并声明交换机
func (x *ClientNode) createChannel() (*amqp.Channel, error) {
	conn, err := x.SharedNode.GetSafely()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if x.Config.Exchange != "" {
		//声明交换机
		err = ch.ExchangeDeclare(
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
			ch.Close()
			ch, err = conn.Channel()
			if err != nil {
				return nil, err
			}
		}
	}

	return ch, nil
}
