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

	// Channel pool configuration
	DefaultChannelPoolSize = 10
	MaxChannelPoolSize     = 100
)

// channelPool channel pool structure
type channelPool struct {
	mu       sync.RWMutex
	channels chan *amqp.Channel
	factory  func() (*amqp.Channel, error)
	close    func(*amqp.Channel)
	maxSize  int
}

// newChannelPool creates a channel pool
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

// Get channels from the pool
func (p *channelPool) Get() (*amqp.Channel, error) {
	select {
	case ch := <-p.channels:
		if ch != nil && !ch.IsClosed() {
			return ch, nil
		}
		// The channel is closed, and new ones are being created
		return p.factory()
	default:
		// The pool is empty, creating new channels
		return p.factory()
	}
}

// Put puts the channel back into the pool
func (p *channelPool) Put(ch *amqp.Channel) {
	if ch == nil || ch.IsClosed() {
		return
	}

	select {
	case p.channels <- ch:
		// Successfully returned to the pool
	default:
		// The pool is full, so the passage is closed
		p.close(ch)
	}
}

// Close: Close the channel pool
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
	// RabbitMQ server address, format: "amqp:// Username:Password@ServerAddress:PortNumber"
	Server string `json:"server" label:"Server" desc:"RabbitMQ server address, e.g. amqp://user:pass@host:5672" required:"true" ref:"primary"`
	// Router key
	Key string `json:"key" label:"Routing Key" desc:"Routing key. Supports ${metadata.key} and ${msg.key} substitution" required:"true"`
	// Switch name
	Exchange string `json:"exchange" label:"Exchange" desc:"Exchange name" required:"true"`
	// Switch type: direct, fanout, topic
	ExchangeType string `json:"exchangeType" label:"Exchange Type" desc:"Exchange type: direct, fanout, topic"`
	//Indicates whether the switch is persistent
	Durable bool `json:"durable" label:"Durable" desc:"true=persistent exchange survives server restart"`
	//Indicates whether the switch is automatically deleted
	AutoDelete bool `json:"autoDelete" label:"Auto Delete" desc:"true=auto-delete when no queues bound"`
}

type ClientNode struct {
	base.SharedNode[*amqp.Connection]
	// Node configuration
	Config ClientNodeConfiguration
	// Passage pool
	channelPool *channelPool
	// Routing key template
	keyTemplate el.Template
	// Whether the identification template contains variables for performance optimization
	hasVar bool
}

// Type returns the component type
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

// Init initializes the component
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}

	// Initialize SharedNode
	_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*amqp.Connection, error) {
		return x.initClient()
	}, func(conn *amqp.Connection) error {
		// Cleanup callback function
		if x.channelPool != nil {
			x.channelPool.Close()
			x.channelPool = nil
		}
		if conn != nil && !conn.IsClosed() {
			return conn.Close()
		}
		return nil
	})

	// Initialize the channel pool (using default size)
	x.channelPool = newChannelPool(DefaultChannelPoolSize, func() (*amqp.Channel, error) {
		return x.createChannel()
	})

	// Initialize the routing key template
	template, err := el.NewTemplate(x.Config.Key)
	if err != nil {
		return err
	}
	x.keyTemplate = template
	x.hasVar = template.HasVar()
	return nil
}

// OnMsg processes a message
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	var evn map[string]interface{}
	if x.hasVar {
		evn = base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	}
	key := x.keyTemplate.ExecuteAsString(evn)

	// Use channel pools to obtain channels
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

// Destroy releases resources
func (x *ClientNode) Destroy() {
	_ = x.SharedNode.Close()
}

// Desc returns the component description
func (x *ClientNode) Desc() string {
	return "RabbitMQ client for publishing messages. Key supports ${metadata.key} and ${msg.key} substitution. Routes to Success/Failure"
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

// createChannel: Create a new channel and declare a switch
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
		//Declaration switch
		err = ch.ExchangeDeclare(
			x.Config.Exchange,     // Switch name
			x.Config.ExchangeType, // Switch type
			x.Config.Durable,      //Is it persistent?
			x.Config.AutoDelete,   //Whether it is automatically deleted
			false,
			false,
			nil,
		)
		if err != nil {
			//If the switch already exists, it is no longer declared and the channel is recreated
			ch.Close()
			ch, err = conn.Channel()
			if err != nil {
				return nil, err
			}
		}
	}

	return ch, nil
}
