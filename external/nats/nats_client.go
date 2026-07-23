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
	// NATS server address
	Server string `json:"server" label:"Server" desc:"NATS server address, e.g. nats://127.0.0.1:4222" required:"true" ref:"primary"`
	// NATS username
	Username string `json:"username" label:"Username" desc:"NATS username" ref:"shared"`
	// NATS password
	Password string `json:"password" label:"Password" desc:"NATS password" ref:"shared"`
	// Release the theme
	Topic string `json:"topic" label:"Topic" desc:"Publish topic. Supports ${metadata.key} and ${msg.key} substitution" required:"true"`
}

type ClientNode struct {
	base.SharedNode[*nats.Conn]
	// Node configuration
	Config ClientNodeConfiguration
	// Whether the NATS server is being connected
	connecting int32
	// topicTemplate, used to parse dynamic themes
	// topicTemplate template for resolving dynamic topic
	topicTemplate el.Template
	// hasVar identifies whether the template contains variables
	hasVar bool
}

// Type returns the component type
func (x *ClientNode) Type() string {
	return "x/natsClient"
}

func (x *ClientNode) New() types.Node {
	return &ClientNode{Config: ClientNodeConfiguration{
		Topic:  "/device/msg",
		Server: "nats://127.0.0.1:4222",
	}}
}

// Init initializes the component
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*nats.Conn, error) {
			return x.initClient()
		}, func(client *nats.Conn) error {
			// Cleanup callback function
			client.Close()
			return nil
		})
		x.topicTemplate, err = el.NewTemplate(x.Config.Topic)
		if err != nil {
			return err
		}
		// Check if the template contains variables
		x.hasVar = x.topicTemplate.HasVar()
	}
	return err
}

// OnMsg processes a message
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

// Desc returns the component description
func (x *ClientNode) Desc() string {
	return "NATS client for publishing messages. Topic supports ${metadata.key} and ${msg.key} substitution. Routes to Success/Failure"
}

func (x *ClientNode) initClient() (*nats.Conn, error) {
	client, err := nats.Connect(x.Config.Server, nats.UserInfo(x.Config.Username, x.Config.Password))
	return client, err
}
