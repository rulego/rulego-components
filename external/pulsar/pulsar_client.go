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

package pulsar

import (
	"context"
	"errors"
	"github.com/rulego/rulego/utils/str"
	"sync"

	"github.com/rulego/rulego/utils/el"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/maps"
)

// Register the component
func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

// ClientNodeConfiguration Pulsar client node configuration
type ClientNodeConfiguration struct {
	// Pulsar server address
	Server string `json:"server" label:"Server" desc:"Pulsar server address, e.g. pulsar://host:port" required:"true" ref:"primary"`
	// Publish topics, support ${} variables
	Topic string `json:"topic" label:"Topic" desc:"Publish topic. Supports ${metadata.key} and ${msg.key} substitution" required:"true"`
	// Message key template, supports ${} variables
	Key string `json:"key" label:"Key" desc:"Message key. Supports ${metadata.key} and ${msg.key} substitution"`
	// Headers request heads
	Headers map[string]string `json:"headers" label:"Headers" desc:"Message headers. Supports ${metadata.key} and ${msg.key} substitution"`
	// Authority and token of authority
	AuthToken string `json:"authToken" label:"Auth Token" desc:"Pulsar JWT authentication token" ref:"shared"`
	// TLS certificate file
	CertFile string `json:"certFile" label:"Cert File" desc:"TLS certificate file path" ref:"shared"`
	// TLS private key file
	CertKeyFile string `json:"certKeyFile" label:"Cert Key File" desc:"TLS private key file path" ref:"shared"`
}

// ClientNode Pulsar client node
type ClientNode struct {
	base.SharedNode[pulsar.Client]
	// Node configuration
	Config ClientNodeConfiguration
	// Producer mapping: key is topic, value is the corresponding producer
	producers sync.Map
	//topic template
	topicTemplate el.Template
	//messageKey template
	messageKeyTemplate el.Template
	//Headers template, supports replacing both key and value with variables
	headersTemplate map[*el.MixedTemplate]*el.MixedTemplate
	// Whether variables are included
	hasVar bool
}

// Type returns the component type
func (x *ClientNode) Type() string {
	return "x/pulsarClient"
}

// New creates an instance
func (x *ClientNode) New() types.Node {
	return &ClientNode{Config: ClientNodeConfiguration{
		Topic:  "/device/msg",
		Server: "pulsar://localhost:6650",
	}}
}

// Init initializes the component
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	// Remove all preceding and following spaces for all string values in the configuration
	base.NodeUtils.TrimStrings(configuration)

	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (pulsar.Client, error) {
			return x.initClient()
		}, func(client pulsar.Client) error {
			// Cleanup callback function - shut down all producers
			x.producers.Range(func(key, value interface{}) bool {
				if producer, ok := value.(pulsar.Producer); ok && producer != nil {
					producer.Close()
				}
				return true
			})
			// Clear sync.Map
			x.producers = sync.Map{}
			if client != nil {
				client.Close()
			}
			return nil
		})
		if x.Config.Topic == "" {
			return errors.New("topic cannot be empty")
		}
		x.topicTemplate, err = el.NewTemplate(x.Config.Topic)
		if err != nil {
			return err
		}
		if x.topicTemplate.HasVar() {
			x.hasVar = true
		}
		if x.Config.Key != "" {
			x.messageKeyTemplate, err = el.NewTemplate(x.Config.Key)
			if err != nil {
				return err
			}
			if x.messageKeyTemplate.HasVar() {
				x.hasVar = true
			}
		}
		if len(x.Config.Headers) > 0 {
			// Create templates for each header's key and value, supporting variable substitution
			var headerTemplates = make(map[*el.MixedTemplate]*el.MixedTemplate)
			for key, value := range x.Config.Headers {
				keyTmpl, err := el.NewMixedTemplate(key)
				if err != nil {
					return err
				}
				valueTmpl, err := el.NewMixedTemplate(value)
				if err != nil {
					return err
				}
				headerTemplates[keyTmpl] = valueTmpl
				if keyTmpl.HasVar() || valueTmpl.HasVar() {
					x.hasVar = true
				}
			}
			x.headersTemplate = headerTemplates
		}
	}
	return err
}

// OnMsg processes a message
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	// Retrieve template variables
	var evn map[string]interface{}
	if x.hasVar {
		evn = base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	}
	// Analyze the topic
	topic, err := x.topicTemplate.Execute(evn)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	topicStr := str.ToString(topic)

	// Get the client
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// Obtain or create the producer for the corresponding topic (using sync.Map's lock-free operation)
	var producer pulsar.Producer
	if value, exists := x.producers.Load(topicStr); exists {
		producer = value.(pulsar.Producer)
	} else {
		// Create new producers
		producerOptions := pulsar.ProducerOptions{
			Topic: topicStr,
		}

		newProducer, err := client.CreateProducer(producerOptions)
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}

		// Using LoadOrStore ensures that only one producer is created and stored
		if actual, loaded := x.producers.LoadOrStore(topicStr, newProducer); loaded {
			// If it already exists, close the newly created producer and use the existing one
			newProducer.Close()
			producer = actual.(pulsar.Producer)
		} else {
			// Use newly created producers
			producer = newProducer
		}
	}

	// Build messages
	producerMessage := &pulsar.ProducerMessage{
		Payload: []byte(msg.GetData()),
	}

	// Set the message key
	if x.messageKeyTemplate != nil {
		messageKey, _ := x.messageKeyTemplate.Execute(evn)
		if messageKeyStr := str.ToString(messageKey); messageKeyStr != "" {
			producerMessage.Key = messageKeyStr
		}
	}

	// Set custom properties, supporting variable replacement for both key and value
	if len(x.headersTemplate) > 0 {
		headers := make(map[string]string)
		for keyTmpl, valueTmpl := range x.headersTemplate {
			key := keyTmpl.ExecuteAsString(evn)
			value := valueTmpl.ExecuteAsString(evn)
			headers[key] = value
		}
		producerMessage.Properties = headers
	}

	// Send the message
	_, err = producer.Send(context.Background(), producerMessage)
	if err != nil {
		ctx.TellFailure(msg, err)
	} else {
		ctx.TellSuccess(msg)
	}
}

// Destroy releases resources
func (x *ClientNode) Destroy() {
	// SharedNode.Close() automatically calls the cleanup callback function registered in the Init
	_ = x.SharedNode.Close()
}

// Desc returns the component description
func (x *ClientNode) Desc() string {
	return "Pulsar client for publishing messages. Topic and key support ${metadata.key} and ${msg.key} substitution. Routes to Success/Failure"
}

// initClient Initializes the Pulsar client
func (x *ClientNode) initClient() (pulsar.Client, error) {
	clientOptions := pulsar.ClientOptions{
		URL: x.Config.Server,
	}

	// Set JWT Token authentication
	if x.Config.AuthToken != "" {
		clientOptions.Authentication = pulsar.NewAuthenticationToken(x.Config.AuthToken)
	}

	// Set up TLS configuration
	if x.Config.CertFile != "" {
		clientOptions.TLSCertificateFile = x.Config.CertFile
	}
	if x.Config.CertKeyFile != "" {
		clientOptions.TLSKeyFilePath = x.Config.CertKeyFile
	}

	client, err := pulsar.NewClient(clientOptions)
	return client, err
}
