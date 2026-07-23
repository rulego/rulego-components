/*
 * Copyright 2023 The RuleGo Authors.
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

package kafka

import (
	"crypto/tls"
	"errors"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
)

const (
	KeyPartition = "partition"
	KeOffset     = "offset"
)

// Register the node
func init() {
	_ = rulego.Registry.Register(&ProducerNode{})
}

// NodeConfiguration: node configuration
type NodeConfiguration struct {
	Server    string     `json:"server" label:"Server" desc:"Kafka server address, multiple addresses separated by commas" required:"true" ref:"primary"`
	Topic     string     `json:"topic" label:"Topic" desc:"Publish topic, supports ${metadata.key} and ${msg.key} replacement" required:"true"`
	Key       string     `json:"key" label:"Key" desc:"Message partition key, supports ${metadata.key} and ${msg.key} replacement"`
	Partition int32      `json:"partition" label:"Partition" desc:"Partition number, -1 for auto selection"`
	SASL      SASLConfig `json:"sasl" label:"SASL Auth" desc:"SASL authentication configuration"`
	TLS       TLSConfig  `json:"tls" label:"TLS" desc:"TLS encryption configuration"`
}

// SASLConfig SASL Certification Configuration
type SASLConfig struct {
	Enable    bool   `json:"enable" label:"Enable" desc:"Enable SASL authentication"`
	Mechanism string `json:"mechanism" label:"Mechanism" desc:"SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512"`
	Username  string `json:"username" label:"Username" desc:"SASL authentication username" ref:"shared"`
	Password  string `json:"password" label:"Password" desc:"SASL authentication password" ref:"shared"`
}

// TLSConfig TLS configuration
type TLSConfig struct {
	Enable             bool `json:"enable" label:"Enable" desc:"Enable TLS encryption"`
	InsecureSkipVerify bool `json:"insecureSkipVerify" label:"Skip Verify" desc:"Skip server certificate verification"`
}

type ProducerNode struct {
	base.SharedNode[sarama.SyncProducer]
	Config NodeConfiguration
	// Brokers Kafka server address list
	brokers []string
	// topicTemplate, used to parse dynamic themes
	// topicTemplate template for resolving dynamic topic
	topicTemplate el.Template
	// keyTemplate partition key template, used to parse dynamic partition keys
	// keyTemplate template for resolving dynamic partition key
	keyTemplate el.Template
	// hasVar identifies whether the template contains variables
	// hasVar indicates whether the template contains variables
	hasVar bool
}

// Type returns the component type
func (x *ProducerNode) Type() string {
	return "x/kafkaProducer"
}

func (x *ProducerNode) New() types.Node {
	return &ProducerNode{
		Config: NodeConfiguration{
			Server:    "127.0.0.1:9092",
			Partition: 0,
			SASL: SASLConfig{
				Mechanism: "PLAIN",
			},
		},
	}
}

// Init initializes the component
func (x *ProducerNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		x.brokers = x.getBrokerFromOldVersion(configuration)
		if len(x.brokers) == 0 && x.Config.Server != "" {
			x.brokers = strings.Split(x.Config.Server, ",")
		}
		if len(x.brokers) == 0 {
			return errors.New("brokers is empty")
		}
		_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.brokers[0], ruleConfig.NodeClientInitNow, func() (sarama.SyncProducer, error) {
			return x.initClient()
		}, func(client sarama.SyncProducer) error {
			return client.Close()
		})

		x.topicTemplate, err = el.NewTemplate(x.Config.Topic)
		if err != nil {
			return err
		}
		x.keyTemplate, err = el.NewTemplate(x.Config.Key)
		if err != nil {
			return err
		}
		// Check if any templates contain variables
		x.hasVar = x.topicTemplate.HasVar() || x.keyTemplate.HasVar()
	}
	return err
}

// OnMsg processes a message
func (x *ProducerNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	var evn map[string]interface{}
	if x.hasVar {
		evn = base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	}
	topic := x.topicTemplate.ExecuteAsString(evn)
	key := x.keyTemplate.ExecuteAsString(evn)

	client, err := x.SharedNode.GetSafely()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	message := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: x.Config.Partition,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.StringEncoder(msg.GetData()),
	}
	partition, offset, err := client.SendMessage(message)
	if err != nil {
		// Check if there is a network connection error; if so, reset the client connection
		if x.isNetworkError(err) {
			x.resetClient()
			// Try again
			client, retryErr := x.SharedNode.GetSafely()
			if retryErr == nil {
				partition, offset, err = client.SendMessage(message)
				if err == nil {
					msg.Metadata.PutValue(KeyPartition, strconv.Itoa(int(partition)))
					msg.Metadata.PutValue(KeOffset, strconv.Itoa(int(offset)))
					ctx.TellSuccess(msg)
					return
				}
			}
		}
		ctx.TellFailure(msg, err)
	} else {
		msg.Metadata.PutValue(KeyPartition, strconv.Itoa(int(partition)))
		msg.Metadata.PutValue(KeOffset, strconv.Itoa(int(offset)))
		ctx.TellSuccess(msg)
	}
}

func (x *ProducerNode) Destroy() {
	_ = x.SharedNode.Close()
}

// Desc returns the component description
func (x *ProducerNode) Desc() string {
	return "Kafka producer for publishing messages. Topic and key support ${metadata.key} and ${msg.key} substitution. Routes to Success/Failure"
}

func (x *ProducerNode) getBrokerFromOldVersion(configuration types.Configuration) []string {
	if v, ok := configuration["brokers"]; ok {
		return v.([]string)
	} else {
		return nil
	}
}

func (x *ProducerNode) initClient() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true // Sync mode needs to be set to true
	// Set the reconnection configuration settings
	config.Metadata.Retry.Max = 3
	config.Metadata.Retry.Backoff = 250 * 1000000 // 250ms
	config.Producer.Retry.Max = 3
	config.Producer.Retry.Backoff = 100 * 1000000 // 100ms

	// Configure SASL certification
	if x.Config.SASL.Enable {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = x.Config.SASL.Username
		config.Net.SASL.Password = x.Config.SASL.Password

		switch strings.ToUpper(x.Config.SASL.Mechanism) {
		case "PLAIN":
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "SCRAM-SHA-256":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "SCRAM-SHA-512":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
	}

	// Configure TLS
	if x.Config.TLS.Enable {
		config.Net.TLS.Enable = true
		if x.Config.TLS.InsecureSkipVerify {
			config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}
		}
	}

	return sarama.NewSyncProducer(x.brokers, config)
}

// isNetworkError checks whether it is a network connection error
func (x *ProducerNode) isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if err == sarama.ErrOutOfBrokers {
		return true
	}
	errorStr := err.Error()
	// Check for common network errors
	return strings.Contains(errorStr, sarama.ErrOutOfBrokers.Error()) ||
		strings.Contains(errorStr, sarama.ErrClosedClient.Error()) ||
		strings.Contains(errorStr, sarama.ErrNotConnected.Error()) ||
		strings.Contains(errorStr, "connection refused") ||
		strings.Contains(errorStr, "no route to host") ||
		strings.Contains(errorStr, "network is unreachable") ||
		strings.Contains(errorStr, "connection reset") ||
		strings.Contains(errorStr, "broken pipe") ||
		strings.Contains(errorStr, "EOF") ||
		err == sarama.ErrOutOfBrokers
}

// resetClient resets the client connection
func (x *ProducerNode) resetClient() {
	_ = x.SharedNode.Close()
}
