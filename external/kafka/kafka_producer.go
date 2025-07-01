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
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

const (
	KeyPartition = "partition"
	KeOffset     = "offset"
)

// 注册节点
func init() {
	_ = rulego.Registry.Register(&ProducerNode{})
}

// NodeConfiguration 节点配置
type NodeConfiguration struct {
	// kafka服务器地址列表，多个与逗号隔开
	Server string `json:"server"`
	// Topic 发布主题，可以使用 ${metadata.key} 读取元数据中的变量或者使用 ${msg.key} 读取消息负荷中的变量进行替换
	Topic string `json:"topic"`
	// Key 分区键，可以使用 ${metadata.key} 读取元数据中的变量或者使用 ${msg.key} 读取消息负荷中的变量进行替换
	Key string `json:"key"`
	//Partition 分区编号
	Partition int32 `json:"partition"`
	// SASL认证配置
	SASL SASLConfig `json:"sasl"`
	// TLS配置
	TLS TLSConfig `json:"tls"`
}

// SASLConfig SASL认证配置
type SASLConfig struct {
	// Enable 是否启用SASL认证
	Enable bool `json:"enable"`
	// Mechanism 认证机制，支持 PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	Mechanism string `json:"mechanism"`
	// Username 用户名
	Username string `json:"username"`
	// Password 密码
	Password string `json:"password"`
}

// TLSConfig TLS配置
type TLSConfig struct {
	// Enable 是否启用TLS
	Enable bool `json:"enable"`
	// InsecureSkipVerify 是否跳过证书验证
	InsecureSkipVerify bool `json:"insecureSkipVerify"`
}

type ProducerNode struct {
	base.SharedNode[sarama.SyncProducer]
	Config NodeConfiguration
	client sarama.SyncProducer
	// brokers kafka服务器地址列表
	brokers []string
	//topic 模板
	topicTemplate str.Template
	//key 模板
	keyTemplate str.Template
}

// Type 返回组件类型
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

// Init 初始化组件
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
		_ = x.SharedNode.Init(ruleConfig, x.Type(), x.brokers[0], ruleConfig.NodeClientInitNow, func() (sarama.SyncProducer, error) {
			return x.initClient()
		})

		x.topicTemplate = str.NewTemplate(x.Config.Topic)
		x.keyTemplate = str.NewTemplate(x.Config.Key)
	}
	return err
}

// OnMsg 处理消息
func (x *ProducerNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	defer x.SharedNode.EndOp()

	// 开始操作计数
	x.SharedNode.BeginOp()

	// 检查是否正在关闭
	if x.SharedNode.IsShuttingDown() {
		ctx.TellFailure(msg, errors.New("kafka producer is shutting down"))
		return
	}

	topic := x.Config.Topic
	key := x.Config.Key
	if !x.topicTemplate.IsNotVar() || !x.keyTemplate.IsNotVar() {
		evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
		topic = str.ExecuteTemplate(topic, evn)
		key = str.ExecuteTemplate(key, evn)
	}

	client, err := x.SharedNode.Get()
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
		// 检查是否是网络连接错误，如果是则重置客户端连接
		if x.isNetworkError(err) {
			x.resetClient()
			// 重试一次
			client, retryErr := x.SharedNode.Get()
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

// Destroy 销毁组件
func (x *ProducerNode) Destroy() {
	// 使用SharedNode的优雅关闭机制
	// 如果是共享资源池的客户端，会自动跳过关闭
	// 如果是本地客户端，会等待活跃操作完成后执行关闭函数
	x.SharedNode.GracefulShutdown(0, func() {
		// 只有非资源池模式下才关闭本地客户端
		if x.client != nil {
			_ = x.client.Close()
			x.client = nil
		}
	})
}

func (x *ProducerNode) getBrokerFromOldVersion(configuration types.Configuration) []string {
	if v, ok := configuration["brokers"]; ok {
		return v.([]string)
	} else {
		return nil
	}
}

func (x *ProducerNode) initClient() (sarama.SyncProducer, error) {
	if x.client != nil {
		return x.client, nil
	} else {
		x.Locker.Lock()
		defer x.Locker.Unlock()
		if x.client != nil {
			return x.client, nil
		}
		var err error
		config := sarama.NewConfig()
		config.Producer.Return.Successes = true // 同步模式需要设置这个参数为true
		// 设置重连相关配置
		config.Metadata.Retry.Max = 3
		config.Metadata.Retry.Backoff = 250 * 1000000 // 250ms
		config.Producer.Retry.Max = 3
		config.Producer.Retry.Backoff = 100 * 1000000 // 100ms

		// 配置SASL认证
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

		// 配置TLS
		if x.Config.TLS.Enable {
			config.Net.TLS.Enable = true
			if x.Config.TLS.InsecureSkipVerify {
				config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}
			}
		}

		x.client, err = sarama.NewSyncProducer(x.brokers, config)
		return x.client, err
	}
}

// isNetworkError 判断是否是网络连接错误
func (x *ProducerNode) isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if err == sarama.ErrOutOfBrokers {
		return true
	}
	errorStr := err.Error()
	// 检查常见的网络错误
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

// resetClient 重置客户端连接
func (x *ProducerNode) resetClient() {
	x.Locker.Lock()
	defer x.Locker.Unlock()
	if x.client != nil {
		_ = x.client.Close()
		x.client = nil
	}
}
