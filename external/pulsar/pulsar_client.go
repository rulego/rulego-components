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

// 注册组件
func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

// ClientNodeConfiguration Pulsar客户端节点配置
type ClientNodeConfiguration struct {
	// Pulsar服务器地址
	Server string `json:"server" label:"Pulsar服务器地址" desc:"Pulsar服务器地址，格式为pulsar://host:port" required:"true"`
	// 发布主题，支持${}变量
	Topic string `json:"topic" label:"发布主题" desc:"消息发布的主题名称" required:"true"`
	// 消息键模板，支持${}变量
	Key string `json:"key" label:"消息键" desc:"消息键模板，可使用变量替换"`
	// Headers 请求头,可以使用 ${metadata.key} 读取元数据中的变量或者使用 ${msg.key} 读取消息负荷中的变量进行替换
	Headers map[string]string `json:"headers" label:"请求头"`
	// 鉴权令牌
	AuthToken string `json:"authToken" label:"鉴权令牌" desc:"Pulsar JWT鉴权令牌"`
	// TLS证书文件
	CertFile string `json:"certFile" label:"TLS证书文件" desc:"TLS证书文件路径"`
	// TLS私钥文件
	CertKeyFile string `json:"certKeyFile" label:"TLS私钥文件" desc:"TLS私钥文件路径"`
}

// ClientNode Pulsar客户端节点
type ClientNode struct {
	base.SharedNode[pulsar.Client]
	// 节点配置
	Config ClientNodeConfiguration
	// 生产者映射，key为topic，value为对应的生产者
	producers sync.Map
	//topic 模板
	topicTemplate el.Template
	//messageKey 模板
	messageKeyTemplate el.Template
	//headers 模板，支持key和value都使用变量替换
	headersTemplate map[*el.MixedTemplate]*el.MixedTemplate
	// 是否包含变量
	hasVar bool
}

// Type 组件类型
func (x *ClientNode) Type() string {
	return "x/pulsarClient"
}

// New 创建新实例
func (x *ClientNode) New() types.Node {
	return &ClientNode{Config: ClientNodeConfiguration{
		Topic:  "/device/msg",
		Server: "pulsar://localhost:6650",
	}}
}

// Init 初始化
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	// 去除配置中所有字符串值的前后空格
	base.NodeUtils.TrimStrings(configuration)

	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (pulsar.Client, error) {
			return x.initClient()
		}, func(client pulsar.Client) error {
			// 清理回调函数 - 关闭所有生产者
			x.producers.Range(func(key, value interface{}) bool {
				if producer, ok := value.(pulsar.Producer); ok && producer != nil {
					producer.Close()
				}
				return true
			})
			// 清空sync.Map
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
			// 为每个header的key和value创建模板，支持变量替换
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

// OnMsg 处理消息
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	// 获取模板变量
	var evn map[string]interface{}
	if x.hasVar {
		evn = base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	}
	// 解析主题
	topic, err := x.topicTemplate.Execute(evn)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	topicStr := str.ToString(topic)

	// 获取客户端
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// 获取或创建对应topic的生产者（使用sync.Map的无锁操作）
	var producer pulsar.Producer
	if value, exists := x.producers.Load(topicStr); exists {
		producer = value.(pulsar.Producer)
	} else {
		// 创建新的生产者
		producerOptions := pulsar.ProducerOptions{
			Topic: topicStr,
		}

		newProducer, err := client.CreateProducer(producerOptions)
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}

		// 使用LoadOrStore确保只有一个生产者被创建和存储
		if actual, loaded := x.producers.LoadOrStore(topicStr, newProducer); loaded {
			// 如果已经存在，关闭新创建的生产者，使用已存在的
			newProducer.Close()
			producer = actual.(pulsar.Producer)
		} else {
			// 使用新创建的生产者
			producer = newProducer
		}
	}

	// 构建消息
	producerMessage := &pulsar.ProducerMessage{
		Payload: []byte(msg.GetData()),
	}

	// 设置消息键
	if x.messageKeyTemplate != nil {
		messageKey, _ := x.messageKeyTemplate.Execute(evn)
		if messageKeyStr := str.ToString(messageKey); messageKeyStr != "" {
			producerMessage.Key = messageKeyStr
		}
	}

	// 设置自定义属性，支持key和value都使用变量替换
	if len(x.headersTemplate) > 0 {
		headers := make(map[string]string)
		for keyTmpl, valueTmpl := range x.headersTemplate {
			key := keyTmpl.ExecuteAsString(evn)
			value := valueTmpl.ExecuteAsString(evn)
			headers[key] = value
		}
		producerMessage.Properties = headers
	}

	// 发送消息
	_, err = producer.Send(context.Background(), producerMessage)
	if err != nil {
		ctx.TellFailure(msg, err)
	} else {
		ctx.TellSuccess(msg)
	}
}

// Destroy 销毁
func (x *ClientNode) Destroy() {
	// SharedNode.Close()会自动调用Init中注册的清理回调函数
	_ = x.SharedNode.Close()
}

// initClient 初始化Pulsar客户端
func (x *ClientNode) initClient() (pulsar.Client, error) {
	clientOptions := pulsar.ClientOptions{
		URL: x.Config.Server,
	}

	// 设置JWT Token鉴权
	if x.Config.AuthToken != "" {
		clientOptions.Authentication = pulsar.NewAuthenticationToken(x.Config.AuthToken)
	}

	// 设置TLS配置
	if x.Config.CertFile != "" {
		clientOptions.TLSCertificateFile = x.Config.CertFile
	}
	if x.Config.CertKeyFile != "" {
		clientOptions.TLSKeyFilePath = x.Config.CertKeyFile
	}

	client, err := pulsar.NewClient(clientOptions)
	return client, err
}
