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

package nsq

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/str"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/maps"
)

// 注册组件
func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

// ClientNodeConfiguration NSQ客户端节点配置
type ClientNodeConfiguration struct {
	// NSQ服务器地址，支持多种格式：
	// 1. 单个nsqd: "127.0.0.1:4150"
	// 2. 多个nsqd: "127.0.0.1:4150,127.0.0.1:4151"
	// 3. lookupd地址: "http://127.0.0.1:4161,http://127.0.0.1:4162"
	Server string `json:"server" label:"NSQ服务器地址" desc:"NSQ服务器地址，多个地址用逗号分隔" required:"true"`
	// 发布主题，支持${}变量
	Topic string `json:"topic" label:"发布主题" desc:"消息发布的主题名称" required:"true"`
	// 鉴权令牌
	AuthToken string `json:"authToken" label:"鉴权令牌" desc:"NSQ鉴权令牌"`
	// TLS证书文件
	CertFile string `json:"certFile" label:"TLS证书文件" desc:"TLS证书文件路径"`
	// TLS私钥文件
	CertKeyFile string `json:"certKeyFile" label:"TLS私钥文件" desc:"TLS私钥文件路径"`
}

// ClientNode NSQ客户端节点
type ClientNode struct {
	base.SharedNode[*nsq.Producer]
	// 节点配置
	Config ClientNodeConfiguration
	//topic 模板
	topicTemplate el.Template
}

// Type 组件类型
func (x *ClientNode) Type() string {
	return "x/nsqClient"
}

// New 创建新实例
func (x *ClientNode) New() types.Node {
	return &ClientNode{Config: ClientNodeConfiguration{
		Server: "127.0.0.1:4150",
		Topic:  "devices_msg",
	}}
}

// Init 初始化
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	// 去除配置中所有字符串值的前后空格
	base.NodeUtils.TrimStrings(configuration)
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*nsq.Producer, error) {
			return x.initClient()
		}, func(client *nsq.Producer) error {
			// 清理回调函数
			client.Stop()
			return nil
		})
		if x.Config.Topic == "" {
			return errors.New("topic cannot be empty")
		}
		x.topicTemplate, err = el.NewTemplate(x.Config.Topic)
	}
	return err
}

// OnMsg 处理消息
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {

	var evn map[string]interface{}
	if x.topicTemplate.HasVar() {
		evn = base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	}
	topic, err := x.topicTemplate.Execute(evn)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	if err := client.Publish(str.ToString(topic), []byte(msg.GetData())); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		ctx.TellSuccess(msg)
	}
}

// Destroy 销毁
func (x *ClientNode) Destroy() {
	_ = x.SharedNode.Close()
}

// parseAddresses 解析Server字段中的地址
// 支持格式：
// 1. 单个nsqd: "127.0.0.1:4150"
// 2. 多个nsqd: "127.0.0.1:4150,127.0.0.1:4151"
// 3. lookupd地址: "http://127.0.0.1:4161,http://127.0.0.1:4162"
func (x *ClientNode) parseAddresses() (nsqdAddrs []string, lookupdAddrs []string) {
	if x.Config.Server == "" {
		return
	}

	addresses := strings.Split(x.Config.Server, ",")
	for _, addr := range addresses {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}

		// 判断是否为lookupd地址（包含http://或https://）
		if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
			lookupdAddrs = append(lookupdAddrs, addr)
		} else {
			// 普通的nsqd地址
			nsqdAddrs = append(nsqdAddrs, addr)
		}
	}
	return
}

// initClient 初始化NSQ生产者客户端
func (x *ClientNode) initClient() (*nsq.Producer, error) {
	config := nsq.NewConfig()

	// 设置鉴权令牌
	if x.Config.AuthToken != "" {
		config.AuthSecret = x.Config.AuthToken
	}

	// 设置TLS配置
	if x.Config.CertFile != "" && x.Config.CertKeyFile != "" {
		config.TlsV1 = true
		config.TlsConfig = &tls.Config{
			InsecureSkipVerify: false,
		}
		// 加载证书
		cert, err := tls.LoadX509KeyPair(x.Config.CertFile, x.Config.CertKeyFile)
		if err != nil {
			return nil, err
		}
		config.TlsConfig.Certificates = []tls.Certificate{cert}
	}

	// 解析地址配置
	nsqdAddrs, lookupdAddrs := x.parseAddresses()

	// NSQ生产者只能连接到单个nsqd，不支持lookupd
	// 如果配置了lookupd地址，需要先通过lookupd发现nsqd地址
	var targetAddr string
	if len(nsqdAddrs) > 0 {
		// 使用第一个nsqd地址
		targetAddr = nsqdAddrs[0]
	} else if len(lookupdAddrs) > 0 {
		// 通过lookupd API发现nsqd地址
		nsqdAddr, err := x.discoverNsqdFromLookupd(lookupdAddrs[0])
		if err != nil {
			return nil, fmt.Errorf("failed to discover nsqd from lookupd %s: %w", lookupdAddrs[0], err)
		}
		targetAddr = nsqdAddr
	} else {
		// 使用原始Server配置
		targetAddr = x.Config.Server
	}

	client, err := nsq.NewProducer(targetAddr, config)
	return client, err
}

// discoverNsqdFromLookupd 通过lookupd API发现可用的nsqd地址
func (x *ClientNode) discoverNsqdFromLookupd(lookupdAddr string) (string, error) {
	// 构建lookupd API URL
	apiURL := fmt.Sprintf("%s/nodes", strings.TrimSuffix(lookupdAddr, "/"))
	
	// 创建HTTP客户端，设置超时
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	
	// 发送GET请求到lookupd
	resp, err := client.Get(apiURL)
	if err != nil {
		return "", fmt.Errorf("failed to query lookupd API: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("lookupd API returned status %d", resp.StatusCode)
	}
	
	// 读取响应体
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read lookupd response: %w", err)
	}
	
	// 解析JSON响应
	var response struct {
		Producers []struct {
			RemoteAddress    string `json:"remote_address"`
			Hostname         string `json:"hostname"`
			BroadcastAddress string `json:"broadcast_address"`
			TCPPort          int    `json:"tcp_port"`
			HTTPPort         int    `json:"http_port"`
			Version          string `json:"version"`
		} `json:"producers"`
	}
	
	err = json.Unmarshal(body, &response)
	if err != nil {
		return "", fmt.Errorf("failed to parse lookupd response: %w", err)
	}
	
	// 检查是否有可用的nsqd节点
	if len(response.Producers) == 0 {
		return "", errors.New("no nsqd nodes found from lookupd")
	}
	
	// 返回第一个可用的nsqd地址
	producer := response.Producers[0]
	var nsqdAddr string
	if producer.BroadcastAddress != "" {
		nsqdAddr = fmt.Sprintf("%s:%d", producer.BroadcastAddress, producer.TCPPort)
	} else {
		nsqdAddr = fmt.Sprintf("%s:%d", producer.RemoteAddress, producer.TCPPort)
	}
	
	return nsqdAddr, nil
}
