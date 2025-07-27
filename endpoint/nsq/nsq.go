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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"strings"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
)

// Type 组件类型
const Type = types.EndpointTypePrefix + "nsq"

// KeyResponseTopic 响应主题metadataKey
const KeyResponseTopic = "responseTopic"

// Endpoint 别名
type Endpoint = Nsq

var _ endpointApi.Endpoint = (*Endpoint)(nil)

// 注册组件
func init() {
	_ = endpoint.Registry.Register(&Endpoint{})
}

// RequestMessage 请求消息
type RequestMessage struct {
	topic   string
	message *nsq.Message
	msg     *types.RuleMsg
	err     error
}

// Body 获取消息体
func (r *RequestMessage) Body() []byte {
	return r.message.Body
}

// Headers 获取消息头
func (r *RequestMessage) Headers() textproto.MIMEHeader {
	header := make(textproto.MIMEHeader)
	header.Set("topic", r.topic)
	header.Set("attempts", fmt.Sprintf("%d", r.message.Attempts))
	header.Set("timestamp", fmt.Sprintf("%d", r.message.Timestamp))
	return header
}

// From 获取消息来源
func (r *RequestMessage) From() string {
	return string(r.topic)
}

// GetParam 获取参数
func (r *RequestMessage) GetParam(key string) string {
	return ""
}

// SetMsg 设置规则消息
func (r *RequestMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

// GetMsg 获取规则消息
func (r *RequestMessage) GetMsg() *types.RuleMsg {
	if r.msg == nil {
		// 默认指定是JSON格式，如果不是该类型，请在process函数中修改
		ruleMsg := types.NewMsg(0, r.From(), types.JSON, types.NewMetadata(), string(r.Body()))
		ruleMsg.Metadata.PutValue("messageId", string(r.message.ID[:]))
		ruleMsg.Metadata.PutValue("attempts", fmt.Sprintf("%d", r.message.Attempts))
		ruleMsg.Metadata.PutValue("timestamp", fmt.Sprintf("%d", r.message.Timestamp))
		r.msg = &ruleMsg
	}
	return r.msg
}

// SetStatusCode 设置状态码
func (r *RequestMessage) SetStatusCode(statusCode int) {
}

// SetBody 设置消息体
func (r *RequestMessage) SetBody(body []byte) {
}

// SetError 设置错误
func (r *RequestMessage) SetError(err error) {
	r.err = err
}

// GetError 获取错误
func (r *RequestMessage) GetError() error {
	return r.err
}

// ResponseMessage 响应消息
type ResponseMessage struct {
	topic    string
	message  *nsq.Message
	producer *nsq.Producer
	body     []byte
	msg      *types.RuleMsg
	headers  textproto.MIMEHeader
	err      error
}

// Body 获取响应体
func (r *ResponseMessage) Body() []byte {
	return r.body
}

// Headers 获取响应头
func (r *ResponseMessage) Headers() textproto.MIMEHeader {
	if r.headers == nil {
		r.headers = make(map[string][]string)
	}
	return r.headers
}

// From 获取消息来源
func (r *ResponseMessage) From() string {
	return r.topic
}

// GetParam 获取参数
func (r *ResponseMessage) GetParam(key string) string {
	return ""
}

// SetMsg 设置规则消息
func (r *ResponseMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

// GetMsg 获取规则消息
func (r *ResponseMessage) GetMsg() *types.RuleMsg {
	return r.msg
}

// SetStatusCode 设置状态码
func (r *ResponseMessage) SetStatusCode(statusCode int) {
}

// getMetadataValue 从msg.Metadata或者响应头获取值
func (r *ResponseMessage) getMetadataValue(metadataName, headerName string) string {
	var v string
	if r.GetMsg() != nil {
		metadata := r.GetMsg().Metadata
		v = metadata.GetValue(metadataName)
	}
	if v == "" {
		return r.Headers().Get(headerName)
	} else {
		return v
	}
}

// SetBody 设置响应体
func (r *ResponseMessage) SetBody(body []byte) {
	r.body = body
	topic := r.getMetadataValue(KeyResponseTopic, KeyResponseTopic)
	if topic != "" && r.producer != nil {
		err := r.producer.Publish(topic, r.body)
		if err != nil {
			r.SetError(err)
		}
	}
}

// SetError 设置错误
func (r *ResponseMessage) SetError(err error) {
	r.err = err
}

// GetError 获取错误
func (r *ResponseMessage) GetError() error {
	return r.err
}

// Config NSQ配置
type Config struct {
	// NSQ服务器地址，支持多种格式：
	// 1. 单个nsqd: "127.0.0.1:4150"
	// 2. 多个nsqd: "127.0.0.1:4150,127.0.0.1:4151"
	// 3. lookupd地址: "http://127.0.0.1:4161,http://127.0.0.1:4162"
	Server string `json:"server" label:"NSQ服务器地址" desc:"NSQ服务器地址，多个地址用逗号分隔" required:"true"`
	// 默认频道名称，如果AddRouter时未指定则使用此值
	Channel string `json:"channel" label:"默认频道" desc:"默认频道名称"`
	// 鉴权令牌
	AuthToken string `json:"authToken" label:"鉴权令牌" desc:"NSQ鉴权令牌"`
	// TLS证书文件
	CertFile string `json:"certFile" label:"TLS证书文件" desc:"TLS证书文件路径"`
	// TLS私钥文件
	CertKeyFile string `json:"certKeyFile" label:"TLS私钥文件" desc:"TLS私钥文件路径"`
}

// Nsq NSQ接收端端点
type Nsq struct {
	impl.BaseEndpoint
	// GracefulShutdown provides graceful shutdown capabilities
	// GracefulShutdown 提供优雅停机功能
	base.GracefulShutdown
	RuleConfig types.Config
	//Config 配置
	Config Config
	// 消费者映射关系，用于停止消费，key为routerId
	consumers map[string]*nsq.Consumer
	// 生产者
	producer *nsq.Producer
	// 互斥锁
	mu sync.RWMutex
}

// Type 组件类型
func (x *Nsq) Type() string {
	return Type
}

// parseAddresses 解析Server字段中的地址
// 支持格式：
// 1. 单个nsqd: "127.0.0.1:4150"
// 2. 多个nsqd: "127.0.0.1:4150,127.0.0.1:4151"
// 3. lookupd地址: "http://127.0.0.1:4161,http://127.0.0.1:4162"
func (x *Nsq) parseAddresses() (nsqdAddrs []string, lookupdAddrs []string) {
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

// Id 获取组件ID
func (x *Nsq) Id() string {
	return x.Config.Server
}

// New 创建新实例
func (x *Nsq) New() types.Node {
	return &Nsq{
		Config: Config{
			Server:  "127.0.0.1:4150",
			Channel: "default",
		},
	}
}

// Init 初始化
func (x *Nsq) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	x.RuleConfig = ruleConfig
	x.consumers = make(map[string]*nsq.Consumer)

	// 初始化优雅停机功能
	x.GracefulShutdown.InitGracefulShutdown(x.RuleConfig.Logger, 0)

	// 初始化生产者
	if x.Config.Server != "" {
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
			nsqdAddr, discoverErr := x.discoverNsqdFromLookupd(lookupdAddrs[0])
			if discoverErr != nil {
				return fmt.Errorf("failed to discover nsqd from lookupd %s: %w", lookupdAddrs[0], discoverErr)
			}
			targetAddr = nsqdAddr
		} else {
			// 使用原始Server配置
			targetAddr = x.Config.Server
		}
		
		if targetAddr != "" {
			producerConfig := nsq.NewConfig()
			// 设置鉴权配置
			if x.Config.AuthToken != "" {
				producerConfig.AuthSecret = x.Config.AuthToken
			}
			// 使用目标地址创建producer
			x.producer, err = nsq.NewProducer(targetAddr, producerConfig)
			if err != nil {
				return err
			}

			// 禁用NSQ内部日志输出
			x.producer.SetLoggerLevel(nsq.LogLevelError)
		}
	}

	return err
}

// Destroy 销毁
func (x *Nsq) Destroy() {
	x.GracefulShutdown.GracefulStop(func() {
		_ = x.Close()
	})
}

// GracefulStop 优雅停机
func (x *Nsq) GracefulStop() {
	x.GracefulShutdown.GracefulStop(func() {
		_ = x.Close()
	})
}

// Close 关闭连接
func (x *Nsq) Close() error {
	x.mu.Lock()
	defer x.mu.Unlock()

	// 停止所有消费者
	for _, consumer := range x.consumers {
		consumer.Stop()
	}
	x.consumers = make(map[string]*nsq.Consumer)

	// 停止生产者
	if x.producer != nil {
		x.producer.Stop()
		x.producer = nil
	}

	x.BaseEndpoint.Destroy()
	return nil
}

// AddRouter 添加路由
// 为每个路由创建独立的消费者，如果路由已存在则直接报错
func (x *Nsq) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router cannot be nil")
	}

	routerId := router.GetId()
	if routerId == "" {
		routerId = router.GetFrom().ToString()
		router.SetId(routerId)
	}

	x.mu.Lock()
	defer x.mu.Unlock()

	// 检查routerId是否已存在，如果存在则直接报错
	if _, exists := x.consumers[routerId]; exists {
		return "", fmt.Errorf("routerId %s already exists", routerId)
	}

	// 解析topic和channel
	from := strings.TrimSpace(router.FromToString())
	topic := from
	channel := strings.TrimSpace(x.Config.Channel)
	if channel == "" {
		channel = "default"
	}

	// 如果有参数，第一个参数作为channel，优先级高于配置
	if len(params) > 0 {
		if ch, ok := params[0].(string); ok && ch != "" {
			channel = ch
		}
	}

	// 创建新的消费者配置
	consumerConfig := nsq.NewConfig()
	// 设置鉴权配置
	if x.Config.AuthToken != "" {
		consumerConfig.AuthSecret = x.Config.AuthToken
	}

	// 创建消费者
	consumer, err := nsq.NewConsumer(topic, channel, consumerConfig)
	if err != nil {
		return "", err
	}

	// 禁用NSQ内部日志输出
	consumer.SetLoggerLevel(nsq.LogLevelError)

	// 设置消息处理器，直接传递router参数
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		return x.handleMessage(message, router, topic)
	}))

	// 连接到lookupd或nsqd
	nsqdAddrs, lookupdAddrs := x.parseAddresses()
	if len(lookupdAddrs) > 0 {
		err = consumer.ConnectToNSQLookupds(lookupdAddrs)
	} else if len(nsqdAddrs) > 0 {
		if len(nsqdAddrs) == 1 {
			err = consumer.ConnectToNSQD(nsqdAddrs[0])
		} else {
			err = consumer.ConnectToNSQDs(nsqdAddrs)
		}
	} else {
		return "", errors.New("no NSQ address configured")
	}

	if err != nil {
		consumer.Stop()
		return "", err
	}

	// 保存消费者
	x.consumers[routerId] = consumer
	return routerId, nil
}

// RemoveRouter 移除路由
// 停止并删除指定路由的消费者
func (x *Nsq) RemoveRouter(routerId string, params ...interface{}) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	consumer, ok := x.consumers[routerId]
	if !ok {
		return errors.New("router not found")
	}

	// 停止消费者
	consumer.Stop()
	// 删除消费者
	delete(x.consumers, routerId)
	return nil
}

// handleMessage 处理单个消息
// 处理NSQ消息，创建Exchange并执行指定路由的规则链处理
func (x *Nsq) handleMessage(message *nsq.Message, router endpointApi.Router, topic string) error {
	defer func() {
		if e := recover(); e != nil {
			x.Printf("nsq endpoint handler err :\n%v", runtime.Stack())
		}
	}()

	exchange := &endpointApi.Exchange{
		In: &RequestMessage{
			message: message,
			topic:   topic,
		},
		Out: &ResponseMessage{
			message:  message,
			producer: x.producer,
			topic:    topic,
		},
	}
	x.DoProcess(context.Background(), router, exchange)
	return nil
}

// Start 启动服务
func (x *Nsq) Start() error {
	return nil
}

// discoverNsqdFromLookupd 通过lookupd API发现可用的nsqd地址
// 查询lookupd的/nodes接口获取所有可用的nsqd节点信息
func (x *Nsq) discoverNsqdFromLookupd(lookupdAddr string) (string, error) {
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

// Printf 打印日志
func (x *Nsq) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}
