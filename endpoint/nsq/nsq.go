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
	"sync/atomic"
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

// nsqPublisher 对单连接或多 nsqd 轮询发布抽象，便于运行期负载均衡
type nsqPublisher interface {
	Publish(topic string, body []byte) error
	Stop()
}

// roundRobinProducers 在多个 *nsq.Producer 上按消息轮询（round-robin）发布；
// 单次 Publish 若失败会依次尝试其余节点，兼顾负载均衡与单节点短暂不可用时的容错。
type roundRobinProducers struct {
	prods []*nsq.Producer
	rr    uint32
}

func (p *roundRobinProducers) Publish(topic string, body []byte) error {
	n := len(p.prods)
	if n == 0 {
		return errors.New("no nsqd producer in pool")
	}
	// 每次从轮询游标起算，使流量在节点间分散
	start := int(atomic.AddUint32(&p.rr, 1)-1) % n
	var lastErr error
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		err := p.prods[idx].Publish(topic, body)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

func (p *roundRobinProducers) Stop() {
	for _, pr := range p.prods {
		if pr != nil {
			pr.Stop()
		}
	}
}

// ResponseMessage 响应消息
type ResponseMessage struct {
	topic     string
	message   *nsq.Message
	publisher nsqPublisher
	body      []byte
	msg       *types.RuleMsg
	headers   textproto.MIMEHeader
	err       error
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
	if topic != "" && r.publisher != nil {
		err := r.publisher.Publish(topic, r.body)
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
	// 2. 多个nsqd: "127.0.0.1:4150,127.0.0.1:4151"（对全部可达节点建连，运行期按消息轮询发布，见 README.md）
	// 3. lookupd地址: "http://127.0.0.1:4161,http://127.0.0.1:4162"（按序尝试各 lookupd 的 /nodes，对返回的 nsqd 建连并轮询发布）
	// 使用说明与示例见同目录 README.md
	Server string `json:"server" label:"Server" desc:"NSQ server address. Supports nsqd 'host:port' (single or comma-separated multiple) or lookupd 'http://host:port' (comma-separated), e.g. 127.0.0.1:4150 or http://127.0.0.1:4161" required:"true" ref:"primary"`
	// 默认频道名称，如果AddRouter时未指定则使用此值
	Channel string `json:"channel" label:"Channel" desc:"Default channel name, used when AddRouter does not specify one"`
	// 鉴权令牌
	AuthToken string `json:"authToken" label:"Auth Token" desc:"NSQ authentication token"`
	// TLS证书文件
	CertFile string `json:"certFile" label:"Cert File" desc:"TLS certificate file path"`
	// TLS私钥文件
	CertKeyFile string `json:"certKeyFile" label:"Cert Key File" desc:"TLS private key file path"`
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
	// 发布端（单节点或多节点轮询）
	publisher nsqPublisher
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

func (x *Nsq) Def() types.ComponentForm {
	return types.ComponentForm{
		Desc: "NSQ consumer endpoint for subscribing to topics and processing messages",
		RouterForm: &types.RouterForm{
			From: &types.RouterFormField{
				Path: types.ComponentFormField{
					Name:     "path",
					Type:     "string",
					Label:    "Topic",
					Desc:     "NSQ topic to subscribe, e.g. orders",
					Required: true,
				},
			},
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

		// 多地址时对所有可达 nsqd 建立 Producer，运行期由 roundRobinProducers 轮询发布
		producerConfig := nsq.NewConfig()
		if x.Config.AuthToken != "" {
			producerConfig.AuthSecret = x.Config.AuthToken
		}

		var nsqdCandidates []string
		if len(nsqdAddrs) > 0 {
			nsqdCandidates = nsqdAddrs
		} else if len(lookupdAddrs) > 0 {
			discovered, discoverErr := discoverNsqdProducersFromLookupds(lookupdAddrs)
			if discoverErr != nil {
				return discoverErr
			}
			nsqdCandidates = discovered
		} else {
			nsqdCandidates = []string{strings.TrimSpace(x.Config.Server)}
		}

		if len(nsqdCandidates) > 0 {
			nsqdCandidates = dedupeAddrsStable(nsqdCandidates)
			prods, connectErr := buildReachableProducers(nsqdCandidates, producerConfig)
			if connectErr != nil {
				return connectErr
			}
			x.publisher = &roundRobinProducers{prods: prods}
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

	// 停止发布端
	if x.publisher != nil {
		x.publisher.Stop()
		x.publisher = nil
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
			message:   message,
			publisher: x.publisher,
			topic:     topic,
		},
	}
	x.DoProcess(context.Background(), router, exchange)
	return nil
}

// Start 启动服务
func (x *Nsq) Start() error {
	return nil
}

// lookupdNodesProducer 对应 lookupd /nodes 中的单个 producer
type lookupdNodesProducer struct {
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

func nsqdAddrFromLookupdProducer(p lookupdNodesProducer) (string, bool) {
	if p.TCPPort <= 0 || p.TCPPort > 65535 {
		return "", false
	}
	// 与历史行为一致：优先 broadcast + tcp_port，否则 remote + tcp_port
	if p.BroadcastAddress != "" {
		return fmt.Sprintf("%s:%d", p.BroadcastAddress, p.TCPPort), true
	}
	if p.RemoteAddress != "" {
		return fmt.Sprintf("%s:%d", p.RemoteAddress, p.TCPPort), true
	}
	return "", false
}

// dedupeAddrsStable 按首次出现顺序去重
func dedupeAddrsStable(addrs []string) []string {
	seen := make(map[string]struct{}, len(addrs))
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		if _, ok := seen[a]; ok {
			continue
		}
		seen[a] = struct{}{}
		out = append(out, a)
	}
	return out
}

// fetchNsqdProducersFromLookupd 请求单个 lookupd 的 /nodes，返回可拨号的 nsqd 地址列表
func fetchNsqdProducersFromLookupd(lookupdAddr string) ([]string, error) {
	apiURL := fmt.Sprintf("%s/nodes", strings.TrimSuffix(lookupdAddr, "/"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build lookupd request: %w", err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query lookupd API: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("lookupd API status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read lookupd response: %w", err)
	}
	var response struct {
		Producers []lookupdNodesProducer `json:"producers"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("parse lookupd response: %w", err)
	}
	if len(response.Producers) == 0 {
		return nil, nil
	}
	candidates := make([]string, 0, len(response.Producers))
	for _, pr := range response.Producers {
		if addr, ok := nsqdAddrFromLookupdProducer(pr); ok {
			candidates = append(candidates, addr)
		}
	}
	return dedupeAddrsStable(candidates), nil
}

// discoverNsqdProducersFromLookupds 按顺序尝试多个 lookupd，在首次成功且返回非空
// 的 nsqd 列表时即采用该列表。全部失败时汇聚错误返回。
func discoverNsqdProducersFromLookupds(lookupdAddrs []string) ([]string, error) {
	if len(lookupdAddrs) == 0 {
		return nil, errors.New("no lookupd address configured")
	}
	var errs []error
	for _, u := range lookupdAddrs {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		addrs, err := fetchNsqdProducersFromLookupd(u)
		if err != nil {
			errs = append(errs, fmt.Errorf("lookupd %s: %w", u, err))
			continue
		}
		if len(addrs) == 0 {
			errs = append(errs, fmt.Errorf("lookupd %s: no nsqd nodes in /nodes response", u))
			continue
		}
		return addrs, nil
	}
	if len(errs) == 0 {
		return nil, errors.New("no non-empty lookupd address in configuration")
	}
	return nil, fmt.Errorf("all lookupd failed: %w", errors.Join(errs...))
}

// buildReachableProducers 为每个候选地址建立 Producer 并 Ping，保留所有成功的实例；不可达会 Stop 并跳过。
// 多实例供 roundRobinProducers 在运行期做负载均衡与发布失败时向其他节点重试。
func buildReachableProducers(candidates []string, cfg *nsq.Config) ([]*nsq.Producer, error) {
	if len(candidates) == 0 {
		return nil, errors.New("no nsqd address candidates")
	}
	var out []*nsq.Producer
	var lastErr error
	for _, addr := range candidates {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		p, err := nsq.NewProducer(addr, cfg)
		if err != nil {
			lastErr = err
			continue
		}
		if err := p.Ping(); err != nil {
			p.Stop()
			lastErr = err
			continue
		}
		p.SetLoggerLevel(nsq.LogLevelError)
		out = append(out, p)
	}
	if len(out) == 0 {
		if lastErr == nil {
			lastErr = errors.New("no valid non-empty address in nsqd candidate list")
		}
		return nil, fmt.Errorf("no reachable nsqd: %w", lastErr)
	}
	return out, nil
}

// Printf 打印日志
func (x *Nsq) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}
