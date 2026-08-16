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

// Package fasthttp provides a high-performance HTTP endpointApi implementation for the RuleGo framework.
// It uses the fasthttp library to achieve better performance compared to the standard net/http package.
//
// Key components in this package include:
// - Endpoint (alias FastHttp): Implements the HTTP server and request handling using fasthttp
// - RequestMessage: Represents an incoming HTTP request
// - ResponseMessage: Represents the HTTP response to be sent back
//
// The FastHTTP endpointApi supports dynamic routing configuration, allowing users to
// define routes and their corresponding rule chain or component destinations.
// It also provides flexibility in handling different HTTP methods and content types.
//
// This package integrates with the broader RuleGo ecosystem, enabling seamless
// data flow from HTTP requests to rule processing and back to HTTP responses.
package fasthttp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/textproto"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rulego/rulego/endpoint/rest"

	"github.com/rulego/rulego/endpoint"

	"github.com/fasthttp/router"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	nodeBase "github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/engine"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
	"github.com/rulego/rulego/utils/str"
	"github.com/valyala/fasthttp"
)

const (
	ContentTypeKey                      = "Content-Type"
	JsonContextType                     = "application/json"
	HeaderKeyAccessControlRequestMethod = "Access-Control-Request-Method"
	HeaderKeyAccessControlAllowMethods  = "Access-Control-Allow-Methods"
	HeaderKeyAccessControlAllowHeaders  = "Access-Control-Allow-Headers"
	HeaderKeyAccessControlAllowOrigin   = "Access-Control-Allow-Origin"
	HeaderValueAll                      = "*"
)

// Type 组件类型
const Type = rest.Type

// Endpoint 别名
type Endpoint = FastHttp

var _ endpointApi.Endpoint = (*Endpoint)(nil)
var _ endpointApi.HttpEndpoint = (*Endpoint)(nil)

// 注册组件
// 在300并发以上，相对于标准的 http endpoint 组件，性能提升3倍
func init() {
	// 可以使用fasthttp代替标准http endpoint组件
	// 1. 删除标准 http endpoint 组件
	_ = endpoint.Registry.Unregister(Type)
	// 2. 注册fasthttp版本 http endpoint组件
	_ = endpoint.Registry.Register(&Endpoint{})
}

// RequestMessage fasthttp请求消息
type RequestMessage struct {
	ctx  *fasthttp.RequestCtx
	body []byte
	// handler 在返回前对请求做的快照：ctx 在 handler 返回后会被 fasthttp 回收复用，
	// 异步链路再访问 ctx 会读到别的请求数据
	method      string
	uri         string
	headers     textproto.MIMEHeader
	queryArgs   map[string]string
	//路径参数
	Params   map[string]string
	msg      *types.RuleMsg
	err      error
	Metadata *types.Metadata
}

// snapshot 在 handler 内拷贝请求侧数据，之后所有访问走快照
func (r *RequestMessage) snapshot(ctx *fasthttp.RequestCtx) {
	r.method = string(ctx.Method())
	r.uri = string(ctx.RequestURI())
	r.body = append([]byte{}, ctx.PostBody()...)
	r.headers = make(textproto.MIMEHeader)
	ctx.Request.Header.VisitAll(func(key, value []byte) {
		r.headers.Add(string(key), string(value))
	})
	r.queryArgs = make(map[string]string)
	ctx.QueryArgs().VisitAll(func(key, value []byte) {
		r.queryArgs[string(key)] = string(value)
	})
}

func (r *RequestMessage) Body() []byte {
	if r.body == nil && r.ctx != nil {
		r.body = ctxPostBody(r.ctx)
	}
	return r.body
}

func (r *RequestMessage) Headers() textproto.MIMEHeader {
	if r.headers != nil {
		return r.headers
	}
	if r.ctx == nil {
		return nil
	}
	headers := make(textproto.MIMEHeader)
	r.ctx.Request.Header.VisitAll(func(key, value []byte) {
		headers.Add(string(key), string(value))
	})
	return headers
}

func (r *RequestMessage) AddHeader(key, value string) {
	if r.headers != nil {
		r.headers.Add(key, value)
	} else if r.ctx != nil {
		r.ctx.Request.Header.Add(key, value)
	}
}

func (r *RequestMessage) SetHeader(key, value string) {
	if r.headers != nil {
		r.headers.Set(key, value)
	} else if r.ctx != nil {
		r.ctx.Request.Header.Set(key, value)
	}
}

func (r *RequestMessage) DelHeader(key string) {
	if r.headers != nil {
		r.headers.Del(key)
	} else if r.ctx != nil {
		r.ctx.Request.Header.Del(key)
	}
}
func (r *RequestMessage) GetMetadata() *types.Metadata {
	if r.Metadata == nil {
		r.Metadata = types.NewMetadata()
	}
	return r.Metadata
}

func (r RequestMessage) From() string {
	if r.uri != "" {
		return r.uri
	}
	if r.ctx == nil {
		return ""
	}
	return string(r.ctx.RequestURI())
}

func (r *RequestMessage) GetParam(key string) string {
	if v, ok := r.Params[key]; ok {
		return v
	}
	if v, ok := r.queryArgs[key]; ok {
		return v
	}
	if r.ctx == nil {
		return ""
	}
	return string(r.ctx.QueryArgs().Peek(key))
}

func (r *RequestMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

func (r *RequestMessage) GetMsg() *types.RuleMsg {
	if r.msg == nil {
		dataType := types.TEXT
		var data string
		method := r.method
		if method == "" && r.ctx != nil {
			method = string(r.ctx.Method())
		}
		if method == fasthttp.MethodGet {
			dataType = types.JSON
			queryArgs := make(map[string]interface{})
			if r.queryArgs != nil {
				for k, v := range r.queryArgs {
					queryArgs[k] = v
				}
			} else if r.ctx != nil {
				r.ctx.QueryArgs().VisitAll(func(key, value []byte) {
					queryArgs[string(key)] = string(value)
				})
			}
			data = str.ToString(queryArgs)
		} else {
			if contentType := r.Headers().Get(ContentTypeKey); strings.HasPrefix(contentType, JsonContextType) {
				dataType = types.JSON
			}
			data = string(r.Body())
		}
		if r.Metadata == nil {
			r.Metadata = types.NewMetadata()
		}
		ruleMsg := types.NewMsg(0, r.From(), dataType, r.Metadata, data)
		r.msg = &ruleMsg
	}
	return r.msg
}

func (r *RequestMessage) SetStatusCode(statusCode int) {
}

func (r *RequestMessage) SetBody(body []byte) {
	r.body = body
}

func (r *RequestMessage) SetError(err error) {
	r.err = err
}

func (r *RequestMessage) GetError() error {
	return r.err
}

func (r *RequestMessage) RequestCtx() *fasthttp.RequestCtx {
	return r.ctx
}

// ResponseMessage fasthttp响应消息
type ResponseMessage struct {
	ctx  *fasthttp.RequestCtx
	body []byte
	to   string
	msg  *types.RuleMsg
	err  error
	// gate 非 nil 表示 handler 走流式路径，SetBody 经由 gate 送到客户端
	gate *streamGate
	mu   sync.Mutex
	// streaming 置位后响应头已提交，header/状态码写入变为 no-op
	streaming     bool
	cachedHeaders textproto.MIMEHeader
	// onStreamStart 首次 Flush 时调用，通知 handler 提前返回以支持 SSE 流式
	onStreamStart func()
}

func (r *ResponseMessage) Body() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.body
}

func (r *ResponseMessage) Headers() textproto.MIMEHeader {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ctx == nil {
		return nil
	}
	if r.streaming {
		return r.cachedHeaders
	}
	headers := make(textproto.MIMEHeader)
	r.ctx.Response.Header.VisitAll(func(key, value []byte) {
		headers.Add(string(key), string(value))
	})
	return headers
}

func (r *ResponseMessage) AddHeader(key, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ctx != nil && !r.streaming {
		r.ctx.Response.Header.Add(key, value)
	}
}

func (r *ResponseMessage) SetHeader(key, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ctx != nil && !r.streaming {
		r.ctx.Response.Header.Set(key, value)
	}
}

func (r *ResponseMessage) DelHeader(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ctx != nil && !r.streaming {
		r.ctx.Response.Header.Del(key)
	}
}

// fasthttpResponseWriter 适配器，将 fasthttp.RequestCtx 适配为 http.ResponseWriter
type fasthttpResponseWriter struct {
	ctx    *fasthttp.RequestCtx
	header http.Header
	status int
}

func (w *fasthttpResponseWriter) Header() http.Header {
	return w.header
}

func (w *fasthttpResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ctx.Write(data)
}

func (w *fasthttpResponseWriter) WriteHeader(statusCode int) {
	if w.status != 0 {
		return // 已经写入过状态码
	}
	w.status = statusCode
	w.ctx.SetStatusCode(statusCode)

	// 复制头部信息到 fasthttp context
	for key, values := range w.header {
		for _, value := range values {
			w.ctx.Response.Header.Add(key, value)
		}
	}
}

func (r *ResponseMessage) GetMetadata() *types.Metadata {
	if msg := r.GetMsg(); msg != nil {
		return msg.GetMetadata()
	}
	return nil
}
func (r *ResponseMessage) From() string {
	if r.ctx == nil {
		return ""
	}
	return string(r.ctx.RequestURI())
}

func (r *ResponseMessage) GetParam(key string) string {
	if r.ctx == nil {
		return ""
	}
	return string(r.ctx.QueryArgs().Peek(key))
}

func (r *ResponseMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

func (r *ResponseMessage) GetMsg() *types.RuleMsg {
	return r.msg
}

func (r *ResponseMessage) SetStatusCode(statusCode int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ctx != nil && !r.streaming {
		r.ctx.SetStatusCode(statusCode)
	}
}

func (r *ResponseMessage) SetBody(body []byte) {
	r.mu.Lock()
	r.body = body
	gate := r.gate
	r.mu.Unlock()
	if gate != nil {
		// 流式路径：通过 gate 写出
		gate.write(body)
	}
	// 非流式路径：body 缓存在 r.body，由 handler 在 procDone 后写出。
	// Flush 被调用时惰性创建 gate 并接管后续写入。
}

func (r *ResponseMessage) SetError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func (r *ResponseMessage) GetError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// Flush 将缓冲数据实时推送到客户端，用于 SSE 流式响应。
// 首次调用时惰性创建 streamGate 和 bodyStreamWriter，未调用 Flush 的请求零开销。
//
// 两条路径下的行为：
//   - 流式路由（streaming=true）：onStreamStart 通知 handler 提前返回，
//     fasthttp 开始增量推送 gate 中的 chunk；
//   - 普通路由（默认）：handler 仍在同步执行，chunk 只在 gate 排队，
//     处理结束后随 handler 返回一次性写出（内容完整，不增量）。
func (r *ResponseMessage) Flush() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ctx == nil {
		return
	}
	// 惰性初始化流式通道：只在首次 Flush 时创建
	if r.gate == nil {
		r.gate = newStreamGate()
		r.ctx.SetBodyStreamWriter(func(w *bufio.Writer) {
			defer func() {
				if e := recover(); e != nil {
					r.gate.markDead()
				}
			}()
			r.gate.drain(w)
		})
		// 把 SetBody 已缓冲的 body 通过 gate 写出
		if len(r.body) > 0 {
			r.gate.write(r.body)
		}
		// 通知 handler 可以提前返回，fasthttp 开始推送流式数据
		if r.onStreamStart != nil {
			r.onStreamStart()
			r.onStreamStart = nil
		}
	}
}

func (r *ResponseMessage) RequestCtx() *fasthttp.RequestCtx {
	return r.ctx
}

// beginStreaming 在首个响应 chunk 写出前调用：此后响应头即将提交，
// header/状态码写入变为 no-op，Headers() 改读快照
func (r *ResponseMessage) beginStreaming() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.streaming {
		return
	}
	r.streaming = true
	if r.ctx != nil {
		headers := make(textproto.MIMEHeader)
		r.ctx.Response.Header.VisitAll(func(key, value []byte) {
			headers.Set(string(key), string(value))
		})
		r.cachedHeaders = headers
	}
}

// streamGate 桥接规则链处理 goroutine（SetBody 生产 chunk）与 fasthttp
// bodyStream 回调 goroutine（drain 消费 chunk 写给客户端）。
// queue 有字节上限：客户端消费慢时生产端阻塞等待，形成背压。
type streamGate struct {
	mu         sync.Mutex
	dataCond   *sync.Cond
	spaceCond  *sync.Cond
	queue      [][]byte
	bytes      int
	closed     bool
	dead       bool
	firstWrite chan struct{}
	firstOnce  sync.Once
}

const streamGateMaxBytes = 2 << 20 // 2MB

func newStreamGate() *streamGate {
	g := &streamGate{firstWrite: make(chan struct{})}
	g.dataCond = sync.NewCond(&g.mu)
	g.spaceCond = sync.NewCond(&g.mu)
	return g
}

// write 由处理 goroutine 调用，firstWrite 唤醒等待中的 handler
func (g *streamGate) write(chunk []byte) {
	if len(chunk) == 0 {
		return
	}
	c := append([]byte{}, chunk...)
	g.mu.Lock()
	defer g.mu.Unlock()
	g.firstOnce.Do(func() { close(g.firstWrite) })
	for !g.closed && !g.dead && g.bytes+len(c) > streamGateMaxBytes {
		g.spaceCond.Wait()
	}
	if g.closed || g.dead {
		return
	}
	g.queue = append(g.queue, c)
	g.bytes += len(c)
	g.dataCond.Signal()
}

// close 处理结束时调用，drain 循环清空队列后退出
func (g *streamGate) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	g.dataCond.Broadcast()
	g.spaceCond.Broadcast()
}

func (g *streamGate) markDead() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.dead = true
	g.queue = nil
	g.bytes = 0
	g.dataCond.Broadcast()
	g.spaceCond.Broadcast()
}

// drain 在 fasthttp bodyStream 回调 goroutine 中运行
func (g *streamGate) drain(w *bufio.Writer) {
	for {
		g.mu.Lock()
		for len(g.queue) == 0 && !g.closed {
			g.dataCond.Wait()
		}
		if len(g.queue) == 0 {
			g.mu.Unlock()
			return
		}
		chunk := g.queue[0]
		g.queue = g.queue[1:]
		g.bytes -= len(chunk)
		g.spaceCond.Signal()
		g.mu.Unlock()
		if _, err := w.Write(chunk); err != nil {
			g.markDead()
			return
		}
		if err := w.Flush(); err != nil {
			g.markDead()
			return
		}
	}
}

func ctxPostBody(ctx *fasthttp.RequestCtx) []byte {
	return append([]byte{}, ctx.PostBody()...)
}

// Config FastHttp 服务配置
type Config struct {
	Server      string `json:"server" label:"Server" desc:"Listen address, format host:port or :port, e.g. :8080" required:"true" ref:"primary"` //服务器地址
	CertFile    string `json:"certFile" label:"Cert File" desc:"TLS certificate file path; provide together with certKeyFile to enable HTTPS"`    //证书文件
	CertKeyFile string `json:"certKeyFile" label:"Cert Key File" desc:"TLS private key file path; provide together with certFile to enable HTTPS"` //密钥文件
	//是否允许跨域
	AllowCors bool `json:"allowCors" label:"Allow CORS" desc:"Whether to allow cross-origin requests"`
	// FastHTTP服务器配置
	ReadTimeout      int    `json:"readTimeout" label:"Read Timeout (s)" desc:"Read timeout in seconds; 0 uses default 10"`               // 读取超时时间（秒），0使用默认值10秒
	WriteTimeout     int    `json:"writeTimeout" label:"Write Timeout (s)" desc:"Write timeout in seconds; 0 uses default 10"`         // 写入超时时间（秒），0使用默认值10秒
	IdleTimeout      int    `json:"idleTimeout" label:"Idle Timeout (s)" desc:"Idle timeout in seconds; 0 uses default 60"`           // 空闲超时时间（秒），0使用默认值60秒
	DisableKeepalive bool   `json:"disableKeepalive" label:"Disable Keepalive" desc:"Whether to disable keep-alive"`                     //  禁用keepalive
	MaxRequestSize   string `json:"maxRequestSize" label:"Max Request Size" desc:"Max request body size, supports 4M/4m/10K formats; default 4M"` // 最大请求体大小，支持4M、4m、10K等格式，默认4M
	Concurrency      int    `json:"concurrency" label:"Concurrency" desc:"Concurrency; 0 uses default 256 * 1024"`                              // 并发数，0使用默认值 256 * 1024
	//// 新增配置项用于控制连接和资源管理
	//MaxConnsPerIP        int           `json:"maxConnsPerIP"`        // 每个IP的最大连接数
	//MaxRequestsPerConn   int           `json:"maxRequestsPerConn"`   // 每个连接的最大请求数
	//MaxKeepaliveDuration time.Duration `json:"maxKeepaliveDuration"` // keepalive最大持续时间
	//ReadBufferSize       int           `json:"readBufferSize"`       // 读缓冲区大小
	//WriteBufferSize      int           `json:"writeBufferSize"`      // 写缓冲区大小
	//ReduceMemoryUsage    bool          `json:"reduceMemoryUsage"`    // 减少内存使用
	//StreamRequestBody    bool          `json:"streamRequestBody"`    // 流式处理请求体
}

// FastHttp 接收端端点
type FastHttp struct {
	impl.BaseEndpoint
	nodeBase.SharedNode[*FastHttp]
	//配置
	Config     Config
	RuleConfig types.Config
	Server     *fasthttp.Server
	//http路由器
	router  *router.Router
	started bool
	// resourceMapping is the resource mapping for static file serving
	resourceMapping string
}

// Type 组件类型
func (fh *FastHttp) Type() string {
	return Type
}

func (fh *FastHttp) New() types.Node {
	return &FastHttp{
		Config: Config{
			Server:         ":6333",
			ReadTimeout:    10,    // 0使用默认值10秒
			WriteTimeout:   10,    // 0使用默认值10秒
			IdleTimeout:    60,    // 0使用默认值60秒
			MaxRequestSize: "4M",  // 默认4MB
			Concurrency:    10000, // 并发数
			//DisableKeepalive: true,             // 禁用keepalive
		},
	}
}

// Init 初始化
func (fh *FastHttp) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &fh.Config)
	if err != nil {
		return err
	}
	fh.RuleConfig = ruleConfig
	return fh.SharedNode.InitWithClose(fh.RuleConfig, fh.Type(), fh.Config.Server, false, func() (*FastHttp, error) {
		return fh.initServer()
	}, func(server *FastHttp) error {
		if server != nil {
			return server.Close()
		}
		return nil
	})
}

// Destroy 销毁
func (fh *FastHttp) Destroy() {
	_ = fh.Close()
}

func (fh *FastHttp) Restart() error {
	// 使用统一的关闭方法
	fh.shutdownServer()

	if fh.SharedNode.InstanceId != "" {
		if shared, err := fh.SharedNode.GetSafely(); err == nil {
			return shared.Restart()
		} else {
			return err
		}
	}
	if fh.router != nil {
		fh.newRouter()
	}
	var oldRouter = make(map[string]endpointApi.Router)

	fh.Lock()
	for id, router := range fh.RouterStorage {
		if !router.IsDisable() {
			oldRouter[id] = router
		}
	}
	fh.Unlock()

	fh.RouterStorage = make(map[string]endpointApi.Router)

	if err := fh.Start(); err != nil {
		return err
	}

	if fh.OnEvent != nil {
		fh.OnEvent(endpointApi.EventRestart, oldRouter)
	}

	for _, router := range oldRouter {
		if len(router.GetParams()) == 0 {
			router.SetParams("GET")
		}
		if !fh.HasRouter(router.GetId()) {
			if _, err := fh.AddRouter(router, router.GetParams()...); err != nil {
				fh.Printf("fasthttp add router path:=%s error:%v", router.FromToString(), err)
				continue
			}
		}
	}
	if fh.resourceMapping != "" {
		fh.RegisterStaticFiles(fh.resourceMapping)
	}
	return nil
}

// shutdownServer 统一的服务器关闭逻辑
func (fh *FastHttp) shutdownServer() {
	fh.Lock()
	if !fh.started || fh.Server == nil {
		fh.Unlock()
		return
	}
	server := fh.Server
	fh.started = false
	fh.Server = nil
	fh.Unlock()

	if fh.Config.Server != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.ShutdownWithContext(ctx)
	}

	// 等待一小段时间确保端口完全释放
	time.Sleep(100 * time.Millisecond)
}

func (fh *FastHttp) Close() error {
	// 使用统一的关闭方法
	fh.shutdownServer()

	fh.Lock()
	if fh.router != nil {
		fh.newRouter()
	}
	fh.Unlock()

	if fh.SharedNode.InstanceId != "" {
		if shared, err := fh.SharedNode.GetSafely(); err == nil {
			fh.RLock()
			defer fh.RUnlock()
			for key := range fh.RouterStorage {
				shared.deleteRouter(key)
			}
			//重启共享服务
			return shared.Restart()
		}
	}

	fh.BaseEndpoint.Destroy()
	return nil
}

func (fh *FastHttp) Id() string {
	return fh.Config.Server
}

func (fh *FastHttp) AddRouter(router endpointApi.Router, params ...interface{}) (id string, err error) {
	if len(params) <= 0 {
		return "", errors.New("need to specify HTTP method")
	} else if router == nil {
		return "", errors.New("router can not nil")
	} else {
		defer func() {
			if e := recover(); e != nil {
				err = fmt.Errorf("addRouter err :%v", e)
			}
		}()
		err2 := fh.addRouter(strings.ToUpper(str.ToString(params[0])), router)
		return router.GetId(), err2
	}
}

func (fh *FastHttp) RemoveRouter(routerId string, params ...interface{}) error {
	routerId = strings.TrimSpace(routerId)
	fh.Lock()
	defer fh.Unlock()
	if fh.RouterStorage != nil {
		if router, ok := fh.RouterStorage[routerId]; ok && !router.IsDisable() {
			router.Disable(true)
			return nil
		} else {
			return fmt.Errorf("router: %s not found", routerId)
		}
	}
	return nil
}

func (fh *FastHttp) deleteRouter(routerId string) {
	routerId = strings.TrimSpace(routerId)
	fh.Lock()
	defer fh.Unlock()
	if fh.RouterStorage != nil {
		delete(fh.RouterStorage, routerId)
	}
}

func (fh *FastHttp) Start() error {
	if err := fh.checkIsInitSharedNode(); err != nil {
		return err
	}
	if netResource, err := fh.SharedNode.GetSafely(); err == nil {
		return netResource.startServer()
	} else {
		return err
	}
}

func (fh *FastHttp) Listen() (net.Listener, error) {
	addr := fh.Config.Server
	if addr == "" {
		if fh.Config.CertKeyFile != "" && fh.Config.CertFile != "" {
			addr = ":https"
		} else {
			addr = ":http"
		}
	}
	return net.Listen("tcp", addr)
}

// addRouter 注册1个或者多个路由
func (fh *FastHttp) addRouter(method string, routers ...endpointApi.Router) error {
	method = strings.ToUpper(method)

	fh.Lock()
	defer fh.Unlock()

	if fh.RouterStorage == nil {
		fh.RouterStorage = make(map[string]endpointApi.Router)
	}
	for _, item := range routers {
		path := strings.TrimSpace(item.FromToString())
		if id := item.GetId(); id == "" {
			item.SetId(fh.RouterKey(method, path))
		}
		//存储路由
		item.SetParams(method)
		fh.RouterStorage[item.GetId()] = item
		if fh.SharedNode.InstanceId != "" {
			if shared, err := fh.SharedNode.GetSafely(); err == nil {
				return shared.addRouter(method, item)
			} else {
				return err
			}
		} else {
			if fh.router == nil {
				fh.newRouter()
			}
			isWait := false
			isStreaming := false
			if from := item.GetFrom(); from != nil {
				if to := from.GetTo(); to != nil {
					isWait = to.IsWait()
				}
				isStreaming = configIsStreaming(from.GetConfiguration())
			}
			// 转换路径参数格式：将 :id 格式转换为 {id} 格式
			path = convertPathParams(path)
			fh.router.Handle(method, path, fh.handler(item, isWait, isStreaming))
		}
	}
	return nil
}

func (fh *FastHttp) GET(routers ...endpointApi.Router) endpointApi.HttpEndpoint {
	fh.addRouter(fasthttp.MethodGet, routers...)
	return fh
}

func (fh *FastHttp) HEAD(routers ...endpointApi.Router) endpointApi.HttpEndpoint {
	fh.addRouter(fasthttp.MethodHead, routers...)
	return fh
}

func (fh *FastHttp) OPTIONS(routers ...endpointApi.Router) endpointApi.HttpEndpoint {
	fh.addRouter(fasthttp.MethodOptions, routers...)
	return fh
}

func (fh *FastHttp) POST(routers ...endpointApi.Router) endpointApi.HttpEndpoint {
	fh.addRouter(fasthttp.MethodPost, routers...)
	return fh
}

func (fh *FastHttp) PUT(routers ...endpointApi.Router) endpointApi.HttpEndpoint {
	fh.addRouter(fasthttp.MethodPut, routers...)
	return fh
}

func (fh *FastHttp) PATCH(routers ...endpointApi.Router) endpointApi.HttpEndpoint {
	fh.addRouter(fasthttp.MethodPatch, routers...)
	return fh
}

func (fh *FastHttp) DELETE(routers ...endpointApi.Router) endpointApi.HttpEndpoint {
	fh.addRouter(fasthttp.MethodDelete, routers...)
	return fh
}

func (fh *FastHttp) GlobalOPTIONS(handler http.Handler) endpointApi.HttpEndpoint {
	fh.Router().GlobalOPTIONS = func(ctx *fasthttp.RequestCtx) {
		// 创建标准的 http.ResponseWriter 和 *http.Request 适配器
		req := &http.Request{
			Method: string(ctx.Method()),
			//URL:        ctx.URI(),
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header:     make(http.Header),
			Body:       nil,
			Host:       string(ctx.Host()),
			RequestURI: string(ctx.RequestURI()),
		}

		// 复制请求头
		ctx.Request.Header.VisitAll(func(key, value []byte) {
			req.Header.Add(string(key), string(value))
		})

		// 创建响应写入器适配器
		w := &fasthttpResponseWriter{
			ctx:    ctx,
			header: make(http.Header),
		}

		// 调用原始的 http.Handler
		handler.ServeHTTP(w, req)
	}
	return fh
}

// LoadServeFiles 加载静态文件映射
// resourceMapping 格式: "urlPath1=localDir1,urlPath2=localDir2"
// 例如: "/static/*filepath=./static,/assets/*filepath=./assets"
func (fh *FastHttp) RegisterStaticFiles(resourceMapping string) endpointApi.HttpEndpoint {
	if resourceMapping == "" {
		return fh
	}
	fh.resourceMapping = resourceMapping
	mapping := strings.Split(resourceMapping, ",")
	for _, item := range mapping {
		files := strings.Split(item, "=")
		if len(files) == 2 {
			urlPath := strings.TrimSpace(files[0])
			localDir := strings.TrimSpace(files[1])

			// 移除 /*filepath 后缀以获取基础路径
			basePath := urlPath
			if strings.HasSuffix(urlPath, "/*filepath") {
				basePath = urlPath[:len(urlPath)-10]
			}

			// 确保路径以 /{filepath:*} 结尾，这是 fasthttp router 的要求
			if !strings.HasSuffix(urlPath, "/{filepath:*}") {
				if strings.HasSuffix(basePath, "/") {
					urlPath = basePath + "{filepath:*}"
				} else {
					urlPath = basePath + "/{filepath:*}"
				}
			}

			// 使用 router 的 ServeFiles 方法
			fh.Router().ServeFiles(urlPath, localDir)
		}
	}
	return fh
}

func (fh *FastHttp) checkIsInitSharedNode() error {
	if !fh.SharedNode.IsInit() {
		err := fh.SharedNode.InitWithClose(fh.RuleConfig, fh.Type(), fh.Config.Server, false, func() (*FastHttp, error) {
			return fh.initServer()
		}, func(server *FastHttp) error {
			if server != nil {
				return server.Close()
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (fh *FastHttp) Router() *router.Router {
	fh.checkIsInitSharedNode()

	if fromPool, err := fh.SharedNode.GetSafely(); err != nil {
		fh.Printf("get router err :%v", err)
		return fh.newRouter()
	} else {
		return fromPool.router
	}
}

func (fh *FastHttp) RouterKey(method string, from string) string {
	return method + ":" + from
}

// handler 把请求交给规则链处理。fasthttp 的关键约束：没有 handler 内 Flush，
// 流式推送只能靠 SetBodyStreamWriter，而它要求 handler 先返回。
// 由此分两条路径：
//
//	普通请求（默认，未标记 streaming）：同步路径。DoProcess 在 fasthttp worker
//	goroutine 内直接执行完，handler 返回后 fasthttp 写出响应。零额外开销，
//	与 net/http 行为一致。
//
//	流式请求（from 配置 streaming=true，如 SSE）：异步路径。处理逻辑移入独立
//	goroutine，handler 在首个 chunk 就绪（Flush 被调用）后提前返回，fasthttp
//	随即通过 bodyStreamWriter 增量推送。
func (fh *FastHttp) handler(router endpointApi.Router, isWait, isStreaming bool) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		defer func() {
			if e := recover(); e != nil {
				fh.Printf("fasthttp endpointApi handler err :\n%v", runtime.Stack())
				ctx.SetStatusCode(fasthttp.StatusInternalServerError)
				ctx.SetBodyString("Internal Server Error")
			}
		}()
		if router.IsDisable() {
			ctx.SetStatusCode(fasthttp.StatusNotFound)
			return
		}
		metadata := types.NewMetadata()
		// 无路径参数的路由不分配 map
		var params map[string]string

		ctx.VisitUserValues(func(key []byte, value interface{}) {
			if v, ok := value.(string); ok {
				if params == nil {
					params = make(map[string]string)
				}
				params[string(key)] = v
				metadata.PutValue(string(key), v)
			}
		})

		requestMsg := &RequestMessage{
			ctx:      ctx,
			Params:   params,
			Metadata: metadata,
		}
		requestMsg.snapshot(ctx)
		respMsg := &ResponseMessage{ctx: ctx}
		exchange := &endpointApi.Exchange{
			In:  requestMsg,
			Out: respMsg,
		}

		for k, v := range requestMsg.queryArgs {
			metadata.PutValue(k, v)
		}

		if fh.Config.AllowCors {
			ctx.Response.Header.Set(HeaderKeyAccessControlAllowOrigin, HeaderValueAll)
			ctx.Response.Header.Set(HeaderKeyAccessControlAllowMethods, HeaderValueAll)
			ctx.Response.Header.Set(HeaderKeyAccessControlAllowHeaders, HeaderValueAll)
		}

		var reqCtx = context.Background()
		if isWait {
			var cancel context.CancelFunc
			reqCtx, cancel = context.WithTimeout(reqCtx, 30*time.Second)
			defer cancel()
		}
		exchange.Context = reqCtx

		if !isStreaming {
			fh.processSync(reqCtx, router, exchange, respMsg, ctx)
			return
		}
		fh.processStreaming(reqCtx, router, exchange, respMsg, ctx)
	}
}

// processSync 普通请求路径：同步执行，无额外 goroutine/channel。
func (fh *FastHttp) processSync(reqCtx context.Context, router endpointApi.Router, exchange *endpointApi.Exchange, respMsg *ResponseMessage, ctx *fasthttp.RequestCtx) {
	fh.doProcessSafely(reqCtx, router, exchange, respMsg)
	respMsg.mu.Lock()
	gate := respMsg.gate
	body := respMsg.body
	respMsg.mu.Unlock()
	if gate != nil {
		// 未标记流式的路由调用了 Flush（SSE 数据已缓冲在 gate）：
		// handler 返回后由 bodyStreamWriter 一次性写出，内容完整但不增量。
		gate.close()
		return
	}
	if len(body) > 0 {
		ctx.Write(body)
	}
}

// processStreaming 流式请求路径：处理逻辑在独立 goroutine，handler 等待
// 首个 Flush（提前返回，开始增量推送）或处理完成（当作普通请求写出）。
func (fh *FastHttp) processStreaming(reqCtx context.Context, router endpointApi.Router, exchange *endpointApi.Exchange, respMsg *ResponseMessage, ctx *fasthttp.RequestCtx) {
	procDone := make(chan struct{})
	streamStart := make(chan struct{})
	respMsg.onStreamStart = sync.OnceFunc(func() {
		close(streamStart)
	})

	go func() {
		defer close(procDone)
		defer func() {
			// 关闭 gate 让 drain 循环退出（流式路径）
			respMsg.mu.Lock()
			if respMsg.gate != nil {
				respMsg.gate.close()
			}
			respMsg.mu.Unlock()
		}()
		fh.doProcessSafely(reqCtx, router, exchange, respMsg)
	}()

	select {
	case <-streamStart:
		// 流式已启动（Flush 被调用），gate/drain 已接管，
		// handler 提前返回让 fasthttp 开始推送 SSE 数据。
		respMsg.beginStreaming()
	case <-procDone:
		// 处理在 Flush 之前完成（标记了 streaming 但实际未流式，如 stream=false），
		// 走与同步路径相同的收尾。gate 非 nil 说明收尾阶段才 Flush，
		// 此时 chunk 已在 gate 队列，交给 stream writer 写出，不能再用 ctx.Write
		// （stream writer 与 ctx.Write 同时存在会重复/丢失数据）。
		respMsg.mu.Lock()
		gate := respMsg.gate
		body := respMsg.body
		respMsg.mu.Unlock()
		if gate != nil {
			gate.close()
			return
		}
		if len(body) > 0 {
			ctx.Write(body)
		}
	}
}

// doProcessSafely 执行 DoProcess 并兜底处理 panic（两条路径共用）。
func (fh *FastHttp) doProcessSafely(reqCtx context.Context, router endpointApi.Router, exchange *endpointApi.Exchange, respMsg *ResponseMessage) {
	defer func() {
		if e := recover(); e != nil {
			fh.Printf("fasthttp process err :\n%v", runtime.Stack())
			respMsg.SetStatusCode(fasthttp.StatusInternalServerError)
		}
	}()
	fh.DoProcess(reqCtx, router, exchange)
}

// configIsStreaming 读取 from 配置的 streaming 标记。
// DSL JSON 反序列化为 bool；代码构造的配置可能传字符串。
func configIsStreaming(config map[string]interface{}) bool {
	switch v := config[endpointApi.ConfigKeyStreaming].(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(v, "true") || v == "1"
	default:
		return false
	}
}

// pathParamRegex :id 路径参数匹配，注册路由时使用
var pathParamRegex = regexp.MustCompile(`:([a-zA-Z_][a-zA-Z0-9_]*)`)

// convertPathParams 转换路径参数格式：将 :id 格式转换为 {id} 格式
func convertPathParams(path string) string {
	return pathParamRegex.ReplaceAllString(path, "{$1}")
}

func (fh *FastHttp) Printf(format string, v ...interface{}) {
	if fh.RuleConfig.Logger != nil {
		fh.RuleConfig.Logger.Printf(format, v...)
	}
}

// parseSize 解析大小字符串，支持K、M、G等单位
func parseSize(sizeStr string) (int, error) {
	if sizeStr == "" {
		return 4 * 1024 * 1024, nil // 默认4MB
	}

	sizeStr = strings.TrimSpace(strings.ToUpper(sizeStr))
	if sizeStr == "" {
		return 4 * 1024 * 1024, nil // 默认4MB
	}

	// 提取数字部分和单位部分
	var numStr string
	var unit string

	for i, r := range sizeStr {
		if r >= '0' && r <= '9' || r == '.' {
			numStr += string(r)
		} else {
			unit = strings.TrimSpace(sizeStr[i:])
			break
		}
	}

	if numStr == "" {
		return 0, fmt.Errorf("invalid size format: %s", sizeStr)
	}

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number in size: %s", numStr)
	}

	// 检查负数
	if num < 0 {
		return 0, fmt.Errorf("size cannot be negative: %s", sizeStr)
	}

	switch unit {
	case "", "B":
		return int(num), nil
	case "K", "KB":
		return int(num * 1024), nil
	case "M", "MB":
		return int(num * 1024 * 1024), nil
	case "G", "GB":
		return int(num * 1024 * 1024 * 1024), nil
	default:
		return 0, fmt.Errorf("unsupported size unit: %s", unit)
	}
}

// getTimeoutDuration 获取超时时间，如果为0则使用默认值
func getTimeoutDuration(seconds int, defaultSeconds int) time.Duration {
	if seconds <= 0 {
		return time.Duration(defaultSeconds) * time.Second
	}
	return time.Duration(seconds) * time.Second
}

// Started 返回服务是否已经启动
func (fh *FastHttp) Started() bool {
	return fh.started
}

// GetServer 获取FastHTTP服务
func (fh *FastHttp) GetServer() *fasthttp.Server {
	if fh.Server != nil {
		return fh.Server
	} else if fh.SharedNode.InstanceId != "" {
		if shared, err := fh.SharedNode.GetSafely(); err == nil {
			return shared.Server
		}
	}
	return nil
}

// transformMsg 包装函数，将RuleMsg转换函数转换为endpoint.Process
func transformMsg(transform func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg) endpointApi.Process {
	return func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		if exchange.In != nil && exchange.In.GetMsg() != nil {
			msg := *exchange.In.GetMsg()
			// 创建一个简单的RuleContext实现
			ruleCtx := &engine.DefaultRuleContext{}
			ruleCtx.SetContext(exchange.Context)
			newMsg := transform(ruleCtx, msg)
			exchange.In.SetMsg(&newMsg)
		}
		return true
	}
}

func (fh *FastHttp) newRouter() *router.Router {
	fh.router = router.New()
	//设置跨域
	if fh.Config.AllowCors {
		// 设置全局 OPTIONS 处理器
		fh.router.GlobalOPTIONS = func(ctx *fasthttp.RequestCtx) {
			// 设置 CORS 相关的响应头
			ctx.Response.Header.Set(HeaderKeyAccessControlAllowMethods, HeaderValueAll)
			ctx.Response.Header.Set(HeaderKeyAccessControlAllowHeaders, HeaderValueAll)
			ctx.Response.Header.Set(HeaderKeyAccessControlAllowOrigin, HeaderValueAll)
			// 返回 204 状态码
			ctx.Response.SetStatusCode(http.StatusNoContent)
		}

		// 设置自定义的 NotFound 处理器来捕获 OPTIONS 请求
		fh.router.NotFound = func(ctx *fasthttp.RequestCtx) {
			if string(ctx.Method()) == fasthttp.MethodOptions {
				// 对于 OPTIONS 请求，设置 CORS 头并返回
				ctx.Response.Header.Set(HeaderKeyAccessControlAllowMethods, HeaderValueAll)
				ctx.Response.Header.Set(HeaderKeyAccessControlAllowHeaders, HeaderValueAll)
				ctx.Response.Header.Set(HeaderKeyAccessControlAllowOrigin, HeaderValueAll)
				ctx.Response.SetStatusCode(http.StatusNoContent)
			} else {
				// 其他请求返回 404
				ctx.Response.SetStatusCode(http.StatusNotFound)
				ctx.Response.SetBodyString("Not Found")
			}
		}

		fh.AddInterceptors(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
			if respMsg, ok := exchange.Out.(*ResponseMessage); ok && respMsg.ctx != nil {
				respMsg.SetHeader(HeaderKeyAccessControlAllowOrigin, HeaderValueAll)
				respMsg.SetHeader(HeaderKeyAccessControlAllowMethods, HeaderValueAll)
				respMsg.SetHeader(HeaderKeyAccessControlAllowHeaders, HeaderValueAll)
			}
			return true
		})
	}
	return fh.router
}

func (fh *FastHttp) initServer() (*FastHttp, error) {
	if fh.router == nil {
		fh.newRouter()
	}
	return fh, nil
}

func (fh *FastHttp) startServer() error {
	if fh.started {
		return nil
	}
	var err error

	// 解析MaxRequestSize
	maxRequestSize, err := parseSize(fh.Config.MaxRequestSize)
	if err != nil {
		return fmt.Errorf("invalid MaxRequestSize: %v", err)
	}

	// 获取超时配置，0则使用默认值
	readTimeout := getTimeoutDuration(fh.Config.ReadTimeout, 10)   // 默认10秒
	writeTimeout := getTimeoutDuration(fh.Config.WriteTimeout, 10) // 默认10秒
	idleTimeout := getTimeoutDuration(fh.Config.IdleTimeout, 60)   // 默认60秒

	fh.Server = &fasthttp.Server{
		Handler:            fh.router.Handler,
		ReadTimeout:        readTimeout,
		WriteTimeout:       writeTimeout,
		IdleTimeout:        idleTimeout,
		MaxRequestBodySize: maxRequestSize,
		Concurrency:        fh.Config.Concurrency,
		DisableKeepalive:   fh.Config.DisableKeepalive,
		// 设置错误处理器，避免panic导致的goroutine泄漏
		ErrorHandler: func(ctx *fasthttp.RequestCtx, err error) {
			fh.Printf("fasthttp server error: %v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			ctx.SetBodyString("Internal Server Error")
		},
	}
	ln, err := fh.Listen()
	if err != nil {
		return err
	}
	//标记已经启动
	fh.started = true

	// 安全地访问Config字段和Server字段，防止数据竞争
	fh.RLock()
	isTls := fh.Config.CertKeyFile != "" && fh.Config.CertFile != ""
	certFile := fh.Config.CertFile
	certKeyFile := fh.Config.CertKeyFile
	serverAddr := fh.Config.Server
	server := fh.Server // 保存Server引用，防止在goroutine中访问时被其他goroutine修改
	onEvent := fh.OnEvent
	fh.RUnlock()

	if onEvent != nil {
		onEvent(endpointApi.EventInitServer, fh)
	}
	if isTls {
		fh.Printf("started fasthttp server with TLS on %s", serverAddr)
		go func() {
			defer ln.Close()
			err = server.ServeTLS(ln, certFile, certKeyFile)
			// 安全地访问OnEvent字段
			fh.RLock()
			onEvent := fh.OnEvent
			fh.RUnlock()
			if onEvent != nil {
				onEvent(endpointApi.EventCompletedServer, err)
			}
		}()
	} else {
		fh.Printf("started fasthttp server on %s", serverAddr)
		go func() {
			defer ln.Close()
			err = server.Serve(ln)
			// 安全地访问OnEvent字段
			fh.RLock()
			onEvent := fh.OnEvent
			fh.RUnlock()
			if onEvent != nil {
				onEvent(endpointApi.EventCompletedServer, err)
			}
		}()
	}
	return err
}
