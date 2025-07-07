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
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/textproto"
	"regexp"
	"strconv"
	"strings"
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
	//路径参数
	Params   map[string]string
	msg      *types.RuleMsg
	err      error
	Metadata *types.Metadata
}

func (r *RequestMessage) Body() []byte {
	if r.body == nil && r.ctx != nil {
		r.body = r.ctx.PostBody()
	}
	return r.body
}

func (r *RequestMessage) Headers() textproto.MIMEHeader {
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
	if r.ctx != nil {
		r.ctx.Request.Header.Add(key, value)
	}
}

func (r *RequestMessage) SetHeader(key, value string) {
	if r.ctx != nil {
		r.ctx.Request.Header.Set(key, value)
	}
}

func (r *RequestMessage) DelHeader(key string) {
	if r.ctx != nil {
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
	if r.ctx == nil {
		return ""
	}
	return string(r.ctx.RequestURI())
}

func (r *RequestMessage) GetParam(key string) string {
	if r.ctx == nil {
		return ""
	}
	if v, ok := r.Params[key]; ok {
		return v
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
		if r.ctx != nil {
			if string(r.ctx.Method()) == fasthttp.MethodGet {
				dataType = types.JSON
				queryArgs := make(map[string]interface{})
				r.ctx.QueryArgs().VisitAll(func(key, value []byte) {
					queryArgs[string(key)] = string(value)
				})
				data = str.ToString(queryArgs)
			} else {
				if contentType := string(r.ctx.Request.Header.Peek(ContentTypeKey)); strings.HasPrefix(contentType, JsonContextType) {
					dataType = types.JSON
				}
				data = string(r.Body())
			}
		} else {
			// 当ctx为nil时，使用默认值
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

func (r *ResponseMessage) Body() []byte {
	return r.body
}

func (r *ResponseMessage) Headers() textproto.MIMEHeader {
	if r.ctx == nil {
		return nil
	}
	headers := make(textproto.MIMEHeader)
	r.ctx.Response.Header.VisitAll(func(key, value []byte) {
		headers.Add(string(key), string(value))
	})
	return headers
}

func (r *ResponseMessage) AddHeader(key, value string) {
	if r.ctx != nil {
		r.ctx.Response.Header.Add(key, value)
	}
}

func (r *ResponseMessage) SetHeader(key, value string) {
	if r.ctx != nil {
		r.ctx.Response.Header.Set(key, value)
	}
}

func (r *ResponseMessage) DelHeader(key string) {
	if r.ctx != nil {
		r.ctx.Response.Header.Del(key)
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
	if r.ctx != nil {
		r.ctx.SetStatusCode(statusCode)
	}
}

func (r *ResponseMessage) SetBody(body []byte) {
	r.body = body
	if r.ctx != nil {
		r.ctx.SetBody(body)
	}
}

func (r *ResponseMessage) SetError(err error) {
	r.err = err
}

func (r *ResponseMessage) GetError() error {
	return r.err
}

func (r *ResponseMessage) RequestCtx() *fasthttp.RequestCtx {
	return r.ctx
}

// Config FastHttp 服务配置
type Config struct {
	Server      string `json:"server"`      //服务器地址
	CertFile    string `json:"certFile"`    //证书文件
	CertKeyFile string `json:"certKeyFile"` //密钥文件
	//是否允许跨域
	AllowCors bool `json:"allowCors"`
	// FastHTTP服务器配置
	ReadTimeout      int    `json:"readTimeout"`      // 读取超时时间（秒），0使用默认值10秒
	WriteTimeout     int    `json:"writeTimeout"`     // 写入超时时间（秒），0使用默认值10秒
	IdleTimeout      int    `json:"idleTimeout"`      // 空闲超时时间（秒），0使用默认值60秒
	DisableKeepalive bool   `json:"disableKeepalive"` //  禁用keepalive
	MaxRequestSize   string `json:"maxRequestSize"`   // 最大请求体大小，支持4M、4m、10K等格式，默认4M
	Concurrency      int    `json:"concurrency"`      // 并发数，0使用默认值 256 * 1024
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
			if from := item.GetFrom(); from != nil {
				if to := from.GetTo(); to != nil {
					isWait = to.IsWait()
				}
			}
			// 转换路径参数格式：将 :id 格式转换为 {id} 格式
			path = convertPathParams(path)
			fh.router.Handle(method, path, fh.handler(item, isWait))
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
			header: req.Response.Header,
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

func (fh *FastHttp) handler(router endpointApi.Router, isWait bool) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		defer func() {
			//捕捉异常
			if e := recover(); e != nil {
				fh.Printf("fasthttp endpointApi handler err :\n%v", runtime.Stack())
				// 设置错误响应
				ctx.SetStatusCode(fasthttp.StatusInternalServerError)
				ctx.SetBodyString("Internal Server Error")
			}
		}()
		if router.IsDisable() {
			ctx.SetStatusCode(fasthttp.StatusNotFound)
			return
		}
		metadata := types.NewMetadata()
		params := make(map[string]string)

		// 提取路径参数
		ctx.VisitUserValues(func(key []byte, value interface{}) {
			if v, ok := value.(string); ok {
				params[string(key)] = v
				metadata.PutValue(string(key), v)
			}
		})

		exchange := &endpointApi.Exchange{
			In: &RequestMessage{
				ctx:      ctx,
				Params:   params,
				Metadata: metadata,
			},
			Out: &ResponseMessage{
				ctx: ctx,
			},
		}

		//把url?参数放到msg元数据中
		ctx.QueryArgs().VisitAll(func(key, value []byte) {
			metadata.PutValue(string(key), string(value))
		})
		// 创建带超时的context，防止goroutine泄漏
		var reqCtx context.Context
		var cancel context.CancelFunc
		//异步不是设置超时，让引擎控制
		if isWait {
			// 同步处理时也使用带超时的context
			reqCtx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
			// 确保context被取消，防止goroutine泄漏
			defer cancel()
		} else {
			reqCtx = context.Background()
		}

		// 设置context到exchange中
		exchange.Context = reqCtx

		fh.DoProcess(reqCtx, router, exchange)
	}
}

// convertPathParams 转换路径参数格式：将 :id 格式转换为 {id} 格式
func convertPathParams(path string) string {
	// 使用正则表达式匹配 :参数名 格式并转换为 {参数名} 格式
	re := regexp.MustCompile(`:([a-zA-Z_][a-zA-Z0-9_]*)`)
	return re.ReplaceAllString(path, "{$1}")
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
				respMsg.ctx.Response.Header.Set(HeaderKeyAccessControlAllowOrigin, HeaderValueAll)
				respMsg.ctx.Response.Header.Set(HeaderKeyAccessControlAllowMethods, HeaderValueAll)
				respMsg.ctx.Response.Header.Set(HeaderKeyAccessControlAllowHeaders, HeaderValueAll)
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
