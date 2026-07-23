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

// Type returns the component type
const Type = rest.Type

// Endpoint alias
type Endpoint = FastHttp

var _ endpointApi.Endpoint = (*Endpoint)(nil)
var _ endpointApi.HttpEndpoint = (*Endpoint)(nil)

// Register the component
// Above 300 concurrency, performance is three times higher than standard HTTP endpoint components
func init() {
	// You can use fastHTTP instead of the standard HTTP endpoint component
	// 1. Delete the standard HTTP endpoint component
	_ = endpoint.Registry.Unregister(Type)
	// 2. Register the fasthttp version of the HTTP endpoint component
	_ = endpoint.Registry.Register(&Endpoint{})
}

// RequestMessage fasthttp request message
type RequestMessage struct {
	ctx  *fasthttp.RequestCtx
	body []byte
	//Path parameters
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
			// When ctx is nil, the default value is used
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

// ResponseMessage fasthttp Response message
type ResponseMessage struct {
	ctx  *fasthttp.RequestCtx
	body []byte
	to   string
	msg  *types.RuleMsg
	err  error
	// Stream response writer, set by handler via SetBodyStreamWriter
	writer *bufio.Writer
	// cached headers in streaming mode to avoid race access to ctx.Response
	cachedHeaders textproto.MIMEHeader
}

// fasthttpResponseWriter adapter, set fasthttp.RequestCtx adapts to http.ResponseWriter
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
		return // The status code has already been written
	}
	w.status = statusCode
	w.ctx.SetStatusCode(statusCode)

	// Copy header information to the fasthttp context
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
	if r.writer != nil {
		return r.cachedHeaders
	}
	headers := make(textproto.MIMEHeader)
	r.ctx.Response.Header.VisitAll(func(key, value []byte) {
		headers.Add(string(key), string(value))
	})
	return headers
}

func (r *ResponseMessage) AddHeader(key, value string) {
	if r.ctx != nil && r.writer == nil {
		r.ctx.Response.Header.Add(key, value)
	}
}

func (r *ResponseMessage) SetHeader(key, value string) {
	if r.ctx != nil && r.writer == nil {
		r.ctx.Response.Header.Set(key, value)
	}
}

func (r *ResponseMessage) DelHeader(key string) {
	if r.ctx != nil && r.writer == nil {
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
	if r.ctx != nil && r.writer == nil {
		r.ctx.SetStatusCode(statusCode)
	}
}

func (r *ResponseMessage) SetBody(body []byte) {
	r.body = body
	if r.writer != nil {
		r.writer.Write(body)
	} else if r.ctx != nil {
		r.ctx.Write(body)
	}
}

func (r *ResponseMessage) SetError(err error) {
	r.err = err
}

func (r *ResponseMessage) GetError() error {
	return r.err
}

// Flush pushes buffered data in real time to the client for SSE stream response.
func (r *ResponseMessage) Flush() {
	if r.writer != nil {
		r.writer.Flush()
	}
}

func (r *ResponseMessage) RequestCtx() *fasthttp.RequestCtx {
	return r.ctx
}

// Config FastHttp service configuration
type Config struct {
	Server      string `json:"server" label:"Server" desc:"Listen address, format host:port or :port, e.g. :8080" required:"true" ref:"primary"`   //Server address
	CertFile    string `json:"certFile" label:"Cert File" desc:"TLS certificate file path; provide together with certKeyFile to enable HTTPS"`     //Certificate documents
	CertKeyFile string `json:"certKeyFile" label:"Cert Key File" desc:"TLS private key file path; provide together with certFile to enable HTTPS"` //Key file
	//Whether cross-domain is allowed
	AllowCors bool `json:"allowCors" label:"Allow CORS" desc:"Whether to allow cross-origin requests"`
	// FastHTTP server configuration
	ReadTimeout      int    `json:"readTimeout" label:"Read Timeout (s)" desc:"Read timeout in seconds; 0 uses default 10"`                       // Read timeout time (seconds), 0 uses the default value of 10 seconds
	WriteTimeout     int    `json:"writeTimeout" label:"Write Timeout (s)" desc:"Write timeout in seconds; 0 uses default 10"`                    // Write timeout time (seconds), 0 uses the default value of 10 seconds
	IdleTimeout      int    `json:"idleTimeout" label:"Idle Timeout (s)" desc:"Idle timeout in seconds; 0 uses default 60"`                       // Idle timeout time (seconds): 0 uses the default value of 60 seconds
	DisableKeepalive bool   `json:"disableKeepalive" label:"Disable Keepalive" desc:"Whether to disable keep-alive"`                              //  Disable keepalive
	MaxRequestSize   string `json:"maxRequestSize" label:"Max Request Size" desc:"Max request body size, supports 4M/4m/10K formats; default 4M"` // Maximum request body size, supports formats such as 4M, 4M, 10K, default is 4M
	Concurrency      int    `json:"concurrency" label:"Concurrency" desc:"Concurrency; 0 uses default 256 * 1024"`                                // Concurrent count, 0 uses the default value 256 * 1024
	//New configuration items are added to control connections and resource management
	//MaxConnsPerIP int `json:"maxConnsPerIP"` // Maximum number of connections per IP
	//MaxRequestsPerConn int `json:"maxRequestsPerConn"` // Maximum number of requests per connection
	//MaxKeepaliveDuration time.Duration `json:"maxKeepaliveDuration"` // keepalive maximum duration
	//ReadBufferSize int `json:"readBufferSize"` // Read buffer size
	//WriteBufferSize int `json:"writeBufferSize"` // Write buffer size
	//ReduceMemoryUsage bool `json:"reduceMemoryUsage"` // Reduces memory usage
	//StreamRequestBody bool `json:"streamRequestBody"` // Stream the request body
}

// FastHttp receive endpoint
type FastHttp struct {
	impl.BaseEndpoint
	nodeBase.SharedNode[*FastHttp]
	//Configuration
	Config     Config
	RuleConfig types.Config
	Server     *fasthttp.Server
	//HTTP router
	router  *router.Router
	started bool
	// resourceMapping is the resource mapping for static file serving
	resourceMapping string
}

// Type returns the component type
func (fh *FastHttp) Type() string {
	return Type
}

func (fh *FastHttp) New() types.Node {
	return &FastHttp{
		Config: Config{
			Server:         ":6333",
			ReadTimeout:    10,    // 0 uses the default value for 10 seconds
			WriteTimeout:   10,    // 0 uses the default value for 10 seconds
			IdleTimeout:    60,    // 0 uses the default value for 60 seconds
			MaxRequestSize: "4M",  // Default is 4MB
			Concurrency:    10000, // and issued several times simultaneously
			//DisableKeepalive: true, // Disable keepalive
		},
	}
}

// Init initializes the component
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

// Destroy releases resources
func (fh *FastHttp) Destroy() {
	_ = fh.Close()
}

func (fh *FastHttp) Restart() error {
	// Use a unified closing method
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

// shutdownServer uses a unified shutdown logic
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

	// Wait a short while to ensure the port is fully released
	time.Sleep(100 * time.Millisecond)
}

func (fh *FastHttp) Close() error {
	// Use a unified closing method
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
			//Restart the shared service
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

// addRouter registers one or more routes
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
		//Store the route
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
			// Convert path parameter format: Convert:id format to {id} format
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
		// Create a standard http.ResponseWriter and *http.Request adapter
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

		// Copy the request header
		ctx.Request.Header.VisitAll(func(key, value []byte) {
			req.Header.Add(string(key), string(value))
		})

		// Create a response writer adapter
		w := &fasthttpResponseWriter{
			ctx:    ctx,
			header: req.Response.Header,
		}

		// Calling the original http.Handler
		handler.ServeHTTP(w, req)
	}
	return fh
}

// LoadServeFiles loads the static file mapping
// resourceMapping format: "urlPath1=localDir1,urlPath2=localDir2"
// For example: "/static/*filepath=./static,/assets/*filepath=./assets"
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

			// Remove the /*filepath suffix to get the base path
			basePath := urlPath
			if strings.HasSuffix(urlPath, "/*filepath") {
				basePath = urlPath[:len(urlPath)-10]
			}

			// Make sure the path ends with /{filepath:*}, which is a requirement for the fastHTTP router
			if !strings.HasSuffix(urlPath, "/{filepath:*}") {
				if strings.HasSuffix(basePath, "/") {
					urlPath = basePath + "{filepath:*}"
				} else {
					urlPath = basePath + "/{filepath:*}"
				}
			}

			// Use the router's ServeFiles method
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
			//Capture anomalies
			if e := recover(); e != nil {
				fh.Printf("fasthttp endpointApi handler err :\n%v", runtime.Stack())
				// Set error responses
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

		// Extract path parameters
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

		//Place the url? parameter into the msg metadata
		ctx.QueryArgs().VisitAll(func(key, value []byte) {
			metadata.PutValue(string(key), string(value))
		})

		// CORS headers must be set before SetBodyStreamWriter,
		// Because HTTP headers are sent before bodyStream callbacks are executed
		if fh.Config.AllowCors {
			ctx.Response.Header.Set(HeaderKeyAccessControlAllowOrigin, HeaderValueAll)
			ctx.Response.Header.Set(HeaderKeyAccessControlAllowMethods, HeaderValueAll)
			ctx.Response.Header.Set(HeaderKeyAccessControlAllowHeaders, HeaderValueAll)
		}

		// Cache headers to avoid bodyStream callback inbound race access ctx.Response
		if resp, ok := exchange.Out.(*ResponseMessage); ok && resp.ctx != nil {
			headers := make(textproto.MIMEHeader)
			resp.ctx.Response.Header.VisitAll(func(key, value []byte) {
				headers.Set(string(key), string(value))
			})
			resp.cachedHeaders = headers
		}

		ctx.SetBodyStreamWriter(func(w *bufio.Writer) {
			if resp, ok := exchange.Out.(*ResponseMessage); ok {
				resp.writer = w
			}
			var reqCtx context.Context
			var cancel context.CancelFunc
			if isWait {
				reqCtx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
			} else {
				reqCtx = context.Background()
			}

			exchange.Context = reqCtx
			fh.DoProcess(reqCtx, router, exchange)
		})
	}
}

// convertPathParams Convert path parameter format: Convert:id format to {id} format
func convertPathParams(path string) string {
	// Use regular expressions to match:parametername_format and convert to {parameter_name}
	re := regexp.MustCompile(`:([a-zA-Z_][a-zA-Z0-9_]*)`)
	return re.ReplaceAllString(path, "{$1}")
}

func (fh *FastHttp) Printf(format string, v ...interface{}) {
	if fh.RuleConfig.Logger != nil {
		fh.RuleConfig.Logger.Printf(format, v...)
	}
}

// parseSize parses strings of size and size, supports units such as K, M, G
func parseSize(sizeStr string) (int, error) {
	if sizeStr == "" {
		return 4 * 1024 * 1024, nil // Default is 4MB
	}

	sizeStr = strings.TrimSpace(strings.ToUpper(sizeStr))
	if sizeStr == "" {
		return 4 * 1024 * 1024, nil // Default is 4MB
	}

	// Extract the numeric and unit parts
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

	// Check the negative numbers
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

// getTimeoutDuration gets the timeout; if it is 0, the default value is used
func getTimeoutDuration(seconds int, defaultSeconds int) time.Duration {
	if seconds <= 0 {
		return time.Duration(defaultSeconds) * time.Second
	}
	return time.Duration(seconds) * time.Second
}

// Started returns whether the service has started
func (fh *FastHttp) Started() bool {
	return fh.started
}

// GetServer obtains the FastHTTP service
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

// transformMsg wrapper function, converting the RuleMsg transformation function to endpoint.Process
func transformMsg(transform func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg) endpointApi.Process {
	return func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		if exchange.In != nil && exchange.In.GetMsg() != nil {
			msg := *exchange.In.GetMsg()
			// Create a simple RuleContext implementation
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
	//Set up cross-domain
	if fh.Config.AllowCors {
		// Set the global OPTIONS processor
		fh.router.GlobalOPTIONS = func(ctx *fasthttp.RequestCtx) {
			// Set CORS-related response headers
			ctx.Response.Header.Set(HeaderKeyAccessControlAllowMethods, HeaderValueAll)
			ctx.Response.Header.Set(HeaderKeyAccessControlAllowHeaders, HeaderValueAll)
			ctx.Response.Header.Set(HeaderKeyAccessControlAllowOrigin, HeaderValueAll)
			// Return the 204 status code
			ctx.Response.SetStatusCode(http.StatusNoContent)
		}

		// Set up a custom NotFound processor to capture OPTIONS requests
		fh.router.NotFound = func(ctx *fasthttp.RequestCtx) {
			if string(ctx.Method()) == fasthttp.MethodOptions {
				// For OPTIONS requests, set the CORS header and return
				ctx.Response.Header.Set(HeaderKeyAccessControlAllowMethods, HeaderValueAll)
				ctx.Response.Header.Set(HeaderKeyAccessControlAllowHeaders, HeaderValueAll)
				ctx.Response.Header.Set(HeaderKeyAccessControlAllowOrigin, HeaderValueAll)
				ctx.Response.SetStatusCode(http.StatusNoContent)
			} else {
				// Other requests return 404
				ctx.Response.SetStatusCode(http.StatusNotFound)
				ctx.Response.SetBodyString("Not Found")
			}
		}

		fh.AddInterceptors(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
			if respMsg, ok := exchange.Out.(*ResponseMessage); ok && respMsg.ctx != nil && respMsg.writer == nil {
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

	// Parse MaxRequestSize
	maxRequestSize, err := parseSize(fh.Config.MaxRequestSize)
	if err != nil {
		return fmt.Errorf("invalid MaxRequestSize: %v", err)
	}

	// Get the timeout configuration; 0 uses the default value
	readTimeout := getTimeoutDuration(fh.Config.ReadTimeout, 10)   // Default is 10 seconds
	writeTimeout := getTimeoutDuration(fh.Config.WriteTimeout, 10) // Default is 10 seconds
	idleTimeout := getTimeoutDuration(fh.Config.IdleTimeout, 60)   // Default is 60 seconds

	fh.Server = &fasthttp.Server{
		Handler:            fh.router.Handler,
		ReadTimeout:        readTimeout,
		WriteTimeout:       writeTimeout,
		IdleTimeout:        idleTimeout,
		MaxRequestBodySize: maxRequestSize,
		Concurrency:        fh.Config.Concurrency,
		DisableKeepalive:   fh.Config.DisableKeepalive,
		// Set the wrong processor to avoid Goroutine leaks caused by panic
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
	//The marker has already been activated
	fh.started = true

	// Securely access the Config and Server fields to prevent data contention
	fh.RLock()
	isTls := fh.Config.CertKeyFile != "" && fh.Config.CertFile != ""
	certFile := fh.Config.CertFile
	certKeyFile := fh.Config.CertKeyFile
	serverAddr := fh.Config.Server
	server := fh.Server // Save Server references to prevent modifications by other goroutines when accessing in goroutine
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
			// Securely access the OnEvent field
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
			// Securely access the OnEvent field
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
