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

// Package fasthttp provides a WebSocket endpoint implementation for the RuleGo framework using fasthttp.
// It allows creating WebSocket servers that can receive and process incoming WebSocket messages,
// routing them to appropriate rule chains or components for further processing.
//
// Key components in this package include:
// - WebsocketEndpoint: Implements the WebSocket server and message handling using fasthttp
// - WebsocketRequestMessage: Represents an incoming WebSocket message
// - WebsocketResponseMessage: Represents the WebSocket message to be sent back
//
// The WebSocket endpoint supports dynamic routing configuration, allowing users to
// define message patterns and their corresponding rule chain or component destinations.
// It also provides flexibility in handling different WebSocket message types and formats.
//
// This package integrates with the broader RuleGo ecosystem, enabling seamless
// data flow from WebSocket messages to rule processing and back to WebSocket responses.
package fasthttp

import (
	"context"
	"errors"
	"fmt"

	"github.com/fasthttp/websocket"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	nodeBase "github.com/rulego/rulego/components/base"
	websocketEndpoint "github.com/rulego/rulego/endpoint/websocket"

	"net/textproto"
	"strconv"
	"strings"
	"sync"

	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
	"github.com/valyala/fasthttp"
)

// WebsocketType component type
const WebsocketType = websocketEndpoint.Type

// Also known as WebsocketEndpoint
type WebsocketEndpoint = FastHttpWebsocket

var _ endpointApi.Endpoint = (*WebsocketEndpoint)(nil)

func init() {
	// FastHTTP can be used instead of the standard WebSocket endpoint component
	// 1. Delete the standard WebSocket endpoint component
	_ = endpoint.Registry.Unregister(WebsocketType)
	// 2. Register the fasthttp version of the WebSocket endpoint component
	_ = endpoint.Registry.Register(&WebsocketEndpoint{})
}

// WebsocketRequestMessage fasthttp websocket request message
type WebsocketRequestMessage struct {
	//ws message type TextMessage=1 / BinaryMessage=2
	messageType int
	ctx         *fasthttp.RequestCtx
	body        []byte
	//Path parameters
	Params   map[string]string
	msg      *types.RuleMsg
	err      error
	Metadata *types.Metadata
}

func (r *WebsocketRequestMessage) Body() []byte {
	return r.body
}

func (r *WebsocketRequestMessage) Headers() textproto.MIMEHeader {
	if r.ctx == nil {
		return nil
	}
	headers := make(textproto.MIMEHeader)
	r.ctx.Request.Header.VisitAll(func(key, value []byte) {
		headers.Add(string(key), string(value))
	})
	return headers
}

func (r *WebsocketRequestMessage) From() string {
	if r.ctx == nil {
		return ""
	}
	return string(r.ctx.RequestURI())
}

func (r *WebsocketRequestMessage) GetParam(key string) string {
	if r.ctx == nil {
		return ""
	}
	if v, ok := r.Params[key]; ok {
		return v
	}
	return string(r.ctx.QueryArgs().Peek(key))
}

func (r *WebsocketRequestMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

func (r *WebsocketRequestMessage) GetMsg() *types.RuleMsg {
	if r.msg == nil {
		//The default specification is JSON format. If it is not this type, please modify it in the process function
		dataType := types.JSON
		if r.messageType == websocket.BinaryMessage {
			dataType = types.BINARY
		}

		ruleMsg := types.NewMsg(0, r.From(), dataType, types.NewMetadata(), string(r.Body()))
		r.msg = &ruleMsg
	}
	return r.msg
}

func (r *WebsocketRequestMessage) SetStatusCode(statusCode int) {
}

func (r *WebsocketRequestMessage) SetBody(body []byte) {
	r.body = body
}

func (r *WebsocketRequestMessage) SetError(err error) {
	r.err = err
}

func (r *WebsocketRequestMessage) GetError() error {
	return r.err
}

func (r *WebsocketRequestMessage) RequestCtx() *fasthttp.RequestCtx {
	return r.ctx
}

func (r *WebsocketRequestMessage) GetMetadata() *types.Metadata {
	if r.Metadata == nil {
		r.Metadata = types.NewMetadata()
	}
	return r.Metadata
}

// WebsocketResponseMessage fasthttp Websocket response message
type WebsocketResponseMessage struct {
	headers textproto.MIMEHeader
	//ws message type: TextMessage/BinaryMessage
	messageType int
	log         func(format string, v ...interface{})
	ctx         *fasthttp.RequestCtx
	conn        *websocket.Conn
	body        []byte
	to          string
	msg         *types.RuleMsg
	err         error
	locker      sync.RWMutex
}

func (r *WebsocketResponseMessage) Body() []byte {
	return r.body
}

func (r *WebsocketResponseMessage) Headers() textproto.MIMEHeader {
	if r.headers == nil {
		r.headers = make(map[string][]string)
	}
	return r.headers
}

func (r *WebsocketResponseMessage) From() string {
	if r.ctx == nil {
		return ""
	}
	return string(r.ctx.RequestURI())
}

func (r *WebsocketResponseMessage) GetParam(key string) string {
	if r.ctx == nil {
		return ""
	}
	return string(r.ctx.QueryArgs().Peek(key))
}

func (r *WebsocketResponseMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

func (r *WebsocketResponseMessage) GetMsg() *types.RuleMsg {
	return r.msg
}

func (r *WebsocketResponseMessage) GetMetadata() *types.Metadata {
	if msg := r.GetMsg(); msg != nil {
		return msg.GetMetadata()
	}
	return nil
}

// SetStatusCode does not provide a status code
func (r *WebsocketResponseMessage) SetStatusCode(statusCode int) {
}

func (r *WebsocketResponseMessage) SetBody(body []byte) {
	r.body = body
	if r.conn != nil {
		if r.messageType == 0 {
			r.messageType = websocket.TextMessage
		}
		// Lock before writing
		r.locker.Lock()
		defer r.locker.Unlock()

		if err := r.conn.WriteMessage(r.messageType, body); err != nil {
			r.SetError(err)
		}
	}
}

func (r *WebsocketResponseMessage) SetError(err error) {
	r.err = err
}

func (r *WebsocketResponseMessage) GetError() error {
	return r.err
}

func (r *WebsocketResponseMessage) RequestCtx() *fasthttp.RequestCtx {
	return r.ctx
}

// WebsocketConfig FastHttp Websocket service configuration
type WebsocketConfig = Config

// FastHttpWebsocket receives endpoints
type FastHttpWebsocket struct {
	impl.BaseEndpoint
	nodeBase.SharedNode[*FastHttpWebsocket]
	//Configuration
	Config     WebsocketConfig
	RuleConfig types.Config
	//FastHTTP server
	fastHttpEndpoint *FastHttp
	//WebSocket Upgrader
	Upgrader websocket.FastHTTPUpgrader
	started  bool
}

// Type returns the component type
func (ws *FastHttpWebsocket) Type() string {
	return WebsocketType
}

func (ws *FastHttpWebsocket) New() types.Node {
	return &FastHttpWebsocket{
		Config: WebsocketConfig{
			Server:         ":6333",
			ReadTimeout:    10,    // 0 uses the default value for 10 seconds
			WriteTimeout:   10,    // 0 uses the default value for 10 seconds
			IdleTimeout:    60,    // 0 uses the default value for 60 seconds
			MaxRequestSize: "4M",  // Default is 4MB
			Concurrency:    10000, // and issued several times simultaneously
			AllowCors:      true,
		},
	}
}

func (ws *FastHttpWebsocket) Def() types.ComponentForm {
	return types.ComponentForm{
		Desc: "FastHTTP WebSocket server endpoint for receiving and processing WebSocket messages",
		RouterForm: &types.RouterForm{
			From: &types.RouterFormField{
				Path: types.ComponentFormField{
					Name:     "path",
					Type:     "string",
					Label:    "Path",
					Desc:     "WebSocket request path, e.g. /api/ws",
					Required: true,
				},
			},
		},
	}
}

// Init initializes the component
func (ws *FastHttpWebsocket) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &ws.Config)
	if err != nil {
		return err
	}
	ws.RuleConfig = ruleConfig

	// Initialize the fastHTTP endpoint
	ws.fastHttpEndpoint = &FastHttp{}
	if err = ws.fastHttpEndpoint.Init(ruleConfig, configuration); err != nil {
		return err
	}

	// Configure the WebSocket Upgrader
	ws.Upgrader.CheckOrigin = func(ctx *fasthttp.RequestCtx) bool {
		return ws.Config.AllowCors // All cross-origin requests are allowed
	}

	return ws.SharedNode.InitWithClose(ws.RuleConfig, ws.Type(), ws.Config.Server, false, func() (*FastHttpWebsocket, error) {
		return ws.initServer()
	}, func(server *FastHttpWebsocket) error {
		if server != nil {
			return server.Close()
		}
		return nil
	})
}

func (ws *FastHttpWebsocket) initServer() (*FastHttpWebsocket, error) {
	return ws, nil
}

// Destroy releases resources
func (ws *FastHttpWebsocket) Destroy() {
	_ = ws.Close()
}

func (ws *FastHttpWebsocket) Restart() error {
	if ws.fastHttpEndpoint != nil {
		if err := ws.fastHttpEndpoint.Restart(); err != nil {
			return err
		}
	}

	if ws.SharedNode.InstanceId != "" {
		if shared, err := ws.SharedNode.GetSafely(); err == nil {
			return shared.Restart()
		} else {
			return err
		}
	}

	var oldRouter = make(map[string]endpointApi.Router)

	ws.Lock()
	for id, router := range ws.RouterStorage {
		if !router.IsDisable() {
			oldRouter[id] = router
		}
	}
	ws.RouterStorage = make(map[string]endpointApi.Router)
	ws.Unlock()
	ws.started = false

	if err := ws.Start(); err != nil {
		return err
	}
	for _, router := range oldRouter {
		if len(router.GetParams()) == 0 {
			router.SetParams("GET")
		}
		if !ws.HasRouter(router.GetId()) {
			if _, err := ws.AddRouter(router, router.GetParams()...); err != nil {
				ws.Printf("fasthttp websocket add router path:=%s error:%v", router.FromToString(), err)
				continue
			}
		}
	}
	return nil
}

func (ws *FastHttpWebsocket) Close() error {
	if ws.fastHttpEndpoint != nil {
		if err := ws.fastHttpEndpoint.Close(); err != nil {
			return err
		}
	}

	if ws.SharedNode.InstanceId != "" {
		if shared, err := ws.SharedNode.GetSafely(); err == nil {
			ws.RLock()
			defer ws.RUnlock()
			for key := range ws.RouterStorage {
				shared.deleteRouter(key)
			}
			//Restart the shared service
			return shared.Restart()
		}
	}

	ws.started = false
	ws.BaseEndpoint.Destroy()
	return nil
}

func (ws *FastHttpWebsocket) Id() string {
	return ws.Config.Server
}

func (ws *FastHttpWebsocket) AddRouter(router endpointApi.Router, params ...interface{}) (id string, err error) {
	if router == nil {
		return "", errors.New("router can not nil")
	} else {
		defer func() {
			if e := recover(); e != nil {
				err = fmt.Errorf("addRouter err :%v", e)
			}
		}()
		ws.addRouter(router)
		return router.GetId(), err
	}
}

func (ws *FastHttpWebsocket) RemoveRouter(routerId string, params ...interface{}) error {
	routerId = strings.TrimSpace(routerId)
	ws.Lock()
	defer ws.Unlock()
	if ws.RouterStorage != nil {
		if router, ok := ws.RouterStorage[routerId]; ok && !router.IsDisable() {
			router.Disable(true)
			return nil
		} else {
			return fmt.Errorf("router: %s not found", routerId)
		}
	}
	return nil
}

func (ws *FastHttpWebsocket) Printf(format string, v ...interface{}) {
	if ws.RuleConfig.Logger != nil {
		ws.RuleConfig.Logger.Printf(format, v...)
	}
}

func (ws *FastHttpWebsocket) Start() error {
	if ws.OnEvent != nil {
		ws.OnEvent(endpointApi.EventInitServer, ws.fastHttpEndpoint)
	}

	if ws.started {
		return nil
	}

	// Start the fasthttp server
	if err := ws.fastHttpEndpoint.Start(); err != nil {
		return err
	}

	ws.started = true
	return nil
}

// addRouter registers one or more routes
func (ws *FastHttpWebsocket) addRouter(routers ...endpointApi.Router) *FastHttpWebsocket {
	ws.Lock()
	defer ws.Unlock()

	if ws.RouterStorage == nil {
		ws.RouterStorage = make(map[string]endpointApi.Router)
	}
	for _, item := range routers {
		item.SetParams("GET")
		ws.CheckAndSetRouterId(item)
		//Store the route
		ws.RouterStorage[item.GetId()] = item
		// Convert path parameter format: Convert:id format to {id} format
		path := convertPathParams(item.FromToString())
		//Add to the fasthttp router
		ws.fastHttpEndpoint.Router().GET(path, ws.handler(item))
	}

	return ws
}

func (ws *FastHttpWebsocket) deleteRouter(routerId string) {
	ws.Lock()
	defer ws.Unlock()
	if ws.RouterStorage != nil {
		delete(ws.RouterStorage, routerId)
	}
}

func (ws *FastHttpWebsocket) handler(router endpointApi.Router) func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		if router.IsDisable() {
			ctx.SetStatusCode(fasthttp.StatusNotFound)
			return
		}

		// Upgrade to WebSocket connection
		err := ws.Upgrader.Upgrade(ctx, func(conn *websocket.Conn) {
			// Parse path parameters
			params := ws.parseParams(router.FromToString(), string(ctx.Path()))

			connectExchange := &endpointApi.Exchange{
				In: &WebsocketRequestMessage{
					ctx:    ctx,
					Params: params,
					body:   nil,
				},
				Out: &WebsocketResponseMessage{
					log: func(format string, v ...interface{}) {
						ws.Printf(format, v...)
					},
					ctx:  ctx,
					conn: conn,
				},
			}

			// Call OnEvent safely
			ws.RLock()
			onEvent := ws.OnEvent
			ws.RUnlock()
			if onEvent != nil {
				onEvent(endpointApi.EventConnect, connectExchange)
			}

			defer func() {
				_ = conn.Close()
				//Capture anomalies
				if e := recover(); e != nil {
					// Call OnEvent safely
					ws.RLock()
					onEvent := ws.OnEvent
					ws.RUnlock()
					if onEvent != nil {
						onEvent(endpointApi.EventDisconnect, connectExchange)
					}
					ws.Printf("fasthttp websocket endpoint handler err :\n%v", runtime.Stack())
				}
			}()

			for {
				mt, message, err := conn.ReadMessage()
				if err != nil {
					// Call OnEvent safely
					ws.RLock()
					onEvent := ws.OnEvent
					ws.RUnlock()
					if onEvent != nil {
						onEvent(endpointApi.EventDisconnect, connectExchange, ctx)
					}
					break
				}

				if router.IsDisable() {
					// Call OnEvent safely
					ws.RLock()
					onEvent := ws.OnEvent
					ws.RUnlock()
					if onEvent != nil {
						onEvent(endpointApi.EventDisconnect, connectExchange, ctx)
					}
					ctx.SetStatusCode(fasthttp.StatusNotFound)
					break
				}
				if mt != websocket.BinaryMessage && mt != websocket.TextMessage {
					continue
				}

				exchange := &endpointApi.Exchange{
					In: &WebsocketRequestMessage{
						ctx:         ctx,
						Params:      params,
						body:        message,
						messageType: mt,
					},
					Out: &WebsocketResponseMessage{
						log: func(format string, v ...interface{}) {
							ws.Printf(format, v...)
						},
						ctx:         ctx,
						conn:        conn,
						messageType: mt,
					},
				}

				msg := exchange.In.GetMsg()
				//Put the path parameter into the msg metadata
				for key, value := range params {
					msg.Metadata.PutValue(key, value)
				}

				msg.Metadata.PutValue("messageType", strconv.Itoa(mt))

				//Place the url? parameter into the msg metadata
				ctx.QueryArgs().VisitAll(func(key, value []byte) {
					msg.Metadata.PutValue(string(key), string(value))
				})

				ws.DoProcess(context.Background(), router, exchange)
			}
		})

		if err != nil {
			ws.Printf("FastHttp Websocket handler upgrade error: %v", err)
			return
		}
	}
}

// parseParams parses path parameters
func (ws *FastHttpWebsocket) parseParams(pattern, path string) map[string]string {
	params := make(map[string]string)

	// Simple path parameter parsing, supports:p aram format
	patternParts := strings.Split(pattern, "/")
	pathParts := strings.Split(path, "/")

	if len(patternParts) != len(pathParts) {
		return params
	}

	for i, part := range patternParts {
		if strings.HasPrefix(part, ":") {
			key := part[1:] // Remove the prefix
			if i < len(pathParts) {
				params[key] = pathParts[i]
			}
		}
	}

	return params
}
