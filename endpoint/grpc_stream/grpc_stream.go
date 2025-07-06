package grpcstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/textproto"
	"strings"
	"time"

	"github.com/rulego/rulego/components/base"

	"github.com/fullstorydev/grpcurl"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const Type = types.EndpointTypePrefix + "grpc/stream"

type Endpoint = GrpcStream

var _ endpointApi.Endpoint = (*Endpoint)(nil)

func init() {
	_ = endpoint.Registry.Register(&Endpoint{})
}

// Config gRPC流配置
type Config struct {
	Server        string            // gRPC服务器地址
	Service       string            // gRPC服务名称
	Method        string            // gRPC方法名称
	Headers       map[string]string // 请求头
	Request       string            // 请求数据(如果为nil，发送空数据，看服务端的处理逻辑)
	CheckInterval int               // gRPC服务器检查间隔(ms)
}

// GrpcStream 提供了基于 gRPC 流式通信的端点实现。
// 支持与 gRPC 服务端建立长连接，接收服务端推送的消息并通过路由转发处理
//
// 特性：
// - 自动重连：当连接断开时会自动尝试重新建立连接
// - 单路由模式：每个端点实例只支持配置一个消息处理路由
// - 共享连接：相同服务器地址(Server)的多个端点实例会复用同一个gRPC连接，避免重复创建连接
// - 支持配置：可通过 Config 结构体配置服务地址、方法、请求参数、gRPC服务器检查间隔
//
// 示例：
//
// "endpoints": [
//
//	{
//	  "id": "GRPC Stream",
//	  "type": "endpoint/grpc/stream",
//	  "name": "GRPC Stream",
//	  "debugMode": false,
//	  "configuration": {
//		"checkInterval": 10000,
//		"method": "SayHello",
//		"server": "127.0.0.1:9000",
//		"service": "helloworld.Greeter"
//	  },
//	  "processors": null,
//	  "routers": [
//		{
//		  "id": "",
//		  "params": null,
//		  "from": {
//			"path": "*",                      //！！！ 路由只能填写 *，表示所有来源
//			"configuration": null,
//			"processors": null
//		  },
//		  "to": {
//			"path": "bkn3fIAr8x4w:MQTT",
//			"configuration": null,
//			"wait": false,
//			"processors": null
//		  }
//		}
//	  ]
type GrpcStream struct {
	impl.BaseEndpoint
	base.SharedNode[*Client]
	RuleConfig types.Config
	Config     Config
	//client     *Client
	Router endpointApi.Router

	stopCh chan struct{}
}

// RequestMessage 请求消息结构
type RequestMessage struct {
	body []byte
	msg  *types.RuleMsg
	err  error
}

// ResponseMessage 响应消息结构
type ResponseMessage struct {
	body    []byte
	msg     *types.RuleMsg
	headers textproto.MIMEHeader
	err     error
}

func (r *RequestMessage) Body() []byte {
	return r.body
}

func (r *RequestMessage) Headers() textproto.MIMEHeader {
	header := make(textproto.MIMEHeader)
	return header
}

func (r *RequestMessage) From() string {
	return ""
}

func (r *RequestMessage) GetParam(key string) string {
	return ""
}

func (r *RequestMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

func (r *RequestMessage) GetMsg() *types.RuleMsg {
	if r.msg == nil {
		ruleMsg := types.NewMsg(0, r.From(), types.JSON, types.NewMetadata(), string(r.Body()))
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

func (r *ResponseMessage) Body() []byte {
	return r.body
}

func (r *ResponseMessage) Headers() textproto.MIMEHeader {
	if r.headers == nil {
		r.headers = make(textproto.MIMEHeader)
	}
	return r.headers
}

func (r *ResponseMessage) From() string {
	return ""
}

func (r *ResponseMessage) GetParam(key string) string {
	return ""
}

func (r *ResponseMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

func (r *ResponseMessage) GetMsg() *types.RuleMsg {
	return r.msg
}

func (r *ResponseMessage) SetStatusCode(statusCode int) {
}

func (r *ResponseMessage) SetBody(body []byte) {
	r.body = body
}

func (r *ResponseMessage) SetError(err error) {
	r.err = err
}

func (r *ResponseMessage) GetError() error {
	return r.err
}

// Type 返回组件类型
func (x *GrpcStream) Type() string {
	return Type
}

// Id 返回组件ID
func (x *GrpcStream) Id() string {
	return x.Config.Server
}

// New 创建新的实例
func (x *GrpcStream) New() types.Node {
	return &GrpcStream{
		Config: Config{
			Server:        "127.0.0.1:9000",
			Service:       "ble.DataService",
			Method:        "StreamData",
			CheckInterval: 10 * 1000,
		},
	}
}

// Init 初始化组件
func (x *GrpcStream) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	x.RuleConfig = ruleConfig
	_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, false, func() (*Client, error) {
		return x.initClient()
	}, func(client *Client) error {
		if client != nil {
			client.Close()
		}
		return nil
	})
	return nil
}

// Start 启动组件
func (x *GrpcStream) Start() error {
	x.stopCh = make(chan struct{})

	// 确保重连延迟时间有默认值
	if x.Config.CheckInterval <= 0 {
		x.Config.CheckInterval = 10 * 1000
	}

	// 启动流处理和重连
	go x.streamWithReconnect()

	return nil
}

func (x *GrpcStream) streamWithReconnect() {
	for {
		select {
		case <-x.stopCh:
			return
		default:
			if err := x.handleStream(); err != nil {
				if client, _ := x.SharedNode.GetSafely(); client != nil {
					x.SharedNode.Close()
				}
			}
			time.Sleep(time.Duration(x.Config.CheckInterval) * time.Millisecond)
		}
	}
}

// Destroy 销毁组件
func (x *GrpcStream) Destroy() {
	if x.stopCh != nil {
		close(x.stopCh)
	}
	_ = x.SharedNode.Close()
	x.BaseEndpoint.Destroy()
}

type Client struct {
	client *grpcreflect.Client
	conn   *grpc.ClientConn
}

func (c *Client) IsActive() bool {
	return c != nil && c.client != nil && c.conn != nil
}
func (c *Client) Close() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = nil
	c.client = nil
}

func (x *GrpcStream) initClient() (*Client, error) {
	var err error
	conn, err := grpc.Dial(x.Config.Server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	rc := grpcreflect.NewClientAuto(context.Background(), conn)
	client := &Client{
		client: rc,
		conn:   conn,
	}
	return client, err
}

func (x *GrpcStream) handleStream() error {
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		return err
	}
	source := grpcurl.DescriptorSourceFromServer(context.Background(), client.client)
	fullMethod := fmt.Sprintf("%s/%s", x.Config.Service, x.Config.Method)
	//x.Printf("Starting gRPC stream for method: %s", fullMethod)
	var responseBuffer bytes.Buffer
	handler := &grpcurl.DefaultEventHandler{
		Out: &responseBuffer,
		Formatter: func(msg proto.Message) (string, error) {
			dmsg, ok := msg.(*dynamic.Message)
			if !ok {
				return "", fmt.Errorf("failed to convert response to dynamic message")
			}

			jsonBytes, err := dmsg.MarshalJSON()
			if err != nil {
				return "", err
			}
			x.Printf("Received message: %s", string(jsonBytes))
			if x.Router != nil {
				exchange := &endpointApi.Exchange{
					In:  &RequestMessage{body: jsonBytes},
					Out: &ResponseMessage{},
				}
				x.DoProcess(context.Background(), x.Router, exchange)
			}
			return string(jsonBytes), nil
		},
	}

	// 处理headers
	var headers []string
	for k, v := range x.Config.Headers {
		headers = append(headers, fmt.Sprintf("%s:%s", k, v))
	}

	var sent bool
	return grpcurl.InvokeRPC(context.Background(), source, client.conn, fullMethod, headers, handler,
		func(m proto.Message) error {
			if sent {
				return io.EOF
			}
			msg := m.(*dynamic.Message)

			// 根据配置决定请求数据
			var reqData string
			if trimmed := strings.TrimSpace(x.Config.Request); trimmed != "" {
				// 验证是否为有效的 JSON
				if json.Valid([]byte(trimmed)) {
					reqData = trimmed
				} else {
					return fmt.Errorf("invalid JSON in Request config: %s", trimmed)
				}
			}

			if err := msg.UnmarshalJSON([]byte(reqData)); err != nil {
				return err
			}
			sent = true
			return nil
		})
}

// AddRouter 添加路由
func (x *GrpcStream) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router cannot be nil")
	}
	if x.Router != nil {
		return "", errors.New("duplicate router")
	}
	x.Router = router

	return "", nil
}

// RemoveRouter 移除路由
func (x *GrpcStream) RemoveRouter(routerId string, params ...interface{}) error {
	x.Lock()
	defer x.Unlock()
	x.Router = nil
	return nil
}

// Printf 日志输出
func (x *GrpcStream) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}
