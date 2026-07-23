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

// Config gRPC flow configuration
type Config struct {
	Server        string            `json:"server" label:"Server" desc:"gRPC server address, format: host:port" required:"true" ref:"primary"`
	Service       string            `json:"service" label:"Service" desc:"gRPC service name, e.g. pkg.ServiceName" required:"true"`
	Method        string            `json:"method" label:"Method" desc:"gRPC method name, e.g. StreamData" required:"true"`
	Headers       map[string]string `json:"headers" label:"Headers" desc:"Custom gRPC request headers"`
	Request       string            `json:"request" label:"Request" desc:"Initial request data, sends empty data if not set"`
	CheckInterval int               `json:"checkInterval" label:"Check Interval (ms)" desc:"Connection health check interval in milliseconds"`
}

// GrpcStream provides endpoint implementation based on gRPC streaming communication.
// Supports establishing long connections with gRPC servers, receiving messages pushed from the server and processing them through routing forwarding
//
// Features:
// - Automatic reconnection: When a connection is disconnected, it will automatically attempt to reestablish the connection
// - Single routing mode: Each endpoint instance supports configuring only one message processing route
// - Shared connections: Multiple endpoint instances with the same server address (Server) reuse the same gRPC connection to avoid creating duplicate connections
// - Configuration support: Service addresses, methods, request parameters, and gRPC server check intervals can be configured via the Config structure
//
// Example:
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
//			"path": "*", //！！！ Routes can only be filled with *, indicating all sources
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

// RequestMessage Request message structure
type RequestMessage struct {
	body []byte
	msg  *types.RuleMsg
	err  error
}

// ResponseMessage
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

// Type returns the component type
func (x *GrpcStream) Type() string {
	return Type
}

// Id returns the component ID
func (x *GrpcStream) Id() string {
	return x.Config.Server
}

// New: Create a new instance
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

func (x *GrpcStream) Def() types.ComponentForm {
	return types.ComponentForm{
		Desc: "gRPC stream endpoint: receives streaming data from a gRPC service+method (configured via the 'service'/'method' fields, not from.path) and processes each message",
		RouterForm: &types.RouterForm{
			Hide: true,
		},
	}
}

// Init initializes the component
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

// Start the component
func (x *GrpcStream) Start() error {
	x.stopCh = make(chan struct{})

	// Make sure the reconnection delay time has a default value
	if x.Config.CheckInterval <= 0 {
		x.Config.CheckInterval = 10 * 1000
	}

	// Start stream processing and reconnection
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

// Destroy releases component resources
func (x *GrpcStream) Destroy() {
	if x.stopCh != nil {
		close(x.stopCh)
	}
	//Clean up the instance
	_ = x.SharedNode.Close()
	//Set to nil to prevent goroutine rebuild
	x.Locker.Lock()
	x.InitInstanceFunc = nil
	x.Locker.Unlock()

	x.Lock()
	x.Router = nil
	x.Unlock()
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
			x.RLock()
			if x.Router != nil {
				exchange := &endpointApi.Exchange{
					In:  &RequestMessage{body: jsonBytes},
					Out: &ResponseMessage{},
				}
				x.DoProcess(context.Background(), x.Router, exchange)
			}
			x.RUnlock()
			return string(jsonBytes), nil
		},
	}

	// Handle headers
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

			// Data requests are decided based on the configuration
			var reqData string
			if trimmed := strings.TrimSpace(x.Config.Request); trimmed != "" {
				// Verify whether the JSON is valid
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

// AddRouter adds a route
func (x *GrpcStream) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	x.Lock()
	defer x.Unlock()
	if router == nil {
		return "", errors.New("router cannot be nil")
	}
	if x.Router != nil {
		return "", errors.New("duplicate router")
	}
	x.Router = router

	return "", nil
}

// RemoveRouter removes the route
func (x *GrpcStream) RemoveRouter(routerId string, params ...interface{}) error {
	x.Lock()
	defer x.Unlock()
	x.Router = nil
	return nil
}

// Printf log output
func (x *GrpcStream) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}
