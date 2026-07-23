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

package grpc

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/fullstorydev/grpcurl"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

// SeparatorService grpc service and method separator
const SeparatorService = "/"

// SeparatorHeader header key:value separator
const SeparatorHeader = ":"

// ClientConfig defines the gRPC client configuration
type ClientConfig struct {
	Server  string            `json:"server" label:"Server" desc:"gRPC server address, format: host:port" required:"true" ref:"primary"`
	Service string            `json:"service" label:"Service" desc:"gRPC service name, e.g. pkg.ServiceName" required:"true"`
	Method  string            `json:"method" label:"Method" desc:"gRPC method name" required:"true"`
	Request string            `json:"request" label:"Request" desc:"Request JSON data, supports ${metadata.key} and ${msg.key} substitution"`
	Headers map[string]string `json:"headers" label:"Headers" desc:"Custom gRPC request headers"`
}

// ClientNode gRPC query node
type ClientNode struct {
	base.SharedNode[*Client]
	Config          ClientConfig
	serviceTemplate el.Template
	methodTemplate  el.Template
	requestTemplate el.Template
	headersTemplate map[el.Template]el.Template
	hasVar          bool
}

// New Implement the Node interface and create a new instance
func (x *ClientNode) New() types.Node {
	return &ClientNode{
		Config: ClientConfig{
			Server:  "127.0.0.1:50051",
			Service: "helloworld.Greeter",
			Method:  "SayHello",
		},
	}
}

// Type implements the Node interface and returns the component type
func (x *ClientNode) Type() string {
	return "x/grpcClient"
}

// Init initializes the gRPC client
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*Client, error) {
		return x.initClient()
	}, func(client *Client) error {
		// Cleanup callback function
		return client.conn.Close()
	})
	// Initialize service templates
	serviceTemplate, err := el.NewTemplate(x.Config.Service)
	if err != nil {
		return err
	}
	x.serviceTemplate = serviceTemplate
	if serviceTemplate.HasVar() {
		x.hasVar = true
	}

	// Initialize the method template
	methodTemplate, err := el.NewTemplate(x.Config.Method)
	if err != nil {
		return err
	}
	x.methodTemplate = methodTemplate
	if methodTemplate.HasVar() {
		x.hasVar = true
	}

	// Initialize the request template
	requestTemplate, err := el.NewTemplate(x.Config.Request)
	if err != nil {
		return err
	}
	x.requestTemplate = requestTemplate
	if requestTemplate.HasVar() {
		x.hasVar = true
	}

	// Initialize the head template
	var headerTemplates = make(map[el.Template]el.Template)
	for key, value := range x.Config.Headers {
		keyTmpl, err := el.NewTemplate(key)
		if err != nil {
			return err
		}
		valueTmpl, err := el.NewTemplate(value)
		if err != nil {
			return err
		}
		headerTemplates[keyTmpl] = valueTmpl
		if keyTmpl.HasVar() || valueTmpl.HasVar() {
			x.hasVar = true
		}
	}
	x.headersTemplate = headerTemplates
	return nil
}

// OnMsg implements the Node interface to process messages
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	descSource := grpcurl.DescriptorSourceFromServer(context.Background(), client.client)

	var evn map[string]interface{}
	if x.hasVar {
		evn = base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	}
	service := x.serviceTemplate.ExecuteAsString(evn)
	method := x.methodTemplate.ExecuteAsString(evn)
	request := x.requestTemplate.ExecuteAsString(evn)
	if request == "" {
		request = msg.GetData()
	}
	serviceAndMethod := service + SeparatorService + method
	var responseBuffer bytes.Buffer
	handler := &grpcurl.DefaultEventHandler{
		Out: &responseBuffer,
		Formatter: func(message proto.Message) (string, error) {
			protoMessage, ok := message.(*dynamic.Message)
			if !ok {
				return "", errors.New("invalid message type")
			}

			if v, err := protoMessage.MarshalJSON(); err != nil {
				return "", err
			} else {
				return string(v), nil
			}
		},
	}
	// Implement the RequestSupplier function
	requestDataSupplier := func(message proto.Message) error {
		// Fill the request data into the protobuf message
		protoMessage, ok := message.(*dynamic.Message)
		if !ok {
			return errors.New("invalid message type")
		}
		protoMessage.Reset()
		if err := protoMessage.UnmarshalJSON([]byte(request)); err != nil {
			return err
		}
		// If it is a one-time request, return io.EOF stated that there is no further data request
		return io.EOF
	}
	var headers []string
	//Set the header
	for key, value := range x.headersTemplate {
		headers = append(headers, key.ExecuteAsString(evn)+SeparatorHeader+value.ExecuteAsString(evn))
	}
	err = grpcurl.InvokeRPC(context.Background(), descSource, client.conn, serviceAndMethod, headers, handler, requestDataSupplier)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	} else {
		msg.SetData(responseBuffer.String())
		ctx.TellSuccess(msg)
	}
}

func (x *ClientNode) Destroy() {
	_ = x.SharedNode.Close()
}

// Desc returns the component description
func (x *ClientNode) Desc() string {
	return "gRPC client for calling remote gRPC services. Routes to Success/Failure"
}

func (x *ClientNode) initClient() (*Client, error) {
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

type Client struct {
	client *grpcreflect.Client
	conn   *grpc.ClientConn
}
