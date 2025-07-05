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
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

// SeparatorService grpc service和method 分隔符
const SeparatorService = "/"

// SeparatorHeader header key:value 分隔符
const SeparatorHeader = ":"

// ClientConfig 定义 gRPC 客户端配置
type ClientConfig struct {
	// Server 服务器地址，格式：host:port
	Server string
	// Service gRPC 服务名称，允许使用 ${} 占位符变量
	Service string
	// Method gRPC 方法名称，允许使用 ${} 占位符变量
	Method string
	// 请求参数 如果空，则使用当前消息负荷。参数使用JSON编码，必须和service/method要求一致。允许使用 ${} 占位符变量
	Request string
	//Headers 请求头,可以使用 ${metadata.key} 读取元数据中的变量或者使用 ${msg.key} 读取消息负荷中的变量进行替换
	Headers map[string]string
}

// ClientNode gRPC 查询节点
type ClientNode struct {
	base.SharedNode[*Client]
	Config          ClientConfig
	serviceTemplate str.Template
	methodTemplate  str.Template
	requestTemplate str.Template
	headersTemplate map[str.Template]str.Template
	hasVar          bool
}

// New 实现 Node 接口，创建新实例
func (x *ClientNode) New() types.Node {
	return &ClientNode{
		Config: ClientConfig{
			Server:  "127.0.0.1:50051",
			Service: "helloworld.Greeter",
			Method:  "SayHello",
		},
	}
}

// Type 实现 Node 接口，返回组件类型
func (x *ClientNode) Type() string {
	return "x/grpcClient"
}

// Init 初始化 gRPC 客户端
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*Client, error) {
		return x.initClient()
	}, func(client *Client) error {
		// 清理回调函数
		return client.conn.Close()
	})
	x.serviceTemplate = str.NewTemplate(x.Config.Service)
	x.methodTemplate = str.NewTemplate(x.Config.Method)
	x.requestTemplate = str.NewTemplate(x.Config.Request)
	if !x.serviceTemplate.IsNotVar() || !x.methodTemplate.IsNotVar() || !x.requestTemplate.IsNotVar() {
		x.hasVar = true
	}
	var headerTemplates = make(map[str.Template]str.Template)
	for key, value := range x.Config.Headers {
		keyTmpl := str.NewTemplate(key)
		valueTmpl := str.NewTemplate(value)
		headerTemplates[keyTmpl] = valueTmpl
		if !keyTmpl.IsNotVar() || !valueTmpl.IsNotVar() {
			x.hasVar = true
		}
	}
	x.headersTemplate = headerTemplates
	return nil
}

// OnMsg 实现 Node 接口，处理消息
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
	service := x.serviceTemplate.Execute(evn)
	method := x.methodTemplate.Execute(evn)
	request := x.requestTemplate.Execute(evn)
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
	// 实现RequestSupplier函数
	requestDataSupplier := func(message proto.Message) error {
		// 将请求数据填充到protobuf消息中
		protoMessage, ok := message.(*dynamic.Message)
		if !ok {
			return errors.New("invalid message type")
		}
		protoMessage.Reset()
		if err := protoMessage.UnmarshalJSON([]byte(request)); err != nil {
			return err
		}
		// 如果是一次性请求，返回io.EOF表示没有更多请求数据
		return io.EOF
	}
	var headers []string
	//设置header
	for key, value := range x.headersTemplate {
		headers = append(headers, key.Execute(evn)+SeparatorHeader+value.Execute(evn))
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
