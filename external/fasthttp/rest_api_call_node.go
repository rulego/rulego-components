/*
 * Copyright 2025 The RuleGo Authors.
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

package fasthttp

//规则链节点配置示例：
// {
//        "id": "s3",
//        "type": "restApiCall",
//        "name": "推送数据",
//        "debugMode": false,
//        "configuration": {
//          "restEndpointUrlPattern": "http://192.168.118.29:8080/msg",
//          "requestMethod": "POST",
//          "maxParallelRequestsCount": 200
//        }
//      }
import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rulego/rulego"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/components/external"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
	"github.com/valyala/fasthttp"
)

func init() {
	//替换标准 restApiCall组件
	_ = rulego.Registry.Unregister(Type)
	_ = rulego.Registry.Register(&RestApiCallNode{})
}

// Type 组件类型 替换标准 restApiCall组件
var Type = "restApiCall"

// RestApiCallNode 将通过FastHTTP API调用GET | POST | PUT | DELETE到外部REST服务。
// 如果请求成功，把HTTP响应消息发送到`Success`链, 否则发到`Failure`链，
// metaData.status记录响应错误码和metaData.errorBody记录错误信息。
type RestApiCallNode struct {
	//节点配置
	Config external.RestApiCallNodeConfiguration
	//fasthttp客户端
	client   *fasthttp.Client
	template *external.HTTPRequestTemplate
}

// Type 组件类型
func (x *RestApiCallNode) Type() string {
	return Type
}

func (x *RestApiCallNode) New() types.Node {
	headers := map[string]string{"Content-Type": "application/json"}
	config := external.RestApiCallNodeConfiguration{
		RequestMethod:            "POST",
		MaxParallelRequestsCount: 200,
		ReadTimeoutMs:            2000,
		Headers:                  headers,
	}
	return &RestApiCallNode{Config: config}
}

// Init 初始化
func (x *RestApiCallNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		x.Config.RequestMethod = strings.ToUpper(x.Config.RequestMethod)
		x.client = NewFastHttpClient(x.Config)
		if tmp, err := external.HttpUtils.BuildRequestTemplate(&x.Config); err != nil {
			return err
		} else {
			x.template = tmp
		}
	}
	return err
}

// OnMsg 处理消息
func (x *RestApiCallNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	var evn map[string]interface{}
	if x.template.HasVar {
		evn = base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	}
	var endpointUrl = ""
	if v, err := x.template.UrlTemplate.Execute(evn); err != nil {
		ctx.TellFailure(msg, err)
		return
	} else {
		endpointUrl = str.ToString(v)
	}

	// 创建fasthttp请求
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}()

	// 设置URL和方法
	req.SetRequestURI(endpointUrl)
	req.Header.SetMethod(x.Config.RequestMethod)

	// 设置请求体
	var body []byte
	if !x.Config.WithoutRequestBody {
		if x.template.BodyTemplate != nil {
			if v, err := x.template.BodyTemplate.Execute(evn); err != nil {
				ctx.TellFailure(msg, err)
				return
			} else {
				body = []byte(str.ToString(v))
			}
		} else {
			body = []byte(msg.GetData())
		}
		req.SetBody(body)
	}

	// 设置header
	for key, value := range x.template.HeadersTemplate {
		req.Header.Set(key.ExecuteAsString(evn), value.ExecuteAsString(evn))
	}

	// 执行请求
	err := x.client.Do(req, resp)
	if err != nil {
		msg.Metadata.PutValue(external.ErrorBodyMetadataKey, err.Error())
		ctx.TellFailure(msg, err)
		return
	}

	// 处理响应
	statusCode := resp.StatusCode()
	msg.Metadata.PutValue(external.StatusMetadataKey, fmt.Sprintf("%d %s", statusCode, fasthttp.StatusMessage(statusCode)))
	msg.Metadata.PutValue(external.StatusCodeMetadataKey, strconv.Itoa(statusCode))

	if x.template.IsStream {
		if statusCode == 200 {
			readFromFastHttpStream(ctx, msg, resp)
		} else {
			body := resp.Body()
			msg.Metadata.PutValue(external.ErrorBodyMetadataKey, string(body))
			ctx.TellNext(msg, types.Failure)
		}
	} else {
		body := resp.Body()
		if statusCode == 200 {
			msg.SetData(string(body))
			ctx.TellSuccess(msg)
		} else {
			strB := string(body)
			msg.Metadata.PutValue(external.ErrorBodyMetadataKey, strB)
			ctx.TellFailure(msg, errors.New(strB))
		}
	}
}

// Destroy 销毁
func (x *RestApiCallNode) Destroy() {
	if x.client != nil {
		x.client.CloseIdleConnections()
		// 等待连接完全关闭
		time.Sleep(1 * time.Millisecond)
		x.client = nil
	}
}

// NewFastHttpClient 创建FastHTTP客户端
func NewFastHttpClient(config external.RestApiCallNodeConfiguration) *fasthttp.Client {
	client := &fasthttp.Client{
		ReadTimeout:                   time.Duration(config.ReadTimeoutMs) * time.Millisecond,
		MaxConnsPerHost:               config.MaxParallelRequestsCount,
		DisableHeaderNamesNormalizing: true,
		DisablePathNormalizing:        true,
	}

	// 配置TLS
	if config.InsecureSkipVerify {
		client.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}

	// 配置代理
	if config.EnableProxy {
		if config.UseSystemProxyProperties {
			// 使用系统代理设置
			client.Dial = createSystemProxyDialer()
		} else {
			// 使用自定义代理设置
			if proxyURL := external.HttpUtils.BuildProxyURL(config.ProxyScheme, config.ProxyHost, config.ProxyPort, config.ProxyUser, config.ProxyPassword); proxyURL != nil {
				client.Dial = createProxyDialer(proxyURL)
			}
		}
	}

	return client
}

// createProxyDialer 创建代理拨号器
func createProxyDialer(proxyURL *url.URL) func(addr string) (net.Conn, error) {
	return func(addr string) (net.Conn, error) {
		// 解析目标地址
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}

		// 连接到代理服务器
		proxyConn, err := net.DialTimeout("tcp", proxyURL.Host, time.Second*30)
		if err != nil {
			return nil, err
		}

		// 根据代理类型处理
		switch proxyURL.Scheme {
		case "http", "https":
			// HTTP代理
			return setupHTTPProxy(proxyConn, proxyURL, host, port)
		case "socks5":
			// SOCKS5代理
			return setupSOCKS5Proxy(proxyConn, proxyURL, host, port)
		default:
			proxyConn.Close()
			return nil, fmt.Errorf("unsupported proxy scheme: %s", proxyURL.Scheme)
		}
	}
}

// createSystemProxyDialer 创建系统代理拨号器
func createSystemProxyDialer() func(addr string) (net.Conn, error) {
	return func(addr string) (net.Conn, error) {
		// 获取系统代理设置
		proxyURL := external.HttpUtils.GetSystemProxy()
		if proxyURL == nil {
			// 没有系统代理，直接连接
			return fasthttp.DialDualStackTimeout(addr, time.Second*30)
		}
		// 使用系统代理
		return createProxyDialer(proxyURL)(addr)
	}
}

// setupHTTPProxy 设置HTTP代理
func setupHTTPProxy(conn net.Conn, proxyURL *url.URL, targetHost, targetPort string) (net.Conn, error) {
	// 设置连接超时
	conn.SetDeadline(time.Now().Add(time.Second * 30))
	defer conn.SetDeadline(time.Time{}) // 清除超时设置

	// 构建CONNECT请求
	connectReq := fmt.Sprintf("CONNECT %s:%s HTTP/1.1\r\nHost: %s:%s\r\n", targetHost, targetPort, targetHost, targetPort)

	// 添加代理认证
	if proxyURL.User != nil {
		if password, ok := proxyURL.User.Password(); ok {
			auth := proxyURL.User.Username() + ":" + password
			encoded := "Basic " + base64Encode(auth)
			connectReq += "Proxy-Authorization: " + encoded + "\r\n"
		}
	}

	connectReq += "\r\n"

	// 发送CONNECT请求
	if _, err := conn.Write([]byte(connectReq)); err != nil {
		conn.Close()
		return nil, err
	}

	// 读取响应
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		return nil, err
	}

	// 检查响应状态
	response := string(buf[:n])
	if !strings.Contains(response, "200 Connection established") {
		conn.Close()
		return nil, fmt.Errorf("proxy connection failed: %s", response)
	}

	return conn, nil
}

// setupSOCKS5Proxy 设置SOCKS5代理
func setupSOCKS5Proxy(conn net.Conn, proxyURL *url.URL, targetHost, targetPort string) (net.Conn, error) {
	// 复用external包的SOCKS5拨号器
	dialer := external.HttpUtils.CreateSOCKS5Dialer(proxyURL)
	conn.Close() // 关闭原连接
	return dialer("tcp", targetHost+":"+targetPort)
}

// base64Encode 简单的base64编码（复用external包的函数）
func base64Encode(s string) string {
	return external.HttpUtils.Base64Encode(s)
}

// SSE 流式数据读取 - FastHTTP版本（复用external包的ReadFromStream）
func readFromFastHttpStream(ctx types.RuleContext, msg types.RuleMsg, resp *fasthttp.Response) {
	// 创建一个适配器，将fasthttp.Response适配为http.Response
	body := resp.Body()
	bodyReader := bytes.NewReader(body)

	// 创建一个模拟的http.Response来复用external.HttpUtils.ReadFromStream
	adaptedResp := &http.Response{
		Body: io.NopCloser(bodyReader),
	}

	// 复用external包的ReadFromStream函数
	external.HttpUtils.ReadFromStream(ctx, msg, adaptedResp)
}
