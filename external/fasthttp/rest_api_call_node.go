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

//Example of rule chain node configuration:
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
	//Replace the standard restApiCall component
	_ = rulego.Registry.Unregister(Type)
	_ = rulego.Registry.Register(&RestApiCallNode{})
}

// Type component type Replace standard restApiCall components
var Type = "restApiCall"

// RestApiCallNode will call GET | via the FastHTTP API POST | PUT | DELETE to an external REST service.
// If the request is `Success`ful, send the HTTP response message to the 'Success' chain; otherwise, send it to the `Failure` chain,
// metaData.status records response error codes and metaData.errorBody records error messages.
type RestApiCallNode struct {
	//Node configuration
	Config external.RestApiCallNodeConfiguration
	//Fasthttp client
	client   *fasthttp.Client
	template *external.HTTPRequestTemplate
}

// Type returns the component type
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

// Init initializes the component
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

// OnMsg processes a message
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

	// Create a fastHTTP request
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}()

	// Set the URL and method
	req.SetRequestURI(endpointUrl)
	req.Header.SetMethod(x.Config.RequestMethod)

	// Set up the request body
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

	// Set the header
	for key, value := range x.template.HeadersTemplate {
		req.Header.Set(key.ExecuteAsString(evn), value.ExecuteAsString(evn))
	}

	// Request fulfillment
	err := x.client.Do(req, resp)
	if err != nil {
		msg.Metadata.PutValue(external.ErrorBodyMetadataKey, err.Error())
		ctx.TellFailure(msg, err)
		return
	}

	// Handle the response
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

// Destroy releases resources
func (x *RestApiCallNode) Destroy() {
	if x.client != nil {
		x.client.CloseIdleConnections()
		// Wait for the connection to be completely shut down
		time.Sleep(1 * time.Millisecond)
		x.client = nil
	}
}

// NewFastHttpClient creates a FastHTTP client
func NewFastHttpClient(config external.RestApiCallNodeConfiguration) *fasthttp.Client {
	client := &fasthttp.Client{
		ReadTimeout:                   time.Duration(config.ReadTimeoutMs) * time.Millisecond,
		MaxConnsPerHost:               config.MaxParallelRequestsCount,
		DisableHeaderNamesNormalizing: true,
		DisablePathNormalizing:        true,
	}

	// Configure TLS
	if config.InsecureSkipVerify {
		client.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}

	// Configure the agent
	if config.EnableProxy {
		if config.UseSystemProxyProperties {
			// Use system proxy settings
			client.Dial = createSystemProxyDialer()
		} else {
			// Use custom proxy settings
			if proxyURL := external.HttpUtils.BuildProxyURL(config.ProxyScheme, config.ProxyHost, config.ProxyPort, config.ProxyUser, config.ProxyPassword); proxyURL != nil {
				client.Dial = createProxyDialer(proxyURL)
			}
		}
	}

	return client
}

// createProxyDialer creates a proxy dialer
func createProxyDialer(proxyURL *url.URL) func(addr string) (net.Conn, error) {
	return func(addr string) (net.Conn, error) {
		// Parse the target address
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}

		// Connect to the proxy server
		proxyConn, err := net.DialTimeout("tcp", proxyURL.Host, time.Second*30)
		if err != nil {
			return nil, err
		}

		// Handled according to the type of agent
		switch proxyURL.Scheme {
		case "http", "https":
			// HTTP proxy
			return setupHTTPProxy(proxyConn, proxyURL, host, port)
		case "socks5":
			// SOCKS5 agency
			return setupSOCKS5Proxy(proxyConn, proxyURL, host, port)
		default:
			proxyConn.Close()
			return nil, fmt.Errorf("unsupported proxy scheme: %s", proxyURL.Scheme)
		}
	}
}

// createSystemProxyDialer creates a system proxy dialer
func createSystemProxyDialer() func(addr string) (net.Conn, error) {
	return func(addr string) (net.Conn, error) {
		// Obtain system proxy settings
		proxyURL := external.HttpUtils.GetSystemProxy()
		if proxyURL == nil {
			// No system proxy, direct connection
			return fasthttp.DialDualStackTimeout(addr, time.Second*30)
		}
		// Use system proxies
		return createProxyDialer(proxyURL)(addr)
	}
}

// setupHTTPProxy Sets up an HTTP proxy
func setupHTTPProxy(conn net.Conn, proxyURL *url.URL, targetHost, targetPort string) (net.Conn, error) {
	// Set connection timeout
	conn.SetDeadline(time.Now().Add(time.Second * 30))
	defer conn.SetDeadline(time.Time{}) // Clear timeout settings

	// Build a CONNECT request
	connectReq := fmt.Sprintf("CONNECT %s:%s HTTP/1.1\r\nHost: %s:%s\r\n", targetHost, targetPort, targetHost, targetPort)

	// Add agent certification
	if proxyURL.User != nil {
		if password, ok := proxyURL.User.Password(); ok {
			auth := proxyURL.User.Username() + ":" + password
			encoded := "Basic " + base64Encode(auth)
			connectReq += "Proxy-Authorization: " + encoded + "\r\n"
		}
	}

	connectReq += "\r\n"

	// Send a CONNECT request
	if _, err := conn.Write([]byte(connectReq)); err != nil {
		conn.Close()
		return nil, err
	}

	// Read the response
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		return nil, err
	}

	// Check the response status
	response := string(buf[:n])
	if !strings.Contains(response, "200 Connection established") {
		conn.Close()
		return nil, fmt.Errorf("proxy connection failed: %s", response)
	}

	return conn, nil
}

// setupSOCKS5Proxy Sets up SOCKS5 proxy
func setupSOCKS5Proxy(conn net.Conn, proxyURL *url.URL, targetHost, targetPort string) (net.Conn, error) {
	// Multiplex the SOCKS5 dialer of the external package
	dialer := external.HttpUtils.CreateSOCKS5Dialer(proxyURL)
	conn.Close() // Close the original connection
	return dialer("tcp", targetHost+":"+targetPort)
}

// base64Encode Simple base64 encoding (functions that reuse external packets)
func base64Encode(s string) string {
	return external.HttpUtils.Base64Encode(s)
}

// SSE Streaming Data Reading - FastHTTP Version (Reusing ReadFromStream for External Packages)
func readFromFastHttpStream(ctx types.RuleContext, msg types.RuleMsg, resp *fasthttp.Response) {
	// Create an adapter and set fasthttp.Response is adapted to http.Response
	body := resp.Body()
	bodyReader := bytes.NewReader(body)

	// Create a simulated http.Response: reuse external.HttpUtils.ReadFromStream
	adaptedResp := &http.Response{
		Body: io.NopCloser(bodyReader),
	}

	// Reusing the ReadFromStream function of the external package
	external.HttpUtils.ReadFromStream(ctx, msg, adaptedResp)
}
