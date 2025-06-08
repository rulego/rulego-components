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

import (
	"fmt"
	"github.com/rulego/rulego/components/external"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
)

const (
	// 代理测试开关
	ENABLE_PROXY_TEST = false
	// 代理配置
	PROXY_HOST        = "127.0.0.1"
	HTTP_PROXY_PORT   = 10809
	SOCKS5_PROXY_PORT = 10808
)

func TestRestApiCallNode(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&RestApiCallNode{})
	var targetNodeType = Type

	headers := map[string]string{"Content-Type": "application/json"}

	t.Run("NewNode", func(t *testing.T) {
		test.NodeNew(t, targetNodeType, &RestApiCallNode{}, types.Configuration{
			"requestMethod":            "POST",
			"maxParallelRequestsCount": 200,
			"readTimeoutMs":            2000,
			"headers":                  headers,
		}, Registry)
	})

	t.Run("InitNode", func(t *testing.T) {
		test.NodeInit(t, targetNodeType, types.Configuration{
			"requestMethod":            "GET",
			"maxParallelRequestsCount": 100,
			"readTimeoutMs":            0,
			"withoutRequestBody":       true,
			"headers":                  headers,
		}, types.Configuration{
			"requestMethod":            "GET",
			"maxParallelRequestsCount": 100,
			"readTimeoutMs":            0,
			"withoutRequestBody":       true,
			"headers":                  headers,
		}, Registry)
	})

	t.Run("DefaultConfig", func(t *testing.T) {
		test.NodeInit(t, targetNodeType, types.Configuration{
			"requestMethod":            "POST",
			"maxParallelRequestsCount": 200,
			"readTimeoutMs":            2000,
			"headers":                  headers,
		}, types.Configuration{
			"requestMethod":            "POST",
			"maxParallelRequestsCount": 200,
			"readTimeoutMs":            2000,
			"headers":                  headers,
		}, Registry)
	})

	t.Run("OnMsg", func(t *testing.T) {
		// 创建测试服务器
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/success":
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"status":"success"}`))
			case "/notfound":
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"error":"not found"}`))
			case "/method-not-allowed":
				w.WriteHeader(http.StatusMethodNotAllowed)
				w.Write([]byte(`{"error":"method not allowed"}`))
			default:
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"message":"hello world"}`))
			}
		}))
		defer testServer.Close()

		// 测试成功请求
		node1, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"restEndpointUrlPattern": testServer.URL + "/success",
			"requestMethod":          "POST",
		}, Registry)
		assert.Nil(t, err)
		defer node1.Destroy() // 确保节点被销毁

		// 测试404错误
		node2, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"restEndpointUrlPattern": testServer.URL + "/notfound",
			"requestMethod":          "POST",
		}, Registry)
		assert.Nil(t, err)

		// 测试方法不允许错误
		node3, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"restEndpointUrlPattern": testServer.URL + "/method-not-allowed",
			"requestMethod":          "POST",
		}, Registry)
		assert.Nil(t, err)

		// 测试无请求体
		node4, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"restEndpointUrlPattern": testServer.URL + "/success",
			"requestMethod":          "GET",
			"withoutRequestBody":     true,
		}, Registry)
		assert.Nil(t, err)

		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("productType", "test")
		msgList := []test.Msg{
			{
				MetaData:   metaData,
				MsgType:    "ACTIVITY_EVENT",
				Data:       "{\"temperature\":35}",
				AfterSleep: time.Millisecond * 200,
			},
			{
				MetaData:   metaData,
				MsgType:    "ACTIVITY_EVENT2",
				Data:       "{\"temperature\":60}",
				AfterSleep: time.Millisecond * 200,
			},
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node1,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
					code := msg.Metadata.GetValue(external.StatusCodeMetadataKey)
					assert.Equal(t, "200", code)
				},
			},
			{
				Node:    node2,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Failure, relationType)
					code := msg.Metadata.GetValue(external.StatusCodeMetadataKey)
					assert.Equal(t, "404", code)
				},
			},
			{
				Node:    node3,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Failure, relationType)
					code := msg.Metadata.GetValue(external.StatusCodeMetadataKey)
					assert.Equal(t, "405", code)
				},
			},
			{
				Node:    node4,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
					code := msg.Metadata.GetValue(external.StatusCodeMetadataKey)
					assert.Equal(t, "200", code)
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}

		// 测试代理功能
		t.Run("ProxyTest", func(t *testing.T) {
			// 检查是否启用代理测试
			if !ENABLE_PROXY_TEST {
				t.Skip("跳过代理测试，修改常量 ENABLE_PROXY_TEST 为 true 来启用")
				return
			}

			// 创建测试服务器
			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"message":"proxy test success","clientIP":"` + r.RemoteAddr + `"}`))
			}))
			defer testServer.Close()

			// 测试HTTP代理
			node1, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
				"restEndpointUrlPattern": testServer.URL + "/proxy-test",
				"requestMethod":          "GET",
				"withoutRequestBody":     true,
				"enableProxy":            true,
				"proxyScheme":            "http",
				"proxyHost":              PROXY_HOST,
				"proxyPort":              HTTP_PROXY_PORT,
			}, Registry)
			assert.Nil(t, err)
			defer node1.Destroy() // 确保节点被销毁

			// 测试带认证的HTTP代理
			node2, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
				"restEndpointUrlPattern": testServer.URL + "/proxy-test",
				"requestMethod":          "GET",
				"withoutRequestBody":     true,
				"enableProxy":            true,
				"proxyScheme":            "http",
				"proxyHost":              PROXY_HOST,
				"proxyPort":              HTTP_PROXY_PORT,
				"proxyUser":              "testuser",
				"proxyPassword":          "testpass",
			}, Registry)
			assert.Nil(t, err)
			defer node2.Destroy() // 确保节点被销毁

			// 测试SOCKS5代理
			node3, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
				"restEndpointUrlPattern": testServer.URL + "/proxy-test",
				"requestMethod":          "GET",
				"withoutRequestBody":     true,
				"enableProxy":            true,
				"proxyScheme":            "socks5",
				"proxyHost":              PROXY_HOST,
				"proxyPort":              SOCKS5_PROXY_PORT,
			}, Registry)
			assert.Nil(t, err)
			defer node3.Destroy() // 确保节点被销毁

			// 测试系统代理
			node4, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
				"restEndpointUrlPattern":   testServer.URL + "/proxy-test",
				"requestMethod":            "GET",
				"withoutRequestBody":       true,
				"enableProxy":              true,
				"useSystemProxyProperties": true,
			}, Registry)
			assert.Nil(t, err)
			defer node4.Destroy() // 确保节点被销毁

			metaData := types.BuildMetadata(make(map[string]string))
			msgList := []test.Msg{
				{
					MetaData:   metaData,
					MsgType:    "PROXY_TEST",
					Data:       "{}",
					AfterSleep: time.Millisecond * 500,
				},
			}

			var nodeList = []test.NodeAndCallback{
				{
					Node:    node1,
					MsgList: msgList,
					Callback: func(msg types.RuleMsg, relationType string, err error) {
						code := msg.Metadata.GetValue(external.StatusCodeMetadataKey)
						if relationType == types.Success {
							assert.Equal(t, "200", code)
							assert.True(t, strings.Contains(msg.GetData(), "proxy test success"))
						} else {
							t.Logf("HTTP代理测试失败（可能代理不可用）: %s, 错误: %v", code, err)
						}
					},
				},
				{
					Node:    node2,
					MsgList: msgList,
					Callback: func(msg types.RuleMsg, relationType string, err error) {
						code := msg.Metadata.GetValue(external.StatusCodeMetadataKey)
						if relationType == types.Success {
							assert.Equal(t, "200", code)
						} else {
							t.Logf("带认证HTTP代理测试失败（可能代理不可用或认证失败）: %s, 错误: %v", code, err)
						}
					},
				},
				{
					Node:    node3,
					MsgList: msgList,
					Callback: func(msg types.RuleMsg, relationType string, err error) {
						code := msg.Metadata.GetValue(external.StatusCodeMetadataKey)
						if relationType == types.Success {
							assert.Equal(t, "200", code)
							//t.Logf("SOCKS5代理测试成功: %s", msg.GetData())
						} else {
							t.Logf("SOCKS5代理测试失败（可能代理不可用）: %s, 错误: %v", code, err)
						}
					},
				},
				{
					Node:    node4,
					MsgList: msgList,
					Callback: func(msg types.RuleMsg, relationType string, err error) {
						code := msg.Metadata.GetValue(external.StatusCodeMetadataKey)
						if relationType == types.Success {
							assert.Equal(t, "200", code)
							//t.Logf("系统代理测试成功: %s", msg.GetData())
						} else {
							t.Logf("系统代理测试失败（可能系统未配置代理）: %s, 错误: %v", code, err)
						}
					},
				},
			}
			for _, item := range nodeList {
				test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
			}
			time.Sleep(time.Second * 2)
		})

		// 测试系统代理配置
		t.Run("SystemProxyTest", func(t *testing.T) {
			// 创建测试服务器
			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"message":"system proxy test success"}`))
			}))
			defer testServer.Close()

			// 测试使用系统代理配置
			node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
				"restEndpointUrlPattern":   testServer.URL + "/system-proxy-test",
				"requestMethod":            "GET",
				"withoutRequestBody":       true,
				"enableProxy":              true,
				"useSystemProxyProperties": true,
			}, Registry)
			assert.Nil(t, err)
			defer node.Destroy() // 确保节点被销毁

			metaData := types.BuildMetadata(make(map[string]string))
			msgList := []test.Msg{
				{
					MetaData:   metaData,
					MsgType:    "SYSTEM_PROXY_TEST",
					Data:       "{}",
					AfterSleep: time.Millisecond * 500,
				},
			}

			test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
				code := msg.Metadata.GetValue(external.StatusCodeMetadataKey)
				if relationType == types.Success {
					assert.Equal(t, "200", code)
					assert.True(t, strings.Contains(msg.GetData(), "system proxy test success"))
					//t.Logf("系统代理测试成功: %s", msg.GetData())
				} else {
					t.Logf("系统代理测试失败（可能系统未配置代理）: %s, 错误: %v", code, err)
				}
			})
			time.Sleep(time.Second * 2)
		})
	})

	//SSE(Server-Sent Events)流式请求
	t.Run("SSEOnMsg", func(t *testing.T) {
		sseServer := os.Getenv("TEST_SSE_SERVER")
		if sseServer == "" {
			return
		}
		node1, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"headers":                map[string]string{"Content-Type": "application/json", "Accept": "text/event-stream"},
			"restEndpointUrlPattern": sseServer,
			"requestMethod":          "POST",
		}, Registry)
		assert.Nil(t, err)

		//404
		node2, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"headers":                map[string]string{"Content-Type": "application/json", "Accept": "text/event-stream"},
			"restEndpointUrlPattern": sseServer + "/nothings/",
			"requestMethod":          "POST",
		}, Registry)
		assert.Nil(t, err)

		done := false

		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("productType", "test")

		msgList := []test.Msg{
			{
				MetaData:   metaData,
				MsgType:    "chat",
				Data:       "{\"model\": \"chatglm3-6b-32k\", \"messages\": [{\"role\": \"system\", \"content\": \"You are ChatGLM3, a large language model trained by Zhipu.AI. Follow the user's instructions carefully. Respond using markdown.\"}, {\"role\": \"user\", \"content\": \"你好，给我讲一个故事，大概100字\"}], \"stream\": true, \"max_tokens\": 100, \"temperature\": 0.8, \"top_p\": 0.8}",
				AfterSleep: time.Millisecond * 200,
			},
		}
		var nodeList = []test.NodeAndCallback{
			{
				Node:    node1,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
					assert.Equal(t, "200", msg.Metadata.GetValue(external.StatusCodeMetadataKey))
					if msg.GetData() == "[DONE]" {
						done = true
						assert.Equal(t, "data", msg.Metadata.GetValue(external.EventTypeMetadataKey))
					}
				},
			},
			{
				Node:    node2,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Failure, relationType)
					assert.Equal(t, "404", msg.Metadata.GetValue(external.StatusCodeMetadataKey))
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}

		time.Sleep(time.Second * 1)
		assert.True(t, done)
	})

	// 测试模板变量替换
	t.Run("TemplateVariables", func(t *testing.T) {
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(fmt.Sprintf(`{"path":"%s","method":"%s"}`, r.URL.Path, r.Method)))
		}))
		defer testServer.Close()

		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"restEndpointUrlPattern": testServer.URL + "/api/${metadata.version}/users/${msg.userId}",
			"requestMethod":          "${metadata.method}",
			"body":                   "{\"name\":\"${msg.name}\",\"age\":${msg.age}}",
			"headers": map[string]string{
				"Content-Type":  "application/json",
				"Authorization": "Bearer ${metadata.token}",
			},
		}, Registry)
		assert.Nil(t, err)

		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("version", "v1")
		metaData.PutValue("method", "PUT")
		metaData.PutValue("token", "abc123")

		msg := test.Msg{
			MetaData: metaData,
			MsgType:  "USER_UPDATE",
			Data:     "{\"userId\":\"123\",\"name\":\"John\",\"age\":30}",
		}

		test.NodeOnMsgWithChildren(t, node, []test.Msg{msg}, nil, func(msg types.RuleMsg, relationType string, err error) {
			assert.Equal(t, types.Success, relationType)
			assert.Equal(t, "200", msg.Metadata.GetValue(external.StatusCodeMetadataKey))
			// 验证URL模板替换是否正确
			assert.True(t, strings.Contains(msg.GetData(), "/api/v1/users/123"))
			assert.True(t, strings.Contains(msg.GetData(), "PUT"))
		})
	})
}
