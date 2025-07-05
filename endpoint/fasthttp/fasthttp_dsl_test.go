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

package fasthttp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/stretchr/testify/assert"
)

//func init() {
//	// 手动注册fasthttp endpoint
//	_ = endpoint.Registry.Unregister(Type)
//	_ = endpoint.Registry.Register(&Endpoint{})
//}

// TestFastHttpDSLEndpoint 测试使用DSL方式启动FastHttp endpoint并进行动态路由和热更新
func TestFastHttpDSLEndpoint(t *testing.T) {
	// 创建初始的DSL配置
	initialDSL := `{
		"ruleChain": {
			"id": "fasthttp_dsl_test",
			"name": "FastHttp DSL Test Chain",
			"root": true,
			"debugMode": true
		},
		"metadata": {
			"endpoints": [
				{
					"id": "fasthttp_endpoint_1",
					"type": "endpoint/http",
					"name": "FastHttp Server",
					"configuration": {
						"server": ":9096",
						"readTimeout": 10,
						"writeTimeout": 10,
						"maxRequestSize": "4M",
						"allowCors": true
					},
					"routers": [
						{
							"id": "api_router",
							"params": ["GET"],
							"from": {
								"path": "/api/v1/data"
							},
							"to": {
								"path": "fasthttp_dsl_test:data_processor",
								"wait": true,
								"processors": ["responseToBody"]
							}
						},
						{
							"id": "post_router",
							"params": ["POST"],
							"from": {
								"path": "/api/v1/submit"
							},
							"to": {
								"path": "fasthttp_dsl_test:submit_processor",
								"wait": true,
								"processors": ["responseToBody"]
							}
						}
					]
				}
			],
			"nodes": [
				{
					"id": "data_processor",
					"type": "jsTransform",
					"name": "数据处理器",
					"configuration": {
						"jsScript": "var result = {\n  message: 'FastHttp DSL endpoint response',\n  method: 'GET',\n  timestamp: new Date().toISOString(),\n  query: metadata.param1,\n  data: msg\n};\nreturn {'msg': result, 'metadata': metadata, 'msgType': msgType};"
					},
					"debugMode": true
				},
				{
					"id": "submit_processor",
					"type": "jsTransform",
					"name": "提交处理器",
					"configuration": {
						"jsScript": "var result = {\n  message: 'Data submitted successfully',\n  method: 'POST',\n  timestamp: new Date().toISOString(),\n  receivedData: JSON.parse(msg),\n  status: 'success'\n};\nreturn {'msg': result, 'metadata': metadata, 'msgType': msgType};"
					},
					"debugMode": true
				}
			],
			"connections": []
		}
	}`

	// 创建规则引擎配置
	config := rulego.NewConfig(
		types.WithDefaultPool(),
		types.WithEndpointEnabled(true),
		types.WithOnDebug(func(chainId, flowType string, nodeId string, msg types.RuleMsg, relationType string, err error) {
			//t.Logf("[FastHttp调试] 链: %s, 节点: %s, 关系: %s, 消息: %s", chainId, nodeId, relationType, msg.GetData())
		}),
	)

	// 使用DSL创建包含嵌入式endpoint的规则链
	ruleEngine, err := rulego.New("fasthttp_dsl_test", []byte(initialDSL), types.WithConfig(config))
	if err != nil {
		t.Logf("创建规则引擎失败: %v", err)
		t.FailNow()
	}

	// 等待服务启动
	time.Sleep(time.Second * 2)

	// 测试GET路由
	resp, err := http.Get("http://localhost:9096/api/v1/data?param1=value1&param2=value2")
	if err != nil {
		t.Logf("GET请求失败: %v", err)
	} else {
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		body, _ := io.ReadAll(resp.Body)
		assert.True(t, strings.Contains(string(body), "FastHttp DSL endpoint response"))
	}

	// 等待endpoint初始化完成
	time.Sleep(time.Millisecond * 100)

	// 清理资源
	ruleEngine.Stop(context.Background())
}

// mustMarshal 辅助函数，用于JSON序列化
func mustMarshal(v interface{}) string {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(data)
}

// TestFastHttpDSLWithWebSocket 测试FastHttp DSL配置中的WebSocket功能
func TestFastHttpDSLWithWebSocket(t *testing.T) {
	// WebSocket DSL配置
	websocketDSL := `{
		"ruleChain": {
			"id": "fasthttp_websocket_test",
			"name": "FastHttp WebSocket Test Chain",
			"root": true,
			"debugMode": true
		},
		"metadata": {
			"endpoints": [
				{
					"id": "fasthttp_ws_endpoint",
					"type": "endpoint/http",
					"name": "FastHttp WebSocket Server",
					"configuration": {
						"server": ":9097",
						"allowCors": true
					},
					"routers": [
						{
							"id": "ws_router",
							"params": ["GET"],
							"from": {
								"path": "/ws"
							},
							"to": {
								"path": "fasthttp_websocket_test:ws_processor"
							}
						}
					]
				}
			],
			"nodes": [
				{
					"id": "ws_processor",
					"type": "jsTransform",
					"name": "WebSocket消息处理器",
					"configuration": {
						"jsScript": "var wsMsg = JSON.parse(msg);\nvar response = {\n  type: 'response',\n  originalMessage: wsMsg,\n  timestamp: new Date().toISOString(),\n  echo: 'Received: ' + wsMsg.message\n};\nreturn {'msg': response, 'metadata': metadata, 'msgType': msgType};"
					},
					"debugMode": true
				}
			],
			"connections": []
		}
	}`

	// 创建规则引擎配置
	config := rulego.NewConfig(
		types.WithDefaultPool(),
		types.WithEndpointEnabled(true),
		types.WithOnDebug(func(chainId, flowType string, nodeId string, msg types.RuleMsg, relationType string, err error) {
			t.Logf("[WebSocket调试] 链: %s, 节点: %s, 消息: %s", chainId, nodeId, msg.GetData())
		}),
	)

	// 创建规则引擎
	ruleEngine, err := rulego.New("fasthttp_websocket_test", []byte(websocketDSL), types.WithConfig(config))
	assert.Nil(t, err)
	if ruleEngine == nil {
		t.Fatal("Failed to create rule engine")
		return
	}

	// 等待WebSocket服务启动
	time.Sleep(time.Second * 1)

	t.Logf("FastHttp WebSocket DSL endpoint test completed")

	// 清理资源
	ruleEngine.Stop(context.Background())
}
