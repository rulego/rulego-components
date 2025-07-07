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
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test/assert"
)

// TestFastHttpAsyncDebugLog 测试异步请求时的调试日志问题
func TestFastHttpAsyncDebugLog(t *testing.T) {
	// 用于统计调试日志的计数器
	var debugCount int64

	// 记录调试日志的函数
	debugFunc := func(chainId, flowType string, nodeId string, msg types.RuleMsg, relationType string, err error) {
		atomic.AddInt64(&debugCount, 1)
	}

	// 测试异步请求（wait: false）
	t.Run("AsyncRequest", func(t *testing.T) {
		// 重置计数器
		atomic.StoreInt64(&debugCount, 0)

		// 创建异步DSL配置
		asyncDSL := `{
			"ruleChain": {
				"id": "fasthttp_async_debug_test",
				"name": "FastHttp异步调试测试链",
				"root": true,
				"debugMode": true
			},
			"metadata": {
				"endpoints": [
					{
						"id": "fasthttp_async_endpoint",
						"type": "endpoint/http",
						"name": "FastHttp异步服务器",
						"configuration": {
							"server": ":9098",
							"allowCors": true
						},
						"routers": [
							{
								"id": "async_router",
								"params": ["POST"],
								"from": {
									"path": "/api/v1/async"
								},
								"to": {
									"path": "fasthttp_async_debug_test:async_processor",
									"wait": false
								}
							}
						]
					}
				],
				"nodes": [
					{
						"id": "async_processor",
						"type": "jsTransform",
						"name": "异步处理器",
						"configuration": {
							"jsScript": "var result = {\n  message: '异步处理完成',\n  timestamp: new Date().toISOString(),\n  inputData: JSON.parse(msg)\n};\nreturn {'msg': result, 'metadata': metadata, 'msgType': msgType};"
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
			types.WithOnDebug(debugFunc),
		)

		// 创建规则引擎
		ruleEngine, err := rulego.New("fasthttp_async_debug_test", []byte(asyncDSL), types.WithConfig(config))
		assert.Nil(t, err)
		if ruleEngine == nil {
			t.Fatal("创建规则引擎失败")
		}
		// 等待服务启动
		time.Sleep(time.Second * 2)

		// 发送异步请求
		payload := `{"test": "async_data", "id": 1}`
		resp, err := http.Post("http://localhost:9098/api/v1/async", "application/json", strings.NewReader(payload))
		if err != nil {
			t.Logf("异步请求失败: %v", err)
		} else {
			defer resp.Body.Close()
		}

		// 等待异步处理完成
		time.Sleep(time.Second * 3)

		// 检查调试日志
		finalCount := atomic.LoadInt64(&debugCount)

		// 验证是否有调试日志，1个node会产生两条（In/Out）
		assert.Equal(t, int64(2), finalCount, "异步请求未产生预期的调试日志数量")

		// 清理资源
		ruleEngine.Stop(context.Background())
	})

	// 测试同步请求（wait: true）作为对照
	t.Run("SyncRequest", func(t *testing.T) {
		// 重置计数器
		atomic.StoreInt64(&debugCount, 0)

		// 创建同步DSL配置
		syncDSL := `{
			"ruleChain": {
				"id": "fasthttp_sync_debug_test",
				"name": "FastHttp同步调试测试链",
				"root": true,
				"debugMode": true
			},
			"metadata": {
				"endpoints": [
					{
						"id": "fasthttp_sync_endpoint",
						"type": "endpoint/http",
						"name": "FastHttp同步服务器",
						"configuration": {
							"server": ":9099",
							"allowCors": true
						},
						"routers": [
							{
								"id": "sync_router",
								"params": ["POST"],
								"from": {
									"path": "/api/v1/sync"
								},
								"to": {
									"path": "fasthttp_sync_debug_test:sync_processor",
									"wait": true,
									"processors": ["responseToBody"]
								}
							}
						]
					}
				],
				"nodes": [
					{
						"id": "sync_processor",
						"type": "jsTransform",
						"name": "同步处理器",
						"configuration": {
							"jsScript": "var result = {\n  message: '同步处理完成',\n  timestamp: new Date().toISOString(),\n  inputData: JSON.parse(msg)\n};\nreturn {'msg': result, 'metadata': metadata, 'msgType': msgType};"
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
			types.WithOnDebug(debugFunc),
		)

		// 创建规则引擎
		ruleEngine, err := rulego.New("fasthttp_sync_debug_test", []byte(syncDSL), types.WithConfig(config))
		assert.Nil(t, err)
		if ruleEngine == nil {
			t.Fatal("创建规则引擎失败")
		}

		// 等待服务启动
		time.Sleep(time.Second * 2)

		// 发送同步请求
		payload := `{"test": "sync_data", "id": 1}`
		resp, err := http.Post("http://localhost:9099/api/v1/sync", "application/json", strings.NewReader(payload))
		if err != nil {
			t.Logf("同步请求失败: %v", err)
		} else {
			defer resp.Body.Close()
		}

		// 等待处理完成
		time.Sleep(time.Second * 1)

		// 检查调试日志
		finalCount := atomic.LoadInt64(&debugCount)

		// 验证是否有调试日志，1个node会产生两条（In/Out）
		assert.Equal(t, int64(2), finalCount, "同步请求未产生预期的调试日志数量")

		// 清理资源
		ruleEngine.Stop(context.Background())
	})
}

// TestFastHttpConcurrentAsyncDebugLog 测试并发异步请求时的调试日志问题
func TestFastHttpConcurrentAsyncDebugLog(t *testing.T) {
	// 用于统计调试日志的计数器
	var debugCount int64

	// 记录调试日志的函数
	debugFunc := func(chainId, flowType string, nodeId string, msg types.RuleMsg, relationType string, err error) {
		atomic.AddInt64(&debugCount, 1)
	}

	// 创建并发异步DSL配置
	concurrentDSL := `{
		"ruleChain": {
			"id": "fasthttp_concurrent_debug_test",
			"name": "FastHttp并发调试测试链",
			"root": true,
			"debugMode": true
		},
		"metadata": {
			"endpoints": [
				{
					"id": "fasthttp_concurrent_endpoint",
					"type": "endpoint/http",
					"name": "FastHttp并发服务器",
					"configuration": {
						"server": ":9100",
						"allowCors": true
					},
					"routers": [
						{
							"id": "concurrent_router",
							"params": ["POST"],
							"from": {
								"path": "/api/v1/concurrent"
							},
							"to": {
								"path": "fasthttp_concurrent_debug_test:concurrent_processor",
								"wait": false
							}
						}
					]
				}
			],
			"nodes": [
				{
					"id": "concurrent_processor",
					"type": "jsTransform",
					"name": "并发处理器",
					"configuration": {
						"jsScript": "var result = {\n  message: '并发处理完成',\n  timestamp: new Date().toISOString(),\n  inputData: JSON.parse(msg)\n};\nreturn {'msg': result, 'metadata': metadata, 'msgType': msgType};"
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
		types.WithOnDebug(debugFunc),
	)

	// 创建规则引擎
	ruleEngine, err := rulego.New("fasthttp_concurrent_debug_test", []byte(concurrentDSL), types.WithConfig(config))
	assert.Nil(t, err)
	if ruleEngine == nil {
		t.Fatal("创建规则引擎失败")
	}

	// 等待服务启动
	time.Sleep(time.Second * 2)

	// 并发发送异步请求
	const concurrentCount = 10
	var wg sync.WaitGroup

	for i := 0; i < concurrentCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			payload := fmt.Sprintf(`{"test": "concurrent_data", "id": %d}`, id)
			resp, err := http.Post("http://localhost:9100/api/v1/concurrent", "application/json", strings.NewReader(payload))
			if err != nil {
				t.Logf("并发请求[%d]失败: %v", id, err)
			} else {
				defer resp.Body.Close()
			}
		}(i)
	}

	wg.Wait()

	// 等待异步处理完成
	time.Sleep(time.Second * 5)

	// 检查调试日志
	finalCount := atomic.LoadInt64(&debugCount)

	// 验证调试日志数量，每个请求应该有2个调试日志: In和Out
	assert.Equal(t, int64(concurrentCount*2), finalCount, "并发异步请求未产生预期的调试日志数量")

	// 清理资源
	ruleEngine.Stop(context.Background())
}
