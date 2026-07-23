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

// TestFastHttpAsyncDebugLog Issues with debug logs during asynchronous requests
func TestFastHttpAsyncDebugLog(t *testing.T) {
	// A counter used for tallying debug logs
	var debugCount int64

	// A function that records debugging logs
	debugFunc := func(chainId, flowType string, nodeId string, msg types.RuleMsg, relationType string, err error) {
		atomic.AddInt64(&debugCount, 1)
	}

	// Test asynchronous request (wait: false)
	t.Run("AsyncRequest", func(t *testing.T) {
		// Reset the counter
		atomic.StoreInt64(&debugCount, 0)

		// Create an asynchronous DSL configuration
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

		// Create rule engine configurations
		config := rulego.NewConfig(
			types.WithDefaultPool(),
			types.WithEndpointEnabled(true),
			types.WithOnDebug(debugFunc),
		)

		// Create a rule engine
		ruleEngine, err := rulego.New("fasthttp_async_debug_test", []byte(asyncDSL), types.WithConfig(config))
		assert.Nil(t, err)
		if ruleEngine == nil {
			t.Fatal("Failure to create a rule engine")
		}
		// Waiting for the service to start
		time.Sleep(time.Second * 2)

		// Send asynchronous requests
		payload := `{"test": "async_data", "id": 1}`
		resp, err := http.Post("http://localhost:9098/api/v1/async", "application/json", strings.NewReader(payload))
		if err != nil {
			t.Logf("Asynchronous request failed: %v", err)
		} else {
			defer resp.Body.Close()
		}

		// Wait for the asynchronous processing to complete
		time.Sleep(time.Second * 3)

		// Check the debugging log
		finalCount := atomic.LoadInt64(&debugCount)

		// Verify whether there is a debug log; one node generates two (In/Out) entries.
		assert.Equal(t, int64(2), finalCount, "异步请求未产生预期的调试日志数量")

		// Release resources
		ruleEngine.Stop(context.Background())
	})

	// Test synchronization request (wait: true) as a reference
	t.Run("SyncRequest", func(t *testing.T) {
		// Reset the counter
		atomic.StoreInt64(&debugCount, 0)

		// Create a synchronized DSL configuration
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

		// Create rule engine configurations
		config := rulego.NewConfig(
			types.WithDefaultPool(),
			types.WithEndpointEnabled(true),
			types.WithOnDebug(debugFunc),
		)

		// Create a rule engine
		ruleEngine, err := rulego.New("fasthttp_sync_debug_test", []byte(syncDSL), types.WithConfig(config))
		assert.Nil(t, err)
		if ruleEngine == nil {
			t.Fatal("Failure to create a rule engine")
		}

		// Waiting for the service to start
		time.Sleep(time.Second * 2)

		// Send a synchronization request
		payload := `{"test": "sync_data", "id": 1}`
		resp, err := http.Post("http://localhost:9099/api/v1/sync", "application/json", strings.NewReader(payload))
		if err != nil {
			t.Logf("Synchronization request failure: %v", err)
		} else {
			defer resp.Body.Close()
		}

		// Wait for processing to complete
		time.Sleep(time.Second * 1)

		// Check the debugging log
		finalCount := atomic.LoadInt64(&debugCount)

		// Verify whether there is a debug log; one node generates two (In/Out) entries.
		assert.Equal(t, int64(2), finalCount, "同步请求未产生预期的调试日志数量")

		// Release resources
		ruleEngine.Stop(context.Background())
	})
}

// TestFastHttpConcurrentAsyncDebugLog Tests debug log issues when concurrently making asynchronous requests
func TestFastHttpConcurrentAsyncDebugLog(t *testing.T) {
	// A counter used for tallying debug logs
	var debugCount int64

	// A function that records debugging logs
	debugFunc := func(chainId, flowType string, nodeId string, msg types.RuleMsg, relationType string, err error) {
		atomic.AddInt64(&debugCount, 1)
	}

	// Create concurrent asynchronous DSL configurations
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

	// Create rule engine configurations
	config := rulego.NewConfig(
		types.WithDefaultPool(),
		types.WithEndpointEnabled(true),
		types.WithOnDebug(debugFunc),
	)

	// Create a rule engine
	ruleEngine, err := rulego.New("fasthttp_concurrent_debug_test", []byte(concurrentDSL), types.WithConfig(config))
	assert.Nil(t, err)
	if ruleEngine == nil {
		t.Fatal("Failure to create a rule engine")
	}

	// Waiting for the service to start
	time.Sleep(time.Second * 2)

	// Send asynchronous requests concurrently
	const concurrentCount = 10
	var wg sync.WaitGroup

	for i := 0; i < concurrentCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			payload := fmt.Sprintf(`{"test": "concurrent_data", "id": %d}`, id)
			resp, err := http.Post("http://localhost:9100/api/v1/concurrent", "application/json", strings.NewReader(payload))
			if err != nil {
				t.Logf("Concurrent request [%d] failed: %v", id, err)
			} else {
				defer resp.Body.Close()
			}
		}(i)
	}

	wg.Wait()

	// Wait for the asynchronous processing to complete
	time.Sleep(time.Second * 5)

	// Check the debugging log
	finalCount := atomic.LoadInt64(&debugCount)

	// Verify the number of debug logs; each request should have 2 debug logs: In and Out
	assert.Equal(t, int64(concurrentCount*2), finalCount, "并发异步请求未产生预期的调试日志数量")

	// Release resources
	ruleEngine.Stop(context.Background())
}
