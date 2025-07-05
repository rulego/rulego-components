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

package wukongim

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/engine"
	"github.com/rulego/rulego/node_pool"
	"github.com/rulego/rulego/test/assert"
	"github.com/rulego/rulego/utils/json"
)

// TestWukongimSharedNodeBasicOperations 测试WukongIM endpoint的基本SharedNode功能
func TestWukongimSharedNodeBasicOperations(t *testing.T) {
	// 跳过测试，因为需要实际的WukongIM服务器
	t.Skip("Skipping WukongIM tests - requires actual WukongIM server")

	config := engine.NewConfig()
	pool := node_pool.NewNodePool(config)
	config.NetPool = pool

	// 子测试1：基本SharedNode功能
	t.Run("BasicSharedNodeFunctionality", func(t *testing.T) {
		var wukongimDsl = []byte(`
			{
		       "id": "shared_wukongim_endpoint",
		       "type": "endpoint/wukongim",
		       "name": "共享WukongIM端点",
		       "debugMode": false,
		       "configuration": {
		         "server": "tcp://127.0.0.1:15100",
		         "uid": "test-user",
		         "token": "test-token",
		         "connectTimeout": 5,
		         "protoVersion": 4,
		         "pingInterval": 30,
		         "reconnect": true,
		         "autoAck": true
		       }
		     }`)

		// 创建共享节点
		var def types.EndpointDsl
		err := json.Unmarshal(wukongimDsl, &def)
		assert.Nil(t, err)

		ctx, err := pool.NewFromEndpoint(def)
		assert.NotNil(t, ctx)
		assert.Nil(t, err)

		// 验证共享节点已创建
		sharedCtx, ok := pool.Get("shared_wukongim_endpoint")
		assert.True(t, ok)
		assert.NotNil(t, sharedCtx)

		// 获取WukongIM实例
		wukongimInstance, err := pool.GetInstance("shared_wukongim_endpoint")
		assert.Nil(t, err)
		assert.NotNil(t, wukongimInstance)

		// 验证实例类型和配置
		wukongimEndpoint, ok := wukongimInstance.(*Wukongim)
		assert.True(t, ok)
		assert.NotNil(t, wukongimEndpoint)
		assert.Equal(t, "tcp://127.0.0.1:15100", wukongimEndpoint.Config.Server)
		assert.Equal(t, "test-user", wukongimEndpoint.Config.UID)
		assert.Equal(t, "test-token", wukongimEndpoint.Config.Token)

		// 清理
		pool.Del("shared_wukongim_endpoint")
		assert.Equal(t, 0, len(pool.GetAll()))
	})

	// 子测试2：多实例共享验证
	t.Run("MultipleInstancesSharing", func(t *testing.T) {
		// 创建共享的WukongIM节点
		var sharedWukongimDsl = []byte(`
			{
		       "id": "shared_wukongim_server",
		       "type": "endpoint/wukongim",
		       "name": "共享WukongIM服务器",
		       "debugMode": false,
		       "configuration": {
		         "server": "tcp://127.0.0.1:15100",
		         "uid": "shared-user",
		         "token": "shared-token",
		         "connectTimeout": 10,
		         "reconnect": false
		       }
		     }`)

		var sharedDef types.EndpointDsl
		err := json.Unmarshal(sharedWukongimDsl, &sharedDef)
		assert.Nil(t, err)
		sharedCtx, err := pool.NewFromEndpoint(sharedDef)
		assert.NotNil(t, sharedCtx)
		assert.Nil(t, err)

		// 验证只有一个共享节点存在
		assert.Equal(t, 1, len(pool.GetAll()))

		// 获取共享实例
		sharedInstance, err := pool.GetInstance("shared_wukongim_server")
		assert.Nil(t, err)
		sharedWukongim, ok := sharedInstance.(*Wukongim)
		assert.True(t, ok)
		assert.Equal(t, "tcp://127.0.0.1:15100", sharedWukongim.Config.Server)

		// 验证多次获取返回同一个实例
		instance1, err := pool.GetInstance("shared_wukongim_server")
		assert.Nil(t, err)
		instance2, err := pool.GetInstance("shared_wukongim_server")
		assert.Nil(t, err)
		assert.Equal(t, instance1, instance2) // 应该是同一个实例

		// 清理
		pool.Stop()
		assert.Equal(t, 0, len(pool.GetAll()))
	})
}

// TestWukongimSharedNodeLifecycleManagement 测试WukongIM endpoint的生命周期管理
func TestWukongimSharedNodeLifecycleManagement(t *testing.T) {
	// 跳过测试，因为需要实际的WukongIM服务器
	t.Skip("Skipping WukongIM tests - requires actual WukongIM server")

	config := engine.NewConfig()
	pool := node_pool.NewNodePool(config)
	config.NetPool = pool

	// 子测试1：重启功能测试
	t.Run("RestartFunctionality", func(t *testing.T) {
		var wukongimDsl = []byte(`
			{
		       "id": "restart_test_wukongim",
		       "type": "endpoint/wukongim",
		       "name": "重启测试WukongIM端点",
		       "debugMode": false,
		       "configuration": {
		         "server": "tcp://127.0.0.1:15100",
		         "uid": "restart-user",
		         "token": "restart-token",
		         "connectTimeout": 5
		       }
		     }`)

		// 创建共享节点
		var def types.EndpointDsl
		err := json.Unmarshal(wukongimDsl, &def)
		assert.Nil(t, err)

		ctx, err := pool.NewFromEndpoint(def)
		assert.NotNil(t, ctx)
		assert.Nil(t, err)

		// 获取WukongIM实例
		wukongimInstance, err := pool.GetInstance("restart_test_wukongim")
		assert.Nil(t, err)
		_, ok := wukongimInstance.(*Wukongim)
		assert.True(t, ok)

		// 测试重启功能：删除旧节点并创建新节点
		pool.Del("restart_test_wukongim")
		time.Sleep(1 * time.Second)

		// 创建更新的配置
		var newWukongimDsl = []byte(`
			{
		       "id": "restart_test_wukongim",
		       "type": "endpoint/wukongim",
		       "name": "重启测试WukongIM端点-更新",
		       "debugMode": true,
		       "configuration": {
		         "server": "tcp://127.0.0.1:15100",
		         "uid": "restart-user-updated",
		         "token": "restart-token-updated",
		         "connectTimeout": 10,
		         "autoAck": false
		       }
		     }`)

		// 重新创建节点
		var newDef types.EndpointDsl
		err = json.Unmarshal(newWukongimDsl, &newDef)
		assert.Nil(t, err)
		newCtx, err := pool.NewFromEndpoint(newDef)
		assert.NotNil(t, newCtx)
		assert.Nil(t, err)

		// 验证配置已更新
		updatedInstance, err := pool.GetInstance("restart_test_wukongim")
		assert.Nil(t, err)
		updatedWukongim, ok := updatedInstance.(*Wukongim)
		assert.True(t, ok)
		assert.Equal(t, "restart-user-updated", updatedWukongim.Config.UID)
		assert.Equal(t, "restart-token-updated", updatedWukongim.Config.Token)
		assert.Equal(t, int64(10), updatedWukongim.Config.ConnectTimeout)
		assert.False(t, updatedWukongim.Config.AutoAck)

		// 清理
		pool.Del("restart_test_wukongim")
	})

	// 子测试2：注销影响测试
	t.Run("UnregisterImpact", func(t *testing.T) {
		var wukongimDsl = []byte(`
			{
		       "id": "unregister_test_wukongim",
		       "type": "endpoint/wukongim",
		       "name": "注销测试WukongIM端点",
		       "debugMode": false,
		       "configuration": {
		         "server": "tcp://127.0.0.1:15100",
		         "uid": "unregister-user",
		         "token": "unregister-token"
		       }
		     }`)

		// 创建共享节点
		var def types.EndpointDsl
		err := json.Unmarshal(wukongimDsl, &def)
		assert.Nil(t, err)

		ctx, err := pool.NewFromEndpoint(def)
		assert.NotNil(t, ctx)
		assert.Nil(t, err)

		// 验证节点存在
		_, ok := pool.Get("unregister_test_wukongim")
		assert.True(t, ok)
		assert.Equal(t, 1, len(pool.GetAll()))

		// 获取实例
		instance, err := pool.GetInstance("unregister_test_wukongim")
		assert.Nil(t, err)
		assert.NotNil(t, instance)

		// 注销节点
		pool.Del("unregister_test_wukongim")

		// 验证节点已被删除
		_, ok = pool.Get("unregister_test_wukongim")
		assert.False(t, ok)
		assert.Equal(t, 0, len(pool.GetAll()))

		// 尝试获取已删除的实例
		instance, err = pool.GetInstance("unregister_test_wukongim")
		assert.NotNil(t, err)
		assert.Nil(t, instance)
	})

	// 最终清理
	pool.Stop()
}

// TestWukongimSharedNodeAdvancedFeatures 测试WukongIM endpoint的高级功能
func TestWukongimSharedNodeAdvancedFeatures(t *testing.T) {
	// 跳过测试，因为需要实际的WukongIM服务器
	t.Skip("Skipping WukongIM tests - requires actual WukongIM server")

	config := engine.NewConfig()
	pool := node_pool.NewNodePool(config)
	config.NetPool = pool

	// 子测试1：路由功能测试
	t.Run("RouteFunctionality", func(t *testing.T) {
		var wukongimDsl = []byte(`
			{
		       "id": "routes_test_wukongim",
		       "type": "endpoint/wukongim",
		       "name": "路由测试WukongIM端点",
		       "debugMode": false,
		       "configuration": {
		         "server": "tcp://127.0.0.1:15100",
		         "uid": "route-user",
		         "token": "route-token"
		       }
		     }`)

		// 创建共享节点
		var def types.EndpointDsl
		err := json.Unmarshal(wukongimDsl, &def)
		assert.Nil(t, err)

		ctx, err := pool.NewFromEndpoint(def)
		assert.NotNil(t, ctx)
		assert.Nil(t, err)

		// 获取WukongIM实例
		wukongimInstance, err := pool.GetInstance("routes_test_wukongim")
		assert.Nil(t, err)
		wukongimEndpoint, ok := wukongimInstance.(*Wukongim)
		assert.True(t, ok)

		// 添加路由
		router := impl.NewRouter().From("test-channel").Transform(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
			exchange.Out.SetBody([]byte("Hello from shared WukongIM endpoint"))
			return true
		}).End()

		_, err = wukongimEndpoint.AddRouter(router)
		assert.Nil(t, err)

		// 验证路由已添加
		assert.NotNil(t, wukongimEndpoint.Router)

		// 移除路由
		err = wukongimEndpoint.RemoveRouter(router.GetId())
		assert.Nil(t, err)

		// 验证路由已移除
		assert.Nil(t, wukongimEndpoint.Router)

		// 清理
		pool.Del("routes_test_wukongim")
	})

	// 子测试2：并发访问测试
	t.Run("ConcurrentAccess", func(t *testing.T) {
		var wukongimDsl = []byte(`
			{
		       "id": "concurrent_test_wukongim",
		       "type": "endpoint/wukongim",
		       "name": "并发测试WukongIM端点",
		       "debugMode": false,
		       "configuration": {
		         "server": "tcp://127.0.0.1:15100",
		         "uid": "concurrent-user",
		         "token": "concurrent-token"
		       }
		     }`)

		// 创建共享节点
		var def types.EndpointDsl
		err := json.Unmarshal(wukongimDsl, &def)
		assert.Nil(t, err)

		ctx, err := pool.NewFromEndpoint(def)
		assert.NotNil(t, ctx)
		assert.Nil(t, err)

		// 并发获取实例
		const numGoroutines = 10
		results := make(chan interface{}, numGoroutines)
		errors := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				instance, err := pool.GetInstance("concurrent_test_wukongim")
				if err != nil {
					errors <- err
					return
				}
				results <- instance
			}()
		}

		// 收集结果
		var instances []interface{}
		for i := 0; i < numGoroutines; i++ {
			select {
			case instance := <-results:
				instances = append(instances, instance)
			case err := <-errors:
				t.Fatalf("Concurrent access failed: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for concurrent access")
			}
		}

		// 验证所有实例都是相同的（共享实例）
		assert.Equal(t, numGoroutines, len(instances))
		for i := 1; i < len(instances); i++ {
			assert.Equal(t, instances[0], instances[i])
		}

		// 清理
		pool.Del("concurrent_test_wukongim")
	})

	// 最终清理
	pool.Stop()
}

// TestWukongimSharedNodeWithRefProtocol 测试使用ref://方式引入共享WukongIM endpoint
func TestWukongimSharedNodeWithRefProtocol(t *testing.T) {
	// 跳过测试，因为需要实际的WukongIM服务器
	t.Skip("Skipping WukongIM tests - requires actual WukongIM server")

	config := engine.NewConfig()
	pool := node_pool.NewNodePool(config)
	config.NetPool = pool

	// 子测试1：基本ref://引用功能
	t.Run("BasicRefProtocol", func(t *testing.T) {
		// 创建共享节点
		var sharedWukongimDsl = []byte(`
			{
		       "id": "shared_wukongim_endpoint_ref",
		       "type": "endpoint/wukongim",
		       "name": "共享WukongIM端点-ref测试",
		       "debugMode": false,
		       "configuration": {
		         "server": "tcp://127.0.0.1:15100",
		         "uid": "ref-user",
		         "token": "ref-token"
		       }
		     }`)

		var sharedDef types.EndpointDsl
		err := json.Unmarshal(sharedWukongimDsl, &sharedDef)
		assert.Nil(t, err)

		sharedCtx, err := pool.NewFromEndpoint(sharedDef)
		assert.NotNil(t, sharedCtx)
		assert.Nil(t, err)

		// 验证共享节点已创建
		_, ok := pool.Get("shared_wukongim_endpoint_ref")
		assert.True(t, ok)

		// 测试通过ref://获取共享实例
		serverConfig := "ref://shared_wukongim_endpoint_ref"
		if strings.HasPrefix(serverConfig, "ref://") {
			instanceId := serverConfig[len("ref://"):]
			assert.Equal(t, "shared_wukongim_endpoint_ref", instanceId)

			// 从池中获取引用的实例
			sharedInstance, err := pool.GetInstance(instanceId)
			assert.Nil(t, err)
			assert.NotNil(t, sharedInstance)

			// 验证获取的是同一个共享实例
			sharedWukongim, ok := sharedInstance.(*Wukongim)
			assert.True(t, ok)
			assert.Equal(t, "tcp://127.0.0.1:15100", sharedWukongim.Config.Server)
			assert.Equal(t, "ref-user", sharedWukongim.Config.UID)
			assert.Equal(t, "ref-token", sharedWukongim.Config.Token)
		}

		// 验证ref://引用不会创建新的节点实例
		assert.Equal(t, 1, len(pool.GetAll())) // 只有一个共享节点
	})

	// 清理
	pool.Stop()
}

// TestWukongimSharedNodeIdempotencyAndSafety 测试WukongIM endpoint的幂等性和安全性
func TestWukongimSharedNodeIdempotencyAndSafety(t *testing.T) {
	// 跳过测试，因为需要实际的WukongIM服务器
	t.Skip("Skipping WukongIM tests - requires actual WukongIM server")

	config := engine.NewConfig()
	pool := node_pool.NewNodePool(config)
	config.NetPool = pool

	t.Run("MultipleCloseCalls", func(t *testing.T) {
		var wukongimDsl = []byte(`
			{
		       "id": "idempotency_test_wukongim",
		       "type": "endpoint/wukongim",
		       "name": "幂等性测试WukongIM端点",
		       "debugMode": false,
		       "configuration": {
		         "server": "tcp://127.0.0.1:15100",
		         "uid": "idempotency-user",
		         "token": "idempotency-token"
		       }
		     }`)

		var def types.EndpointDsl
		err := json.Unmarshal(wukongimDsl, &def)
		assert.Nil(t, err)

		ctx, err := pool.NewFromEndpoint(def)
		assert.NotNil(t, ctx)
		assert.Nil(t, err)

		// 获取WukongIM实例
		wukongimInstance, err := pool.GetInstance("idempotency_test_wukongim")
		assert.Nil(t, err)
		wukongimEndpoint, ok := wukongimInstance.(*Wukongim)
		assert.True(t, ok)

		// 第一次关闭
		err = wukongimEndpoint.Close()
		assert.Nil(t, err)

		// 重复关闭应该安全且无错误
		err = wukongimEndpoint.Close()
		assert.Nil(t, err)

		// 再次重复关闭
		err = wukongimEndpoint.Close()
		assert.Nil(t, err)

		// 清理
		pool.Del("idempotency_test_wukongim")
	})

	t.Run("ConcurrentCloseCalls", func(t *testing.T) {
		var wukongimDsl = []byte(`
			{
		       "id": "concurrent_close_test_wukongim",
		       "type": "endpoint/wukongim",
		       "name": "并发关闭测试WukongIM端点",
		       "debugMode": false,
		       "configuration": {
		         "server": "tcp://127.0.0.1:15100",
		         "uid": "concurrent-close-user",
		         "token": "concurrent-close-token"
		       }
		     }`)

		var def types.EndpointDsl
		err := json.Unmarshal(wukongimDsl, &def)
		assert.Nil(t, err)

		ctx, err := pool.NewFromEndpoint(def)
		assert.NotNil(t, ctx)
		assert.Nil(t, err)

		// 获取WukongIM实例
		wukongimInstance, err := pool.GetInstance("concurrent_close_test_wukongim")
		assert.Nil(t, err)
		wukongimEndpoint, ok := wukongimInstance.(*Wukongim)
		assert.True(t, ok)

		// 并发关闭测试
		const numGoroutines = 5
		var wg sync.WaitGroup
		errChan := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				err := wukongimEndpoint.Close()
				if err != nil {
					errChan <- fmt.Errorf("goroutine %d: %v", index, err)
				}
			}(i)
		}

		wg.Wait()
		close(errChan)

		// 验证没有错误
		for err := range errChan {
			t.Errorf("Concurrent close error: %v", err)
		}

		// 清理
		pool.Del("concurrent_close_test_wukongim")
	})

	// 最终清理
	pool.Stop()
}

// TestWukongimEndpointConfiguration 测试WukongIM endpoint的配置处理
func TestWukongimEndpointConfiguration(t *testing.T) {
	config := engine.NewConfig()

	t.Run("DefaultConfiguration", func(t *testing.T) {
		wukongimEndpoint := &Wukongim{}
		newInstance := wukongimEndpoint.New()
		newWukongim, ok := newInstance.(*Wukongim)
		assert.True(t, ok)

		// 验证默认配置
		assert.Equal(t, "tcp://175.27.245.108:15100", newWukongim.Config.Server)
		assert.Equal(t, "test1", newWukongim.Config.UID)
		assert.Equal(t, "test1", newWukongim.Config.Token)
		assert.Equal(t, int64(5), newWukongim.Config.ConnectTimeout)
		assert.Equal(t, int64(30), newWukongim.Config.PingInterval)
		assert.True(t, newWukongim.Config.Reconnect)
		assert.True(t, newWukongim.Config.AutoAck)
	})

	t.Run("CustomConfiguration", func(t *testing.T) {
		wukongimEndpoint := &Wukongim{}

		configuration := types.Configuration{
			"server":         "tcp://custom.server:15100",
			"uid":            "custom-user",
			"token":          "custom-token",
			"connectTimeout": int64(10),
			"protoVersion":   5,
			"pingInterval":   int64(60),
			"reconnect":      false,
			"autoAck":        false,
		}

		err := wukongimEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// 验证自定义配置
		assert.Equal(t, "tcp://custom.server:15100", wukongimEndpoint.Config.Server)
		assert.Equal(t, "custom-user", wukongimEndpoint.Config.UID)
		assert.Equal(t, "custom-token", wukongimEndpoint.Config.Token)
		assert.Equal(t, int64(10), wukongimEndpoint.Config.ConnectTimeout)
		assert.Equal(t, 5, wukongimEndpoint.Config.ProtoVersion)
		assert.Equal(t, int64(60), wukongimEndpoint.Config.PingInterval)
		assert.False(t, wukongimEndpoint.Config.Reconnect)
		assert.False(t, wukongimEndpoint.Config.AutoAck)
	})

	t.Run("TypeAndIdMethods", func(t *testing.T) {
		wukongimEndpoint := &Wukongim{}

		configuration := types.Configuration{
			"server": "tcp://test.server:15100",
		}

		err := wukongimEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// 验证类型和ID方法
		assert.Equal(t, "endpoint/wukongim", wukongimEndpoint.Type())
		assert.Equal(t, "tcp://test.server:15100", wukongimEndpoint.Id())
	})
}

// createWukongimTestRouter 创建测试路由
func createWukongimTestRouter(messageType string) endpointApi.Router {
	return impl.NewRouter().From(messageType).To("chain:wukongim-test").End()
}
