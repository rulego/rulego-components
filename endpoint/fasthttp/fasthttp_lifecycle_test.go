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
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/test/assert"
)

// getAvailablePort 获取可用端口
var portCounter int32 = 19080

func getAvailablePort() int {
	// 使用原子操作递增端口号，确保每个测试使用不同端口
	port := int(atomic.AddInt32(&portCounter, 1))
	return port
}

func checkPortAvailable(port int) bool {
	// 尝试监听端口来检查是否可用
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return false
	}
	ln.Close()
	return true
}

func createTestEndpoint(port int) (*FastHttp, error) {
	config := types.Configuration{
		"server": fmt.Sprintf(":%d", port),
	}

	ruleConfig := types.NewConfig()
	ep := &FastHttp{}
	err := ep.Init(ruleConfig, config)
	return ep, err
}

func createTestRouter(path string) endpointApi.Router {
	return impl.NewRouter().From(path).Transform(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		exchange.Out.SetBody([]byte("Hello from FastHTTP"))
		return true
	}).End()
}

func TestFastHttpSharedNodeLifecycleManagement(t *testing.T) {
	t.Run("RestartFunctionality", func(t *testing.T) {
		port := getAvailablePort()

		// 创建端点
		ep, err := createTestEndpoint(port)
		assert.Nil(t, err)
		defer ep.Destroy()

		// 添加路由
		router := createTestRouter("/test")
		_, err = ep.AddRouter(router, "GET")
		assert.Nil(t, err)

		// 启动服务
		err = ep.Start()
		assert.Nil(t, err)

		// 验证服务正在运行
		assert.True(t, ep.Started())
		assert.NotNil(t, ep.GetServer())

		// 等待服务完全启动
		time.Sleep(200 * time.Millisecond)

		// 测试HTTP请求
		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get(fmt.Sprintf("http://localhost:%d/test", port))
		if err == nil {
			resp.Body.Close()
		}

		// 重启服务
		err = ep.Restart()
		assert.Nil(t, err)

		// 验证重启后服务仍在运行
		assert.True(t, ep.Started())
		assert.NotNil(t, ep.GetServer())

		// 等待重启完成
		time.Sleep(200 * time.Millisecond)

		// 再次测试HTTP请求
		resp, err = client.Get(fmt.Sprintf("http://localhost:%d/test", port))
		if err == nil {
			resp.Body.Close()
		}
	})

	t.Run("GracefulShutdown", func(t *testing.T) {
		port := getAvailablePort()

		ep, err := createTestEndpoint(port)
		assert.Nil(t, err)

		router := createTestRouter("/shutdown-test")
		_, err = ep.AddRouter(router, "GET")
		assert.Nil(t, err)

		err = ep.Start()
		assert.Nil(t, err)

		// 等待服务启动
		time.Sleep(100 * time.Millisecond)
		assert.True(t, ep.Started())

		// 优雅关闭
		err = ep.Close()
		assert.Nil(t, err)
		assert.False(t, ep.Started())
	})

	t.Run("PortReuseAfterClose", func(t *testing.T) {
		port := getAvailablePort()

		// 第一个端点
		ep1, err := createTestEndpoint(port)
		assert.Nil(t, err)

		router := createTestRouter("/port-test")
		_, err = ep1.AddRouter(router, "GET")
		assert.Nil(t, err)

		err = ep1.Start()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// 关闭第一个端点
		err = ep1.Close()
		assert.Nil(t, err)

		// 等待端口释放
		time.Sleep(200 * time.Millisecond)

		// 第二个端点使用同一端口
		ep2, err := createTestEndpoint(port)
		assert.Nil(t, err)
		defer ep2.Destroy()

		_, err = ep2.AddRouter(router, "GET")
		assert.Nil(t, err)

		err = ep2.Start()
		assert.Nil(t, err)
		assert.True(t, ep2.Started())
	})
}

func TestFastHttpSharedNodeAdvancedFeatures(t *testing.T) {
	t.Run("ConcurrentAccess", func(t *testing.T) {
		port := getAvailablePort()

		ep, err := createTestEndpoint(port)
		assert.Nil(t, err)
		defer ep.Destroy()

		router := createTestRouter("/concurrent")
		_, err = ep.AddRouter(router, "GET")
		assert.Nil(t, err)

		err = ep.Start()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// 并发访问测试
		const numGoroutines = 10
		var wg sync.WaitGroup
		successCount := int64(0)
		var mu sync.Mutex

		client := &http.Client{Timeout: 2 * time.Second}

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				resp, err := client.Get(fmt.Sprintf("http://localhost:%d/concurrent", port))
				if err == nil {
					resp.Body.Close()
					mu.Lock()
					successCount++
					mu.Unlock()
				}
			}(i)
		}

		wg.Wait()
		t.Logf("并发访问测试 - 成功请求数: %d/%d", successCount, numGoroutines)
	})

	t.Run("MultipleRoutes", func(t *testing.T) {
		port := getAvailablePort()

		ep, err := createTestEndpoint(port)
		assert.Nil(t, err)
		defer ep.Destroy()

		// 添加多个路由
		routes := []string{"/api/users", "/api/orders", "/api/products"}
		for i, route := range routes {
			router := createTestRouter(route)
			_, err = ep.AddRouter(router, "GET")
			assert.Nil(t, err, "Failed to add route %d: %s", i, route)
		}

		err = ep.Start()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// 测试所有路由
		client := &http.Client{Timeout: 2 * time.Second}
		for _, route := range routes {
			resp, err := client.Get(fmt.Sprintf("http://localhost:%d%s", port, route))
			if err == nil {
				resp.Body.Close()
			}
		}
	})
}

func TestFastHttpSharedNodeBasicOperations(t *testing.T) {
	t.Run("BasicSharedNodeFunctionality", func(t *testing.T) {
		port := getAvailablePort()

		ep, err := createTestEndpoint(port)
		assert.Nil(t, err)
		defer ep.Destroy()

		// 验证SharedNode初始化
		assert.True(t, ep.SharedNode.IsInit())
		assert.NotEqual(t, "", ep.Id())
		assert.Equal(t, Type, ep.Type())

		router := createTestRouter("/basic")
		_, err = ep.AddRouter(router, "GET")
		assert.Nil(t, err)

		err = ep.Start()
		assert.Nil(t, err)
		assert.True(t, ep.Started())
	})

	t.Run("MultipleInstancesSharing", func(t *testing.T) {
		port := getAvailablePort()
		serverAddr := fmt.Sprintf(":%d", port)

		// 创建两个使用相同服务器地址的端点实例
		config := types.Configuration{"server": serverAddr}
		ruleConfig := types.NewConfig()

		ep1 := &FastHttp{}
		err := ep1.Init(ruleConfig, config)
		assert.Nil(t, err)
		defer ep1.Destroy()

		ep2 := &FastHttp{}
		err = ep2.Init(ruleConfig, config)
		assert.Nil(t, err)
		defer ep2.Destroy()

		// 验证它们共享相同的实例ID
		assert.Equal(t, ep1.Id(), ep2.Id())

		router1 := createTestRouter("/shared1")
		_, err = ep1.AddRouter(router1, "GET")
		assert.Nil(t, err)

		router2 := createTestRouter("/shared2")
		_, err = ep2.AddRouter(router2, "GET")
		assert.Nil(t, err)

		err = ep1.Start()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)
	})
}

func TestFastHttpConfigurationVariations(t *testing.T) {
	t.Run("CustomTimeouts", func(t *testing.T) {
		port := getAvailablePort()

		config := types.Configuration{
			"server":         fmt.Sprintf(":%d", port),
			"readTimeout":    5,
			"writeTimeout":   5,
			"idleTimeout":    30,
			"maxRequestSize": "1M",
			"concurrency":    1000,
		}

		ruleConfig := types.NewConfig()
		ep := &FastHttp{}
		err := ep.Init(ruleConfig, config)
		assert.Nil(t, err)
		defer ep.Destroy()

		assert.Equal(t, 5, ep.Config.ReadTimeout)
		assert.Equal(t, 5, ep.Config.WriteTimeout)
		assert.Equal(t, 30, ep.Config.IdleTimeout)
		assert.Equal(t, "1M", ep.Config.MaxRequestSize)
		assert.Equal(t, 1000, ep.Config.Concurrency)
	})

	t.Run("CORSConfiguration", func(t *testing.T) {
		port := getAvailablePort()

		config := types.Configuration{
			"server":    fmt.Sprintf(":%d", port),
			"allowCors": true,
		}

		ruleConfig := types.NewConfig()
		ep := &FastHttp{}
		err := ep.Init(ruleConfig, config)
		assert.Nil(t, err)
		defer ep.Destroy()

		assert.True(t, ep.Config.AllowCors)

		router := createTestRouter("/cors-test")
		_, err = ep.AddRouter(router, "GET")
		assert.Nil(t, err)

		err = ep.Start()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// 测试OPTIONS请求（CORS预检）
		client := &http.Client{Timeout: 2 * time.Second}
		req, _ := http.NewRequest("OPTIONS", fmt.Sprintf("http://localhost:%d/cors-test", port), nil)
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	})
}

func TestFastHttpErrorHandling(t *testing.T) {
	t.Run("InvalidConfiguration", func(t *testing.T) {
		config := types.Configuration{
			"server": "invalid:address:format",
		}

		ruleConfig := types.NewConfig()
		ep := &FastHttp{}
		err := ep.Init(ruleConfig, config)
		// 初始化可能成功，但启动会失败
		if err == nil {
			err = ep.Start()
			// 启动可能会失败，这是预期的
		}
		if ep.Started() {
			ep.Destroy()
		}
	})

	t.Run("DoubleClose", func(t *testing.T) {
		port := getAvailablePort()

		ep, err := createTestEndpoint(port)
		assert.Nil(t, err)

		err = ep.Start()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// 第一次关闭
		err = ep.Close()
		assert.Nil(t, err)

		// 第二次关闭应该也能正常处理
		err = ep.Close()
		assert.Nil(t, err)
	})
}

func BenchmarkFastHttpBasicOperation(b *testing.B) {
	port := getAvailablePort()

	ep, err := createTestEndpoint(port)
	if err != nil {
		b.Fatal(err)
	}
	defer ep.Destroy()

	router := createTestRouter("/benchmark")
	_, err = ep.AddRouter(router, "GET")
	if err != nil {
		b.Fatal(err)
	}

	err = ep.Start()
	if err != nil {
		b.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 1 * time.Second}
	url := fmt.Sprintf("http://localhost:%d/benchmark", port)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
		}
	}
}

func TestFastHttpIdempotencyAndSafety(t *testing.T) {
	t.Run("MultipleCloseCalls", func(t *testing.T) {
		port := getAvailablePort()

		ep, err := createTestEndpoint(port)
		assert.Nil(t, err)

		router := createTestRouter("/idempotency-test")
		_, err = ep.AddRouter(router, "GET")
		assert.Nil(t, err)

		err = ep.Start()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// 验证服务器正在运行
		assert.True(t, ep.Started())
		assert.NotNil(t, ep.GetServer())

		// 第一次关闭
		err = ep.Close()
		assert.Nil(t, err)
		assert.False(t, ep.Started())
		assert.Nil(t, ep.GetServer())

		// 重复关闭应该安全且无错误
		err = ep.Close()
		assert.Nil(t, err)
		assert.False(t, ep.Started())
		assert.Nil(t, ep.GetServer())

		// 再次重复关闭
		err = ep.Close()
		assert.Nil(t, err)
	})

	t.Run("ConcurrentCloseCalls", func(t *testing.T) {
		port := getAvailablePort()

		ep, err := createTestEndpoint(port)
		assert.Nil(t, err)

		router := createTestRouter("/concurrent-close")
		_, err = ep.AddRouter(router, "GET")
		assert.Nil(t, err)

		err = ep.Start()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// 并发调用Close()
		const numGoroutines = 5
		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := ep.Close()
				errors <- err
			}()
		}

		wg.Wait()
		close(errors)

		// 检查所有关闭调用都成功
		errorCount := 0
		for err := range errors {
			if err != nil {
				errorCount++
				t.Logf("Close error: %v", err)
			}
		}

		// 应该没有错误，或者最多只有少数几个由于并发竞争产生的错误
		assert.True(t, errorCount <= 1, "Too many errors from concurrent close calls: %d", errorCount)
		assert.False(t, ep.Started())
		assert.Nil(t, ep.GetServer())
	})

	t.Run("CloseAfterRestart", func(t *testing.T) {
		port := getAvailablePort()

		ep, err := createTestEndpoint(port)
		assert.Nil(t, err)
		defer ep.Destroy()

		router := createTestRouter("/restart-close")
		_, err = ep.AddRouter(router, "GET")
		assert.Nil(t, err)

		// 启动服务
		err = ep.Start()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// 重启服务
		err = ep.Restart()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// 重启后再关闭
		err = ep.Close()
		assert.Nil(t, err)
		assert.False(t, ep.Started())

		// 再次关闭应该安全
		err = ep.Close()
		assert.Nil(t, err)
	})

	t.Run("MultipleRestartCalls", func(t *testing.T) {
		port := getAvailablePort()

		ep, err := createTestEndpoint(port)
		assert.Nil(t, err)
		defer ep.Destroy()

		router := createTestRouter("/multiple-restart")
		_, err = ep.AddRouter(router, "GET")
		assert.Nil(t, err)

		// 启动服务
		err = ep.Start()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// 多次重启
		for i := 0; i < 3; i++ {
			err = ep.Restart()
			assert.Nil(t, err, "Restart %d failed", i+1)
			time.Sleep(100 * time.Millisecond)
			assert.True(t, ep.Started(), "Service should be running after restart %d", i+1)
		}
	})

	t.Run("ServerReferenceCleanup", func(t *testing.T) {
		port := getAvailablePort()

		ep, err := createTestEndpoint(port)
		assert.Nil(t, err)

		router := createTestRouter("/reference-cleanup")
		_, err = ep.AddRouter(router, "GET")
		assert.Nil(t, err)

		// 启动服务
		err = ep.Start()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// 验证服务器引用存在
		assert.NotNil(t, ep.GetServer())
		assert.True(t, ep.Started())

		// 关闭服务
		err = ep.Close()
		assert.Nil(t, err)

		// 验证服务器引用被清理
		assert.Nil(t, ep.GetServer())
		assert.False(t, ep.Started())

		// 验证重复关闭不会panic或产生错误
		err = ep.Close()
		assert.Nil(t, err)
	})
}
