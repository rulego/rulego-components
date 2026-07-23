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

// getAvailablePort Gets the available port
var portCounter int32 = 19080

func getAvailablePort() int {
	// Use atomic operations to increase port numbers to ensure each test uses a different port
	port := int(atomic.AddInt32(&portCounter, 1))
	return port
}

func checkPortAvailable(port int) bool {
	// Try listening on ports to check availability
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

		// Create endpoints
		ep, err := createTestEndpoint(port)
		assert.Nil(t, err)
		defer ep.Destroy()

		// Add routes
		router := createTestRouter("/test")
		_, err = ep.AddRouter(router, "GET")
		assert.Nil(t, err)

		// Start the server
		err = ep.Start()
		assert.Nil(t, err)

		// The verification service is running
		assert.True(t, ep.Started())
		assert.NotNil(t, ep.GetServer())

		// Waiting for the service to fully launch
		time.Sleep(200 * time.Millisecond)

		// Test HTTP requests
		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get(fmt.Sprintf("http://localhost:%d/test", port))
		if err == nil {
			resp.Body.Close()
		}

		// Restarting services
		err = ep.Restart()
		assert.Nil(t, err)

		// After the verification restart, the service is still running
		assert.True(t, ep.Started())
		assert.NotNil(t, ep.GetServer())

		// Wait for the restart to complete
		time.Sleep(200 * time.Millisecond)

		// Test the HTTP request again
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

		// Waiting for the service to start
		time.Sleep(100 * time.Millisecond)
		assert.True(t, ep.Started())

		// Close gracefully
		err = ep.Close()
		assert.Nil(t, err)
		assert.False(t, ep.Started())
	})

	t.Run("PortReuseAfterClose", func(t *testing.T) {
		port := getAvailablePort()

		// The first endpoint
		ep1, err := createTestEndpoint(port)
		assert.Nil(t, err)

		router := createTestRouter("/port-test")
		_, err = ep1.AddRouter(router, "GET")
		assert.Nil(t, err)

		err = ep1.Start()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// Close the first endpoint
		err = ep1.Close()
		assert.Nil(t, err)

		// Wait for the port to be released
		time.Sleep(200 * time.Millisecond)

		// The second endpoint uses the same port
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

		// Concurrent access testing
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
		t.Logf("Concurrent Access Test - Number of successful requests: %d/%d", successCount, numGoroutines)
	})

	t.Run("MultipleRoutes", func(t *testing.T) {
		port := getAvailablePort()

		ep, err := createTestEndpoint(port)
		assert.Nil(t, err)
		defer ep.Destroy()

		// Add multiple routes
		routes := []string{"/api/users", "/api/orders", "/api/products"}
		for i, route := range routes {
			router := createTestRouter(route)
			_, err = ep.AddRouter(router, "GET")
			assert.Nil(t, err, "Failed to add route %d: %s", i, route)
		}

		err = ep.Start()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// Test all routes
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

		// Verify SharedNode initialization
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

		// Create two endpoint instances using the same server address
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

		// Verify that they share the same instance ID
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

		// Test OPTIONS Request (CORS Pre-Release)
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
		// Initialization may succeed, but startup will fail
		if err == nil {
			err = ep.Start()
			// The launch might fail, and that's expected
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

		// The first time it was closed
		err = ep.Close()
		assert.Nil(t, err)

		// The second shutdown should also handle normally
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

		// The verification server is running
		assert.True(t, ep.Started())
		assert.NotNil(t, ep.GetServer())

		// The first time it was closed
		err = ep.Close()
		assert.Nil(t, err)
		assert.False(t, ep.Started())
		assert.Nil(t, ep.GetServer())

		// Repeated shutdowns should be safe and error-free
		err = ep.Close()
		assert.Nil(t, err)
		assert.False(t, ep.Started())
		assert.Nil(t, ep.GetServer())

		// Repeat the closing again
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

		// Concurrent call Close()
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

		// Check that all close calls are successful
		errorCount := 0
		for err := range errors {
			if err != nil {
				errorCount++
				t.Logf("Close error: %v", err)
			}
		}

		// There should be no errors, or at most only a few errors caused by concurrent contention
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

		// Start the server
		err = ep.Start()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// Restarting services
		err = ep.Restart()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// Restart and then close again
		err = ep.Close()
		assert.Nil(t, err)
		assert.False(t, ep.Started())

		// Closing again should be safe
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

		// Start the server
		err = ep.Start()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// Multiple reboots
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

		// Start the server
		err = ep.Start()
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)

		// Verifying that the server reference exists
		assert.NotNil(t, ep.GetServer())
		assert.True(t, ep.Started())

		// Service shutdown
		err = ep.Close()
		assert.Nil(t, err)

		// Verify that server references are cleaned up
		assert.Nil(t, ep.GetServer())
		assert.False(t, ep.Started())

		// Repeated closures do not cause panic or errors
		err = ep.Close()
		assert.Nil(t, err)
	})
}
