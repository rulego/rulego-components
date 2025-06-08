/*
 * Copyright 2023 The RuleGo Authors.
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

//
//import (
//	"bytes"
//	"fmt"
//	"github.com/rulego/rulego/api/types"
//	endpointApi "github.com/rulego/rulego/api/types/endpoint"
//	"github.com/rulego/rulego/endpoint/impl"
//	restEndpoint "github.com/rulego/rulego/endpoint/rest"
//	"github.com/rulego/rulego/engine"
//	"github.com/rulego/rulego/utils/maps"
//	"io"
//	"net/http"
//	"runtime"
//	"sync"
//	"testing"
//	"time"
//)
//
//// 性能对比测试：FastHTTP vs 标准HTTP
//func BenchmarkPerformanceComparison(b *testing.B) {
//	testData := `{"name":"performance_test","value":12345,"data":"` + string(make([]byte, 512)) + `"}`
//
//	// FastHTTP 性能测试
//	b.Run("FastHTTP_Endpoint", func(b *testing.B) {
//		config := engine.NewConfig(types.WithDefaultPool())
//		var nodeConfig = make(types.Configuration)
//		_ = maps.Map2Struct(&Config{
//			Server:      ":8080",
//			Concurrency: 1000,
//		}, &nodeConfig)
//		fasthttpEndpoint := &Endpoint{}
//		err := fasthttpEndpoint.Init(config, nodeConfig)
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		// 添加测试路由
//		router := impl.NewRouter().From("/benchmark").Transform(transformMsg(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
//			msg.SetData(fmt.Sprintf(`{"processed":true,"timestamp":%d,"original":%s}`, time.Now().UnixNano(), msg.GetData()))
//			return msg
//		})).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
//			exchange.Out.Headers().Set(ContentTypeKey, JsonContextType)
//			exchange.Out.SetBody([]byte(exchange.In.GetMsg().GetData()))
//			return true
//		}).End()
//		_, err = fasthttpEndpoint.AddRouter(router, "POST")
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		err = fasthttpEndpoint.Start()
//		if err != nil {
//			b.Fatal(err)
//		}
//		time.Sleep(time.Millisecond * 100)
//
//		b.ResetTimer()
//		b.RunParallel(func(pb *testing.PB) {
//			for pb.Next() {
//				resp, err := http.Post("http://localhost:8080/benchmark", "application/json", bytes.NewBufferString(testData))
//				if err != nil {
//					b.Error(err)
//					continue
//				}
//				_, _ = io.ReadAll(resp.Body)
//				resp.Body.Close()
//			}
//		})
//
//		fasthttpEndpoint.Destroy()
//	})
//
//	// 标准HTTP 性能测试
//	b.Run("Standard_HTTP_Endpoint", func(b *testing.B) {
//		config := engine.NewConfig(types.WithDefaultPool())
//		var nodeConfig = make(types.Configuration)
//		_ = maps.Map2Struct(&restEndpoint.Config{
//			Server: ":8081",
//		}, &nodeConfig)
//		restEp := &restEndpoint.Endpoint{}
//		err := restEp.Init(config, nodeConfig)
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		// 添加测试路由
//		router := impl.NewRouter().From("/benchmark").Transform(transformMsg(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
//			msg.SetData(fmt.Sprintf(`{"processed":true,"timestamp":%d,"original":%s}`, time.Now().UnixNano(), msg.GetData()))
//			return msg
//		})).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
//			exchange.Out.Headers().Set(ContentTypeKey, JsonContextType)
//			exchange.Out.SetBody([]byte(exchange.In.GetMsg().GetData()))
//			return true
//		}).End()
//		_, err = restEp.AddRouter(router, "POST")
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		err = restEp.Start()
//		if err != nil {
//			b.Fatal(err)
//		}
//		time.Sleep(time.Millisecond * 100)
//
//		b.ResetTimer()
//		b.RunParallel(func(pb *testing.PB) {
//			for pb.Next() {
//				resp, err := http.Post("http://localhost:8081/benchmark", "application/json", bytes.NewBufferString(testData))
//				if err != nil {
//					b.Error(err)
//					continue
//				}
//				_, _ = io.ReadAll(resp.Body)
//				resp.Body.Close()
//			}
//		})
//
//		restEp.Destroy()
//	})
//}
//
//// 内存分配对比测试
//func BenchmarkMemoryAllocation(b *testing.B) {
//	testData := `{"test":"memory","size":1024}`
//
//	b.Run("FastHTTP_Memory", func(b *testing.B) {
//		config := engine.NewConfig(types.WithDefaultPool())
//		var nodeConfig = make(types.Configuration)
//		_ = maps.Map2Struct(&Config{
//			Server: ":8082",
//		}, &nodeConfig)
//		fasthttpEndpoint := &Endpoint{}
//		err := fasthttpEndpoint.Init(config, nodeConfig)
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		router := impl.NewRouter().From("/memory").Transform(transformMsg(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
//			return msg
//		})).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
//			exchange.Out.Headers().Set(ContentTypeKey, JsonContextType)
//			exchange.Out.SetBody([]byte(exchange.In.GetMsg().GetData()))
//			return true
//		}).End()
//		_, err = fasthttpEndpoint.AddRouter(router, "POST")
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		err = fasthttpEndpoint.Start()
//		if err != nil {
//			b.Fatal(err)
//		}
//		time.Sleep(time.Millisecond * 100)
//
//		b.ResetTimer()
//		for i := 0; i < b.N; i++ {
//			resp, err := http.Post("http://localhost:8082/memory", "application/json", bytes.NewBufferString(testData))
//			if err != nil {
//				b.Error(err)
//				continue
//			}
//			_, _ = io.ReadAll(resp.Body)
//			resp.Body.Close()
//		}
//
//		fasthttpEndpoint.Destroy()
//	})
//
//	b.Run("Standard_HTTP_Memory", func(b *testing.B) {
//		config := engine.NewConfig(types.WithDefaultPool())
//		var nodeConfig = make(types.Configuration)
//		_ = maps.Map2Struct(&restEndpoint.Config{
//			Server: ":8083",
//		}, &nodeConfig)
//		restEp := &restEndpoint.Endpoint{}
//		err := restEp.Init(config, nodeConfig)
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		router := impl.NewRouter().From("/memory").Transform(transformMsg(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
//			return msg
//		})).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
//			exchange.Out.Headers().Set(ContentTypeKey, JsonContextType)
//			exchange.Out.SetBody([]byte(exchange.In.GetMsg().GetData()))
//			return true
//		}).End()
//		_, err = restEp.AddRouter(router, "POST")
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		err = restEp.Start()
//		if err != nil {
//			b.Fatal(err)
//		}
//		time.Sleep(time.Millisecond * 100)
//
//		b.ResetTimer()
//		for i := 0; i < b.N; i++ {
//			resp, err := http.Post("http://localhost:8083/memory", "application/json", bytes.NewBufferString(testData))
//			if err != nil {
//				b.Error(err)
//				continue
//			}
//			_, _ = io.ReadAll(resp.Body)
//			resp.Body.Close()
//		}
//
//		restEp.Destroy()
//	})
//}
//
//// 并发性能对比测试
//func TestConcurrencyComparison(t *testing.T) {
//	concurrencyLevels := []int{10, 50, 100, 200, 500}
//	requestsPerGoroutine := 100
//	testData := `{"concurrent":"test","data":"` + string(make([]byte, 256)) + `"}`
//
//	for _, concurrency := range concurrencyLevels {
//		t.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(t *testing.T) {
//			// 测试 FastHTTP
//			fasthttpResult := testConcurrency(t, "fasthttp", ":8084", concurrency, requestsPerGoroutine, testData)
//			// 测试标准 HTTP
//			stdHttpResult := testConcurrency(t, "stdhttp", ":8085", concurrency, requestsPerGoroutine, testData)
//
//			t.Logf("Concurrency %d - FastHTTP: %.2f req/s, StdHTTP: %.2f req/s, Improvement: %.2fx",
//				concurrency,
//				fasthttpResult.RequestsPerSecond,
//				stdHttpResult.RequestsPerSecond,
//				fasthttpResult.RequestsPerSecond/stdHttpResult.RequestsPerSecond)
//		})
//	}
//}
//
//type BenchmarkResult struct {
//	TotalRequests      int64
//	SuccessfulRequests int64
//	FailedRequests     int64
//	Duration           time.Duration
//	RequestsPerSecond  float64
//	SuccessRate        float64
//}
//
//func testConcurrency(t *testing.T, endpointType, port string, concurrency, requestsPerGoroutine int, testData string) BenchmarkResult {
//	config := engine.NewConfig(types.WithDefaultPool())
//
//	var endpoint interface{}
//	var err error
//
//	if endpointType == "fasthttp" {
//		var nodeConfig = make(types.Configuration)
//		_ = maps.Map2Struct(&Config{
//			Server:      port,
//			Concurrency: concurrency * 2,
//		}, &nodeConfig)
//		fasthttpEp := &Endpoint{}
//		err = fasthttpEp.Init(config, nodeConfig)
//		if err != nil {
//			t.Fatal(err)
//		}
//		router := impl.NewRouter().From("/test").Transform(transformMsg(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
//			msg.SetData(fmt.Sprintf(`{"processed":true,"data":%s}`, msg.GetData()))
//			return msg
//		})).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
//			exchange.Out.Headers().Set(ContentTypeKey, JsonContextType)
//			exchange.Out.SetBody([]byte(exchange.In.GetMsg().GetData()))
//			return true
//		}).End()
//		_, err = fasthttpEp.AddRouter(router, "POST")
//		if err != nil {
//			t.Fatal(err)
//		}
//		err = fasthttpEp.Start()
//		if err != nil {
//			t.Fatal(err)
//		}
//		endpoint = fasthttpEp
//	} else {
//		var nodeConfig = make(types.Configuration)
//		_ = maps.Map2Struct(&restEndpoint.Config{
//			Server: port,
//		}, &nodeConfig)
//		restEp := &restEndpoint.Endpoint{}
//		err = restEp.Init(config, nodeConfig)
//		if err != nil {
//			t.Fatal(err)
//		}
//		router := impl.NewRouter().From("/test").Transform(transformMsg(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
//			msg.SetData(fmt.Sprintf(`{"processed":true,"data":%s}`, msg.GetData()))
//			return msg
//		})).End()
//		_, err = restEp.AddRouter(router, "POST")
//		if err != nil {
//			t.Fatal(err)
//		}
//		err = restEp.Start()
//		if err != nil {
//			t.Fatal(err)
//		}
//		endpoint = restEp
//	}
//
//	time.Sleep(time.Millisecond * 200)
//
//	var wg sync.WaitGroup
//	var successCount int64
//	var errorCount int64
//	var mu sync.Mutex
//
//	start := time.Now()
//
//	for i := 0; i < concurrency; i++ {
//		wg.Add(1)
//		go func() {
//			defer wg.Done()
//			for j := 0; j < requestsPerGoroutine; j++ {
//				url := fmt.Sprintf("http://localhost%s/test", port)
//				resp, err := http.Post(url, "application/json", bytes.NewBufferString(testData))
//				mu.Lock()
//				if err != nil {
//					errorCount++
//				} else {
//					successCount++
//					_, _ = io.ReadAll(resp.Body)
//					resp.Body.Close()
//				}
//				mu.Unlock()
//			}
//		}()
//	}
//
//	wg.Wait()
//	duration := time.Since(start)
//
//	// 清理
//	if endpointType == "fasthttp" {
//		if ep, ok := endpoint.(*Endpoint); ok {
//			ep.Destroy()
//		}
//	} else {
//		if ep, ok := endpoint.(*restEndpoint.Endpoint); ok {
//			ep.Destroy()
//		}
//	}
//
//	totalRequests := int64(concurrency * requestsPerGoroutine)
//	return BenchmarkResult{
//		TotalRequests:      totalRequests,
//		SuccessfulRequests: successCount,
//		FailedRequests:     errorCount,
//		Duration:           duration,
//		RequestsPerSecond:  float64(totalRequests) / duration.Seconds(),
//		SuccessRate:        float64(successCount) / float64(totalRequests),
//	}
//}
//
//// 延迟测试
//func TestLatencyComparison(t *testing.T) {
//	numRequests := 1000
//	testData := `{"latency":"test"}`
//
//	// 测试 FastHTTP 延迟
//	fasthttpLatencies := measureLatency(t, "fasthttp", ":8086", numRequests, testData)
//	// 测试标准 HTTP 延迟
//	stdHttpLatencies := measureLatency(t, "stdhttp", ":8087", numRequests, testData)
//
//	// 计算统计信息
//	fasthttpStats := calculateLatencyStats(fasthttpLatencies)
//	stdHttpStats := calculateLatencyStats(stdHttpLatencies)
//
//	t.Logf("Latency Comparison (microseconds):")
//	t.Logf("FastHTTP - Min: %d, Max: %d, Avg: %.2f, P95: %d, P99: %d",
//		fasthttpStats.Min, fasthttpStats.Max, fasthttpStats.Avg, fasthttpStats.P95, fasthttpStats.P99)
//	t.Logf("StdHTTP  - Min: %d, Max: %d, Avg: %.2f, P95: %d, P99: %d",
//		stdHttpStats.Min, stdHttpStats.Max, stdHttpStats.Avg, stdHttpStats.P95, stdHttpStats.P99)
//	t.Logf("Improvement - Avg: %.2fx, P95: %.2fx, P99: %.2fx",
//		stdHttpStats.Avg/fasthttpStats.Avg,
//		float64(stdHttpStats.P95)/float64(fasthttpStats.P95),
//		float64(stdHttpStats.P99)/float64(fasthttpStats.P99))
//}
//
//type LatencyStats struct {
//	Min int64
//	Max int64
//	Avg float64
//	P95 int64
//	P99 int64
//}
//
//func measureLatency(t *testing.T, endpointType, port string, numRequests int, testData string) []int64 {
//	config := engine.NewConfig(types.WithDefaultPool())
//	var endpoint interface{}
//	var err error
//
//	if endpointType == "fasthttp" {
//		var nodeConfig = make(types.Configuration)
//		_ = maps.Map2Struct(&Config{
//			Server: port,
//		}, &nodeConfig)
//		fasthttpEp := &Endpoint{}
//		err = fasthttpEp.Init(config, nodeConfig)
//		if err != nil {
//			t.Fatal(err)
//		}
//		router := impl.NewRouter().From("/latency").Transform(transformMsg(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
//			return msg
//		})).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
//			exchange.Out.Headers().Set(ContentTypeKey, JsonContextType)
//			exchange.Out.SetBody([]byte(exchange.In.GetMsg().GetData()))
//			return true
//		}).End()
//		_, err = fasthttpEp.AddRouter(router, "POST")
//		if err != nil {
//			t.Fatal(err)
//		}
//		err = fasthttpEp.Start()
//		if err != nil {
//			t.Fatal(err)
//		}
//		endpoint = fasthttpEp
//	} else {
//		var nodeConfig = make(types.Configuration)
//		_ = maps.Map2Struct(&restEndpoint.Config{
//			Server: port,
//		}, &nodeConfig)
//		restEp := &restEndpoint.Endpoint{}
//		err = restEp.Init(config, nodeConfig)
//		if err != nil {
//			t.Fatal(err)
//		}
//		router := impl.NewRouter().From("/latency").Transform(transformMsg(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
//			return msg
//		})).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
//			exchange.Out.Headers().Set(ContentTypeKey, JsonContextType)
//			exchange.Out.SetBody([]byte(exchange.In.GetMsg().GetData()))
//			return true
//		}).End()
//		_, err = restEp.AddRouter(router, "POST")
//		if err != nil {
//			t.Fatal(err)
//		}
//		err = restEp.Start()
//		if err != nil {
//			t.Fatal(err)
//		}
//		endpoint = restEp
//	}
//
//	time.Sleep(time.Millisecond * 200)
//
//	latencies := make([]int64, numRequests)
//	url := fmt.Sprintf("http://localhost%s/latency", port)
//
//	for i := 0; i < numRequests; i++ {
//		start := time.Now()
//		resp, err := http.Post(url, "application/json", bytes.NewBufferString(testData))
//		if err != nil {
//			t.Error(err)
//			continue
//		}
//		_, _ = io.ReadAll(resp.Body)
//		resp.Body.Close()
//		latencies[i] = time.Since(start).Microseconds()
//	}
//
//	// 清理
//	if endpointType == "fasthttp" {
//		if ep, ok := endpoint.(*Endpoint); ok {
//			ep.Destroy()
//		}
//	} else {
//		if ep, ok := endpoint.(*restEndpoint.Endpoint); ok {
//			ep.Destroy()
//		}
//	}
//
//	return latencies
//}
//
//func calculateLatencyStats(latencies []int64) LatencyStats {
//	if len(latencies) == 0 {
//		return LatencyStats{}
//	}
//
//	// 排序
//	for i := 0; i < len(latencies); i++ {
//		for j := i + 1; j < len(latencies); j++ {
//			if latencies[i] > latencies[j] {
//				latencies[i], latencies[j] = latencies[j], latencies[i]
//			}
//		}
//	}
//
//	var sum int64
//	for _, lat := range latencies {
//		sum += lat
//	}
//
//	p95Index := int(float64(len(latencies)) * 0.95)
//	p99Index := int(float64(len(latencies)) * 0.99)
//
//	return LatencyStats{
//		Min: latencies[0],
//		Max: latencies[len(latencies)-1],
//		Avg: float64(sum) / float64(len(latencies)),
//		P95: latencies[p95Index],
//		P99: latencies[p99Index],
//	}
//}
//
//// 资源使用对比测试
//func TestResourceUsage(t *testing.T) {
//	numRequests := 10000
//	testData := `{"resource":"test","payload":"` + string(make([]byte, 1024)) + `"}`
//
//	t.Run("FastHTTP_Resource_Usage", func(t *testing.T) {
//		var m1, m2 runtime.MemStats
//		runtime.GC()
//		runtime.ReadMemStats(&m1)
//
//		config := engine.NewConfig(types.WithDefaultPool())
//		var nodeConfig = make(types.Configuration)
//		_ = maps.Map2Struct(&Config{
//			Server: ":8088",
//		}, &nodeConfig)
//		fasthttpEp := &Endpoint{}
//		err := fasthttpEp.Init(config, nodeConfig)
//		if err != nil {
//			t.Fatal(err)
//		}
//
//		router := impl.NewRouter().From("/memory").Transform(transformMsg(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
//			return msg
//		})).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
//			exchange.Out.Headers().Set(ContentTypeKey, JsonContextType)
//			exchange.Out.SetBody([]byte(exchange.In.GetMsg().GetData()))
//			return true
//		}).End()
//		_, err = fasthttpEp.AddRouter(router, "POST")
//		if err != nil {
//			t.Fatal(err)
//		}
//
//		err = fasthttpEp.Start()
//		if err != nil {
//			t.Fatal(err)
//		}
//		time.Sleep(time.Millisecond * 200)
//
//		for i := 0; i < numRequests; i++ {
//			resp, err := http.Post("http://localhost:8088/resource", "application/json", bytes.NewBufferString(testData))
//			if err != nil {
//				t.Error(err)
//				continue
//			}
//			_, _ = io.ReadAll(resp.Body)
//			resp.Body.Close()
//		}
//
//		runtime.GC()
//		runtime.ReadMemStats(&m2)
//
//		t.Logf("FastHTTP - Memory allocated: %d KB, Total allocations: %d",
//			(m2.TotalAlloc-m1.TotalAlloc)/1024, m2.Mallocs-m1.Mallocs)
//
//		fasthttpEp.Destroy()
//	})
//
//	t.Run("Standard_HTTP_Resource_Usage", func(t *testing.T) {
//		var m1, m2 runtime.MemStats
//		runtime.GC()
//		runtime.ReadMemStats(&m1)
//
//		config := engine.NewConfig(types.WithDefaultPool())
//		var nodeConfig = make(types.Configuration)
//		_ = maps.Map2Struct(&restEndpoint.Config{
//			Server: ":8089",
//		}, &nodeConfig)
//		restEp := &restEndpoint.Endpoint{}
//		err := restEp.Init(config, nodeConfig)
//		if err != nil {
//			t.Fatal(err)
//		}
//
//		router := impl.NewRouter().From("/memory").Transform(transformMsg(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
//			return msg
//		})).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
//			exchange.Out.Headers().Set(ContentTypeKey, JsonContextType)
//			exchange.Out.SetBody([]byte(exchange.In.GetMsg().GetData()))
//			return true
//		}).End()
//		_, err = restEp.AddRouter(router, "POST")
//		if err != nil {
//			t.Fatal(err)
//		}
//
//		err = restEp.Start()
//		if err != nil {
//			t.Fatal(err)
//		}
//		time.Sleep(time.Millisecond * 200)
//
//		for i := 0; i < numRequests; i++ {
//			resp, err := http.Post("http://localhost:8089/resource", "application/json", bytes.NewBufferString(testData))
//			if err != nil {
//				t.Error(err)
//				continue
//			}
//			_, _ = io.ReadAll(resp.Body)
//			resp.Body.Close()
//		}
//
//		runtime.GC()
//		runtime.ReadMemStats(&m2)
//
//		t.Logf("Standard HTTP - Memory allocated: %d KB, Total allocations: %d",
//			(m2.TotalAlloc-m1.TotalAlloc)/1024, m2.Mallocs-m1.Mallocs)
//
//		restEp.Destroy()
//	})
//}
