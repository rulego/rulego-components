/*
 * Copyright 2026 The RuleGo Authors.
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
	"testing"
	"time"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/valyala/fasthttp"
)

// 对比普通请求（同步路径）与流式标记请求（异步路径）的每请求开销：
// 差值即 1 goroutine + 2 channel + select 的调度成本。
func BenchmarkRequestPath(b *testing.B) {
	config := rulego.NewConfig(types.WithDefaultPool(), types.WithEndpointEnabled(true))
	ep, err := endpoint.Registry.New(Type, config, types.Configuration{
		"server": "127.0.0.1:19877",
	})
	if err != nil {
		b.Fatal(err)
	}
	fh := ep.(*FastHttp)

	process := func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		exchange.Out.SetBody([]byte(`{"ok":true}`))
		return true
	}
	fh.POST(endpoint.NewRouter().From("/sync").Process(process).End())
	fh.POST(endpoint.NewRouter().From("/stream", types.Configuration{
		endpointApi.ConfigKeyStreaming: true,
	}).Process(process).End())

	if err := fh.Start(); err != nil {
		b.Fatal(err)
	}
	defer fh.Close()
	time.Sleep(200 * time.Millisecond)

	client := &fasthttp.Client{}
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	// 路由用 POST 注册，不显式设 method 会发 GET 全部 404，量到的是 NotFound 路径
	req.Header.SetMethod(fasthttp.MethodPost)

	b.Run("SyncPath", func(b *testing.B) {
		req.SetRequestURI("http://127.0.0.1:19877/sync")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Do(req, resp); err != nil {
				b.Fatal(err)
			}
			if resp.StatusCode() != fasthttp.StatusOK {
				b.Fatalf("status=%d", resp.StatusCode())
			}
		}
	})
	b.Run("StreamingPath", func(b *testing.B) {
		req.SetRequestURI("http://127.0.0.1:19877/stream")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Do(req, resp); err != nil {
				b.Fatal(err)
			}
			if resp.StatusCode() != fasthttp.StatusOK {
				b.Fatalf("status=%d", resp.StatusCode())
			}
		}
	})

	// 并行在途请求：模拟压测场景（多连接饱和），goroutine/channel 开销
	// 只有在调度压力下才能体现，串行循环会把它隐藏掉
	b.Run("ParallelSyncPath", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			req := fasthttp.AcquireRequest()
			resp := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseRequest(req)
			defer fasthttp.ReleaseResponse(resp)
			req.Header.SetMethod(fasthttp.MethodPost)
			req.SetRequestURI("http://127.0.0.1:19877/sync")
			for pb.Next() {
				if err := client.Do(req, resp); err != nil {
					b.Fatal(err)
				}
				if resp.StatusCode() != fasthttp.StatusOK {
					b.Fatalf("status=%d", resp.StatusCode())
				}
			}
		})
	})
	b.Run("ParallelStreamingPath", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			req := fasthttp.AcquireRequest()
			resp := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseRequest(req)
			defer fasthttp.ReleaseResponse(resp)
			req.Header.SetMethod(fasthttp.MethodPost)
			req.SetRequestURI("http://127.0.0.1:19877/stream")
			for pb.Next() {
				if err := client.Do(req, resp); err != nil {
					b.Fatal(err)
				}
				if resp.StatusCode() != fasthttp.StatusOK {
					b.Fatalf("status=%d", resp.StatusCode())
				}
			}
		})
	})
}
