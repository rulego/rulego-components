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
	"bufio"
	"bytes"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	restEndpoint "github.com/rulego/rulego/endpoint/rest"
	"github.com/valyala/fasthttp"
)

// abEchoProcess 两个 endpoint 共用的路由逻辑：取 msg、echo 回去。
// header 写入走 HeaderModifier 断言——fasthttp 的 Headers() 返回拷贝 map，直接写它无效。
func abEchoProcess(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
	msg := exchange.In.GetMsg()
	if hm, ok := exchange.Out.(endpointApi.HeaderModifier); ok {
		hm.SetHeader(ContentTypeKey, JsonContextType)
	}
	exchange.Out.SetBody([]byte(msg.GetData()))
	return true
}

func abPayload() []byte {
	var b strings.Builder
	b.WriteString(`{"device":"gw-01","ts":1750000000000,"metrics":{"temperature":25.5,"humidity":60.1,"voltage":220.2}`)
	b.WriteString(strings.Repeat(`,"pad":1`, 50))
	b.WriteString(`}`)
	return []byte(b.String())
}

// 本包 init() 已把 registry 里的 endpoint/http 换成 FastHttp，rest 不能再从 registry 取，直接构造
func abStartFastHTTP(b *testing.B, addr string) *FastHttp {
	b.Helper()
	config := rulego.NewConfig(types.WithDefaultPool(), types.WithEndpointEnabled(true))
	fh := &FastHttp{}
	if err := fh.Init(config, types.Configuration{"server": addr}); err != nil {
		b.Fatal(err)
	}
	fh.POST(endpoint.NewRouter().From("/bench").Process(abEchoProcess).End())
	if err := fh.Start(); err != nil {
		b.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	return fh
}

func abStartRest(b *testing.B, addr string) *restEndpoint.Endpoint {
	b.Helper()
	config := rulego.NewConfig(types.WithDefaultPool(), types.WithEndpointEnabled(true))
	restEp := &restEndpoint.Endpoint{}
	if err := restEp.Init(config, types.Configuration{"server": addr}); err != nil {
		b.Fatal(err)
	}
	restEp.POST(endpoint.NewRouter().From("/bench").Process(abEchoProcess).End())
	if err := restEp.Start(); err != nil {
		b.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	return restEp
}

// BenchmarkAB 真实回环压测：fasthttp client（keep-alive）打两个 endpoint。
// 客户端部分两者相同，耗时差即服务端差。GOMAXPROCS 由外部环境变量控制。
func BenchmarkAB(b *testing.B) {
	payload := abPayload()

	b.Run("fasthttp", func(b *testing.B) {
		ep := abStartFastHTTP(b, "127.0.0.1:21901")
		defer ep.Close()
		client := &fasthttp.Client{}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			req := fasthttp.AcquireRequest()
			resp := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseRequest(req)
			defer fasthttp.ReleaseResponse(resp)
			req.Header.SetMethod(fasthttp.MethodPost)
			req.SetRequestURI("http://127.0.0.1:21901/bench")
			req.Header.Set(ContentTypeKey, JsonContextType)
			req.SetBody(payload)
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

	b.Run("rest", func(b *testing.B) {
		ep := abStartRest(b, "127.0.0.1:21902")
		defer ep.Close()
		client := &fasthttp.Client{}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			req := fasthttp.AcquireRequest()
			resp := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseRequest(req)
			defer fasthttp.ReleaseResponse(resp)
			req.Header.SetMethod(fasthttp.MethodPost)
			req.SetRequestURI("http://127.0.0.1:21902/bench")
			req.Header.Set(ContentTypeKey, JsonContextType)
			req.SetBody(payload)
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

// replayConn 无限重放同一段请求字节，Write 丢弃。用于单连接度量服务端成本。
type replayConn struct {
	req []byte
}

func (c *replayConn) Read(b []byte) (int, error)         { return copy(b, c.req), nil }
func (c *replayConn) Write(b []byte) (int, error)        { return len(b), nil }
func (c *replayConn) Close() error                       { return nil }
func (c *replayConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (c *replayConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (c *replayConn) SetDeadline(t time.Time) error      { return nil }
func (c *replayConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *replayConn) SetWriteDeadline(t time.Time) error { return nil }

// discardWriter net/http 响应黑洞
type discardWriter struct {
	header http.Header
}

func (w *discardWriter) Header() http.Header         { return w.header }
func (w *discardWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *discardWriter) WriteHeader(int)             {}

func abRawRequest() []byte {
	payload := abPayload()
	var b strings.Builder
	b.WriteString("POST /bench HTTP/1.1\r\n")
	b.WriteString("Host: 127.0.0.1\r\n")
	b.WriteString("User-Agent: loadtest/1.0\r\n")
	b.WriteString("Accept: application/json\r\n")
	b.WriteString(ContentTypeKey + ": " + JsonContextType + "\r\n")
	b.WriteString("X-Request-Id: bench-0001\r\n")
	b.WriteString("Content-Length: " + itoa(len(payload)) + "\r\n\r\n")
	b.Write(payload)
	return []byte(b.String())
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// BenchmarkServerCost 服务端单请求成本（无客户端、无网络栈调度）。
// fasthttp 侧经 ServeConn 完整走 server 解析+handler+响应编码；
// rest 侧经 http.ReadRequest 解析 + httprouter 分发（不含 conn 管理，对 rest 有利）。
// -benchmem 的 allocs/op 即服务端每请求分配数。
func BenchmarkServerCost(b *testing.B) {
	rawReq := abRawRequest()

	b.Run("fasthttp", func(b *testing.B) {
		ep := abStartFastHTTP(b, "127.0.0.1:21903")
		defer ep.Close()
		handler := ep.router.Handler
		var served uint64
		go func() {
			// replayConn 无限重放同一请求，连接永不关闭
			_ = fasthttp.ServeConn(&replayConn{req: rawReq}, func(ctx *fasthttp.RequestCtx) {
				handler(ctx)
				atomic.AddUint64(&served, 1)
			})
		}()
		time.Sleep(100 * time.Millisecond)
		b.ResetTimer()
		for atomic.LoadUint64(&served) < uint64(b.N) {
			time.Sleep(50 * time.Microsecond)
		}
	})

	b.Run("rest", func(b *testing.B) {
		ep := abStartRest(b, "127.0.0.1:21904")
		defer ep.Close()
		router := ep.Router()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			req, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(rawReq)))
			if err != nil {
				b.Fatal(err)
			}
			w := &discardWriter{header: make(http.Header)}
			router.ServeHTTP(w, req)
			_ = req.Body.Close()
		}
	})
}
