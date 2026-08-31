package fasthttp

import (
	"bufio"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/test/assert"
)

// 用真实 server 路径验证流式响应：处理器在 DoProcess 期间设置 SSE headers
// 并分多次 SetBody 写 chunk，headers/状态码必须到达客户端。
// SSE 路由必须在 from 配置标记 streaming=true 才走增量推送路径。
func TestFastHttpStreamingEndToEnd(t *testing.T) {
	config := rulego.NewConfig(types.WithDefaultPool(), types.WithEndpointEnabled(true))
	ep, err := endpoint.Registry.New(Type, config, types.Configuration{
		"server": "127.0.0.1:19801",
	})
	assert.Nil(t, err)
	fh := ep.(*FastHttp)

	fh.GET(endpoint.NewRouter().From("/api/v1/sse", types.Configuration{
		endpointApi.ConfigKeyStreaming: true,
	}).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		out := exchange.Out.(endpointApi.HeaderModifier)
		out.SetHeader("Content-Type", "text/event-stream")
		out.SetHeader("Cache-Control", "no-cache")
		exchange.Out.SetBody([]byte("data: chunk1\n\n"))
		exchange.Out.(endpointApi.Flusher).Flush()
		time.Sleep(500 * time.Millisecond)
		exchange.Out.SetBody([]byte("data: chunk2\n\n"))
		exchange.Out.(endpointApi.Flusher).Flush()
		return true
	}).End())

	// 智能体 SSE 形态：OpenAI chunk + [DONE]，多轮间隔推送
	fh.GET(endpoint.NewRouter().From("/api/v1/agent-sse", types.Configuration{
		endpointApi.ConfigKeyStreaming: true,
	}).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		out := exchange.Out.(endpointApi.HeaderModifier)
		out.SetHeader("Content-Type", "text/event-stream")
		out.SetHeader("Cache-Control", "no-cache")
		out.SetHeader("X-Accel-Buffering", "no")
		for _, chunk := range []string{
			`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"你"},"finish_reason":null}]}` + "\n\n",
			`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"好"},"finish_reason":null}]}` + "\n\n",
			`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n",
			"data: [DONE]\n\n",
		} {
			exchange.Out.SetBody([]byte(chunk))
			exchange.Out.(endpointApi.Flusher).Flush()
			time.Sleep(300 * time.Millisecond)
		}
		return true
	}).End())

	// 未标记 streaming 的路由调用 Flush：同步路径兜底，数据完整送达但不增量
	fh.GET(endpoint.NewRouter().From("/api/v1/sse-unmarked").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		out := exchange.Out.(endpointApi.HeaderModifier)
		out.SetHeader("Content-Type", "text/event-stream")
		exchange.Out.SetBody([]byte("data: chunk1\n\n"))
		exchange.Out.(endpointApi.Flusher).Flush()
		time.Sleep(300 * time.Millisecond)
		exchange.Out.SetBody([]byte("data: chunk2\n\n"))
		exchange.Out.(endpointApi.Flusher).Flush()
		return true
	}).End())

	fh.GET(endpoint.NewRouter().From("/api/v1/created").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		exchange.Out.SetStatusCode(http.StatusCreated)
		exchange.Out.(endpointApi.HeaderModifier).SetHeader("Content-Type", "application/json")
		exchange.Out.SetBody([]byte(`{"ok":true}`))
		return true
	}).End())

	fh.GET(endpoint.NewRouter().From("/api/v1/panic").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		panic("boom")
	}).End())

	assert.Nil(t, fh.Start())
	defer fh.Close()
	time.Sleep(500 * time.Millisecond)

	t.Run("SSEHeadersAndChunkOrder", func(t *testing.T) {
		resp, err := http.Get("http://127.0.0.1:19801/api/v1/sse")
		assert.Nil(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))
		assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))

		body, err := io.ReadAll(resp.Body)
		assert.Nil(t, err)
		assert.Equal(t, "data: chunk1\n\ndata: chunk2\n\n", string(body))
	})

	t.Run("IncrementalDelivery", func(t *testing.T) {
		resp, err := http.Get("http://127.0.0.1:19801/api/v1/sse")
		assert.Nil(t, err)
		defer resp.Body.Close()
		reader := bufio.NewReader(resp.Body)
		start := time.Now()
		line, err := reader.ReadString('\n')
		assert.Nil(t, err)
		// 第一个 chunk 必须在第二个 chunk 产出（500ms sleep）之前到达
		assert.True(t, strings.HasPrefix(line, "data: chunk1"))
		assert.True(t, time.Since(start) < 450*time.Millisecond, "first chunk should arrive incrementally, took %v", time.Since(start))
		_, _ = io.ReadAll(reader)
	})

	t.Run("AgentSSEByteExactAndIncremental", func(t *testing.T) {
		resp, err := http.Get("http://127.0.0.1:19801/api/v1/agent-sse")
		assert.Nil(t, err)
		defer resp.Body.Close()
		assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))
		assert.Equal(t, "no", resp.Header.Get("X-Accel-Buffering"))

		reader := bufio.NewReader(resp.Body)
		start := time.Now()
		line, err := reader.ReadString('\n')
		assert.Nil(t, err)
		// 首个 token chunk 必须早于后续 3×300ms 的产出节奏到达
		assert.True(t, strings.HasPrefix(line, `data: {"id":"chatcmpl-1"`))
		assert.True(t, strings.Contains(line, `"content":"你"`))
		assert.True(t, time.Since(start) < 250*time.Millisecond, "first agent chunk should arrive incrementally, took %v", time.Since(start))
		rest, err := io.ReadAll(reader)
		assert.Nil(t, err)
		full := line + string(rest)
		assert.True(t, strings.HasSuffix(full, `data: [DONE]`+"\n\n"))
		assert.Equal(t, 4, strings.Count(full, "data: "), "agent SSE 应包含 4 个 data 事件")
	})

	t.Run("UnmarkedFlushDeliversCompleteBody", func(t *testing.T) {
		start := time.Now()
		resp, err := http.Get("http://127.0.0.1:19801/api/v1/sse-unmarked")
		assert.Nil(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

		body, err := io.ReadAll(resp.Body)
		assert.Nil(t, err)
		assert.Equal(t, "data: chunk1\n\ndata: chunk2\n\n", string(body))
		// 同步路径：响应只能在处理器（含 300ms sleep）结束后写出
		assert.True(t, time.Since(start) >= 280*time.Millisecond, "unmarked flush route should buffer until process ends, took %v", time.Since(start))
	})

	t.Run("StatusCodeAndHeader", func(t *testing.T) {
		resp, err := http.Get("http://127.0.0.1:19801/api/v1/created")
		assert.Nil(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
		body, _ := io.ReadAll(resp.Body)
		assert.Equal(t, `{"ok":true}`, string(body))
	})

	t.Run("ProcessPanicReturns500", func(t *testing.T) {
		resp, err := http.Get("http://127.0.0.1:19801/api/v1/panic")
		assert.Nil(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	})
}
