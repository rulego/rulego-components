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
	"github.com/rulego/rulego/endpoint"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/test/assert"
)

// 用真实 server 路径验证流式响应：处理器在 DoProcess 期间设置 SSE headers
// 并分多次 SetBody 写 chunk，headers/状态码必须到达客户端
func TestFastHttpStreamingEndToEnd(t *testing.T) {
	config := rulego.NewConfig(types.WithDefaultPool(), types.WithEndpointEnabled(true))
	ep, err := endpoint.Registry.New(Type, config, types.Configuration{
		"server": "127.0.0.1:19801",
	})
	assert.Nil(t, err)
	fh := ep.(*FastHttp)

	fh.GET(endpoint.NewRouter().From("/api/v1/sse").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
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
