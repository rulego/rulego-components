package fasthttp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

// TestSetBody_BuffersBody 测试 SetBody 缓存 body（非流式路径由 handler 统一写出）
func TestSetBody_BuffersBody(t *testing.T) {
	var ctx fasthttp.RequestCtx
	resp := &ResponseMessage{ctx: &ctx}

	resp.SetBody([]byte(`{"result":"ok"}`))

	// SetBody 缓存在 resp.body 中，不直接写 ctx
	assert.Equal(t, `{"result":"ok"}`, string(resp.Body()), "Body() 应返回缓存的内容")
}

// TestFlush_CreatesGate 测试 Flush 首次调用时惰性创建 gate
func TestFlush_CreatesGate(t *testing.T) {
	var ctx fasthttp.RequestCtx
	resp := &ResponseMessage{ctx: &ctx}

	assert.Nil(t, resp.gate, "初始 gate 应为 nil")

	resp.Flush()

	assert.NotNil(t, resp.gate, "Flush 后 gate 应被创建")
}

// TestSSEHeaders 测试 SSE headers 设置是否正确
func TestSSEHeaders(t *testing.T) {
	var ctx fasthttp.RequestCtx
	resp := &ResponseMessage{ctx: &ctx}

	resp.SetHeader("Content-Type", "text/event-stream")
	resp.SetHeader("Cache-Control", "no-cache")
	resp.SetHeader("Connection", "keep-alive")

	contentType := string(ctx.Response.Header.Peek("Content-Type"))
	assert.Equal(t, "text/event-stream", contentType)

	cacheControl := string(ctx.Response.Header.Peek("Cache-Control"))
	assert.Equal(t, "no-cache", cacheControl)
}

// TestSetBody_NonStreaming 测试非流式场景（单次 SetBody，Body() 返回缓存）
func TestSetBody_NonStreaming(t *testing.T) {
	var ctx fasthttp.RequestCtx
	resp := &ResponseMessage{ctx: &ctx}

	resp.SetBody([]byte(`{"result":"ok"}`))

	assert.Equal(t, `{"result":"ok"}`, string(resp.Body()), "Body() 应返回最后一次设置的内容")
}

// TestSetBody_EmptyBody 测试空 body 不影响缓存
func TestSetBody_EmptyBody(t *testing.T) {
	var ctx fasthttp.RequestCtx
	resp := &ResponseMessage{ctx: &ctx}

	resp.SetBody([]byte("data: chunk1\n\n"))
	resp.SetBody([]byte("")) // 空 body 覆盖缓存

	assert.Equal(t, "", string(resp.Body()), "空 body 应覆盖之前的缓存")
}
