package fasthttp

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

// TestSetBody_SSEStreaming_AppendBehavior 测试 SetBody 在 SSE 流式场景下是否正确追加数据
// SSE 流式响应要求每次 SetBody 调用追加数据，而非替换
func TestSetBody_SSEStreaming_AppendBehavior(t *testing.T) {
	var ctx fasthttp.RequestCtx
	resp := &ResponseMessage{ctx: &ctx}

	// 模拟 SSE 流式写入：多次调用 SetBody，数据应追加而非替换
	resp.SetBody([]byte("data: chunk1\n\n"))
	resp.SetBody([]byte("data: chunk2\n\n"))
	resp.SetBody([]byte("data: chunk3\n\n"))

	body := string(ctx.Response.Body())

	// 验证所有 chunk 都在响应体中
	assert.Contains(t, body, "data: chunk1\n\n", "响应体应包含 chunk1")
	assert.Contains(t, body, "data: chunk2\n\n", "响应体应包含 chunk2")
	assert.Contains(t, body, "data: chunk3\n\n", "响应体应包含 chunk3")

	// 验证顺序正确
	expected := "data: chunk1\n\ndata: chunk2\n\ndata: chunk3\n\n"
	assert.Equal(t, expected, body, "SSE 数据应按追加顺序排列")
}

// TestComparisonWithRestEndpoint 对比 fasthttp 和标准 http 的行为一致性
// 标准 http 的 ResponseWriter.Write 是追加行为，fasthttp 应保持一致
func TestComparisonWithRestEndpoint(t *testing.T) {
	// 模拟标准 http 的行为（httptest.ResponseRecorder.Write 追加数据）
	recorder := httptest.NewRecorder()
	recorder.Write([]byte("data: chunk1\n\n"))
	recorder.Write([]byte("data: chunk2\n\n"))
	recorder.Write([]byte("data: chunk3\n\n"))
	restBody := recorder.Body.String()

	// 测试 fasthttp 的行为
	var ctx fasthttp.RequestCtx
	resp := &ResponseMessage{ctx: &ctx}
	resp.SetBody([]byte("data: chunk1\n\n"))
	resp.SetBody([]byte("data: chunk2\n\n"))
	resp.SetBody([]byte("data: chunk3\n\n"))
	fasthttpBody := string(ctx.Response.Body())

	assert.Equal(t, restBody, fasthttpBody,
		"fasthttp 和 rest 的 SetBody 行为应一致（追加而非替换）")
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

// TestSSEFullFlow 测试完整的 SSE 流式响应流程（模拟 openaiStreamingResponse 处理器的行为）
func TestSSEFullFlow(t *testing.T) {
	var ctx fasthttp.RequestCtx
	resp := &ResponseMessage{ctx: &ctx}

	// 1. 设置 SSE headers（对应 setSSEHeaders）
	resp.SetHeader("Content-Type", "text/event-stream")
	resp.SetHeader("Cache-Control", "no-cache")
	resp.SetHeader("Connection", "keep-alive")

	// 2. 发送多个 SSE chunk（对应 handleChunk）
	resp.SetBody([]byte("data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n"))
	resp.Flush()

	resp.SetBody([]byte("data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" World\"},\"finish_reason\":null}]}\n\n"))
	resp.Flush()

	// 3. 发送完成信号（对应 handleCompletion）
	resp.SetBody([]byte("data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	resp.Flush()

	// 验证完整响应
	body := string(ctx.Response.Body())

	// 验证所有 chunk 和完成信号都存在
	assert.True(t, strings.Contains(body, `"content":"Hello"`), "应包含第一个 chunk")
	assert.True(t, strings.Contains(body, `"content":" World"`), "应包含第二个 chunk")
	assert.True(t, strings.Contains(body, `"finish_reason":"stop"`), "应包含完成信号")
	assert.True(t, strings.Contains(body, "[DONE]"), "应包含 [DONE] 标记")

	// 验证 SSE headers 正确
	contentType := string(ctx.Response.Header.Peek("Content-Type"))
	assert.Equal(t, "text/event-stream", contentType)
}

// TestFlush_DoesNotCorruptData 测试 Flush 不损坏已有数据
func TestFlush_DoesNotCorruptData(t *testing.T) {
	var ctx fasthttp.RequestCtx
	resp := &ResponseMessage{ctx: &ctx}

	resp.SetBody([]byte("data: chunk1\n\n"))
	resp.Flush()
	resp.SetBody([]byte("data: chunk2\n\n"))
	resp.Flush()

	body := string(ctx.Response.Body())
	expected := "data: chunk1\n\ndata: chunk2\n\n"
	assert.Equal(t, expected, body, "Flush 不应损坏已写入的数据")
}

// TestSetBody_NonStreaming 测试非流式场景（单次 SetBody）
func TestSetBody_NonStreaming(t *testing.T) {
	var ctx fasthttp.RequestCtx
	resp := &ResponseMessage{ctx: &ctx}

	resp.SetBody([]byte(`{"result":"ok"}`))

	body := string(ctx.Response.Body())
	assert.Equal(t, `{"result":"ok"}`, body, "非流式单次 SetBody 应正常工作")
	assert.Equal(t, `{"result":"ok"}`, string(resp.Body()), "Body() 应返回最后一次设置的内容")
}

// TestSetBody_EmptyBody 测试空 body 不影响已有数据
func TestSetBody_EmptyBody(t *testing.T) {
	var ctx fasthttp.RequestCtx
	resp := &ResponseMessage{ctx: &ctx}

	resp.SetBody([]byte("data: chunk1\n\n"))
	resp.SetBody([]byte("")) // 空 body

	body := string(ctx.Response.Body())
	// 空 body 不应破坏之前的数据（追加空字节不改变内容）
	assert.Contains(t, body, "data: chunk1\n\n")
}
