package fasthttp

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

// TestSetBody_SSEStreaming_AppendBehavior Testing whether SetBody correctly appends data in SSE streaming scenarios
// SSE stream responses require each SetBody call to add data instead of replace
func TestSetBody_SSEStreaming_AppendBehavior(t *testing.T) {
	var ctx fasthttp.RequestCtx
	resp := &ResponseMessage{ctx: &ctx}

	// Simulating SSE stream writes: Multiple calls to SetBody, data should be appended rather than replaced
	resp.SetBody([]byte("data: chunk1\n\n"))
	resp.SetBody([]byte("data: chunk2\n\n"))
	resp.SetBody([]byte("data: chunk3\n\n"))

	body := string(ctx.Response.Body())

	// Verify that all chunks are in the response body
	assert.Contains(t, body, "data: chunk1\n\n", "响应体应包含 chunk1")
	assert.Contains(t, body, "data: chunk2\n\n", "响应体应包含 chunk2")
	assert.Contains(t, body, "data: chunk3\n\n", "响应体应包含 chunk3")

	// The verification sequence is correct
	expected := "data: chunk1\n\ndata: chunk2\n\ndata: chunk3\n\n"
	assert.Equal(t, expected, body, "SSE 数据应按追加顺序排列")
}

// TestComparisonWithRestEndpoint compares the behavior consistency of fasthttp and standard http
// Standard HTTP's ResponseWriter.Write is an addition behavior, and fasthttp should remain consistent
func TestComparisonWithRestEndpoint(t *testing.T) {
	// Simulates the behavior of standard HTTP (httptest.ResponseRecorder.Write Additional Data)
	recorder := httptest.NewRecorder()
	recorder.Write([]byte("data: chunk1\n\n"))
	recorder.Write([]byte("data: chunk2\n\n"))
	recorder.Write([]byte("data: chunk3\n\n"))
	restBody := recorder.Body.String()

	// Testing fasthttp's behavior
	var ctx fasthttp.RequestCtx
	resp := &ResponseMessage{ctx: &ctx}
	resp.SetBody([]byte("data: chunk1\n\n"))
	resp.SetBody([]byte("data: chunk2\n\n"))
	resp.SetBody([]byte("data: chunk3\n\n"))
	fasthttpBody := string(ctx.Response.Body())

	assert.Equal(t, restBody, fasthttpBody,
		"fasthttp 和 rest 的 SetBody 行为应一致（追加而非替换）")
}

// TestSSEHeaders tests whether SSE headers settings are correct
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

// TestSSEFullFlow tests the complete SSE streaming response flow (simulates the behavior of the openaiStreamingResponse processor)
func TestSSEFullFlow(t *testing.T) {
	var ctx fasthttp.RequestCtx
	resp := &ResponseMessage{ctx: &ctx}

	// 1. Set SSE headers (corresponding to setSSEHeaders)
	resp.SetHeader("Content-Type", "text/event-stream")
	resp.SetHeader("Cache-Control", "no-cache")
	resp.SetHeader("Connection", "keep-alive")

	// 2. Send multiple SSE chunks (corresponding to handleChunk)
	resp.SetBody([]byte("data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n"))
	resp.Flush()

	resp.SetBody([]byte("data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" World\"},\"finish_reason\":null}]}\n\n"))
	resp.Flush()

	// 3. Send a completion signal (corresponding to handleCompletion)
	resp.SetBody([]byte("data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	resp.Flush()

	// Verify complete responses
	body := string(ctx.Response.Body())

	// Verify that all chunk and completion signals exist
	assert.True(t, strings.Contains(body, `"content":"Hello"`), "应包含第一个 chunk")
	assert.True(t, strings.Contains(body, `"content":" World"`), "应包含第二个 chunk")
	assert.True(t, strings.Contains(body, `"finish_reason":"stop"`), "应包含完成信号")
	assert.True(t, strings.Contains(body, "[DONE]"), "应包含 [DONE] 标记")

	// Verify the SSE headers correctly
	contentType := string(ctx.Response.Header.Peek("Content-Type"))
	assert.Equal(t, "text/event-stream", contentType)
}

// TestFlush_DoesNotCorruptData Test Flush without corrupting existing data
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

// TestSetBody_NonStreaming Testing non-streaming scenes (single SetBody)
func TestSetBody_NonStreaming(t *testing.T) {
	var ctx fasthttp.RequestCtx
	resp := &ResponseMessage{ctx: &ctx}

	resp.SetBody([]byte(`{"result":"ok"}`))

	body := string(ctx.Response.Body())
	assert.Equal(t, `{"result":"ok"}`, body, "非流式单次 SetBody 应正常工作")
	assert.Equal(t, `{"result":"ok"}`, string(resp.Body()), "Body() 应返回最后一次设置的内容")
}

// TestSetBody_EmptyBody Testing empty bodies does not affect existing data
func TestSetBody_EmptyBody(t *testing.T) {
	var ctx fasthttp.RequestCtx
	resp := &ResponseMessage{ctx: &ctx}

	resp.SetBody([]byte("data: chunk1\n\n"))
	resp.SetBody([]byte("")) // Empty body

	body := string(ctx.Response.Body())
	// The empty body should not corrupt previous data (adding empty bytes does not change the content).
	assert.Contains(t, body, "data: chunk1\n\n")
}
