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

import (
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fasthttp/websocket"
	"github.com/rulego/rulego"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/test/assert"
)

func TestFastHttpWebsocketEndpoint(t *testing.T) {
	server := ":9094" // 使用固定端口进行测试

	// 创建规则引擎
	_, err := rulego.New("rule01", []byte(ruleChainFile))
	assert.Nil(t, err)

	// 创建websocket端点配置
	config := WebsocketConfig{
		Server:    server,
		AllowCors: true,
	}

	// 创建websocket端点
	ep, err := endpoint.Registry.New(WebsocketType, rulego.NewConfig(), config)
	assert.Nil(t, err)

	websocketEndpoint := ep.(*FastHttpWebsocket)

	// 添加路由
	router := endpoint.NewRouter().From("/ws/:userId").Transform(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// 处理websocket消息
		msg := exchange.In.GetMsg()
		msg.Type = "TEST"
		msg.SetData(fmt.Sprintf("echo: %s", msg.GetData()))
		exchange.Out.SetBody([]byte(msg.GetData()))
		return true
	}).To("chain:rule01").End()

	_, err = websocketEndpoint.AddRouter(router)
	assert.Nil(t, err)

	// 启动websocket服务器
	err = websocketEndpoint.Start()
	assert.Nil(t, err)

	// 等待服务器启动
	time.Sleep(200 * time.Millisecond)

	// 测试websocket连接
	t.Run("websocket connection test", func(t *testing.T) {
		// 创建websocket客户端连接
		u := url.URL{Scheme: "ws", Host: fmt.Sprintf("localhost%s", server), Path: "/ws/user123"}
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		assert.Nil(t, err)
		defer c.Close()

		// 发送消息
		testMessage := "Hello WebSocket"
		err = c.WriteMessage(websocket.TextMessage, []byte(testMessage))
		assert.Nil(t, err)

		// 读取响应
		_, message, err := c.ReadMessage()
		assert.Nil(t, err)
		expected := fmt.Sprintf("echo: %s", testMessage)
		assert.Equal(t, expected, string(message))
	})

	// 测试二进制消息
	t.Run("binary message test", func(t *testing.T) {
		u := url.URL{Scheme: "ws", Host: fmt.Sprintf("localhost%s", server), Path: "/ws/user456"}
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		assert.Nil(t, err)
		defer c.Close()

		// 发送二进制消息
		testData := []byte{0x01, 0x02, 0x03, 0x04}
		err = c.WriteMessage(websocket.BinaryMessage, testData)
		assert.Nil(t, err)

		// 读取响应
		_, message, err := c.ReadMessage()
		assert.Nil(t, err)
		expected := fmt.Sprintf("echo: %s", string(testData))
		assert.Equal(t, expected, string(message))
	})

	// 清理
	err = websocketEndpoint.Close()
	assert.Nil(t, err)
}

func TestFastHttpWebsocketEndpointWithEvents(t *testing.T) {
	server := ":9095" // 使用固定端口进行测试

	// 创建规则引擎
	_, err := rulego.New("rule01", []byte(ruleChainFile))
	assert.Nil(t, err)

	// 事件计数器 - 使用原子操作避免数据竞争
	var connectCount, disconnectCount int64

	// 创建websocket端点配置
	config := WebsocketConfig{
		Server:    server,
		AllowCors: true,
	}

	// 创建websocket端点
	ep, err := endpoint.Registry.New(WebsocketType, rulego.NewConfig(), config)
	assert.Nil(t, err)

	websocketEndpoint := ep.(*FastHttpWebsocket)

	// 设置事件处理器
	websocketEndpoint.OnEvent = func(eventType string, params ...interface{}) {
		switch eventType {
		case endpointApi.EventConnect:
			atomic.AddInt64(&connectCount, 1)
		case endpointApi.EventDisconnect:
			atomic.AddInt64(&disconnectCount, 1)
		}
	}

	// 添加路由
	router := endpoint.NewRouter().From("/ws").Transform(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		msg := exchange.In.GetMsg()
		msg.Type = "TEST"
		msg.SetData(fmt.Sprintf("processed: %s", msg.GetData()))
		exchange.Out.SetBody([]byte(msg.GetData()))
		return true
	}).To("chain:rule01").End()

	_, err = websocketEndpoint.AddRouter(router)
	assert.Nil(t, err)

	// 启动websocket服务器
	err = websocketEndpoint.Start()
	assert.Nil(t, err)

	// 等待服务器启动
	time.Sleep(200 * time.Millisecond)

	// 测试连接和断开事件
	t.Run("connection events test", func(t *testing.T) {
		u := url.URL{Scheme: "ws", Host: fmt.Sprintf("localhost%s", server), Path: "/ws"}
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		assert.Nil(t, err)

		// 发送消息
		err = c.WriteMessage(websocket.TextMessage, []byte("test message"))
		assert.Nil(t, err)

		// 读取响应
		_, message, err := c.ReadMessage()
		assert.Nil(t, err)
		assert.Equal(t, "processed: test message", string(message))

		// 关闭连接
		c.Close()

		// 等待事件处理
		time.Sleep(100 * time.Millisecond)

		// 验证事件计数
		assert.Equal(t, int64(1), atomic.LoadInt64(&connectCount))
		assert.Equal(t, int64(1), atomic.LoadInt64(&disconnectCount))
	})

	// 清理
	err = websocketEndpoint.Close()
	assert.Nil(t, err)
}

func TestFastHttpWebsocketEndpointParams(t *testing.T) {
	server := ":9096" // 使用固定端口进行测试

	// 创建规则引擎
	_, err := rulego.New("rule01", []byte(ruleChainFile))
	assert.Nil(t, err)

	// 创建websocket端点配置
	config := WebsocketConfig{
		Server:    server,
		AllowCors: true,
	}

	// 创建websocket端点
	ep, err := endpoint.Registry.New(WebsocketType, rulego.NewConfig(), config)
	assert.Nil(t, err)

	websocketEndpoint := ep.(*FastHttpWebsocket)

	// 添加带参数的路由
	router := endpoint.NewRouter().From("/ws/:roomId/:userId").Transform(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		msg := exchange.In.GetMsg()
		// 获取路径参数
		roomId := msg.Metadata.GetValue("roomId")
		userId := msg.Metadata.GetValue("userId")
		msg.SetData(fmt.Sprintf("room:%s,user:%s,data:%s", roomId, userId, msg.GetData()))
		exchange.Out.SetBody([]byte(msg.GetData()))
		return true
	}).To("chain:rule01").End()

	_, err = websocketEndpoint.AddRouter(router)
	assert.Nil(t, err)

	// 启动websocket服务器
	err = websocketEndpoint.Start()
	assert.Nil(t, err)

	// 等待服务器启动
	time.Sleep(200 * time.Millisecond)

	// 测试路径参数
	t.Run("path parameters test", func(t *testing.T) {
		u := url.URL{Scheme: "ws", Host: fmt.Sprintf("localhost%s", server), Path: "/ws/room123/user456"}
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		assert.Nil(t, err)
		defer c.Close()

		// 发送消息
		testMessage := "hello"
		err = c.WriteMessage(websocket.TextMessage, []byte(testMessage))
		assert.Nil(t, err)

		// 读取响应
		_, message, err := c.ReadMessage()
		assert.Nil(t, err)
		expected := "room:room123,user:user456,data:hello"
		assert.Equal(t, expected, string(message))
	})

	// 清理
	err = websocketEndpoint.Close()
	assert.Nil(t, err)
}

// 测试用的规则链配置
const ruleChainFile = `
{
  "ruleChain": {
    "id": "rule01",
    "name": "测试规则链",
    "root": true
  },
  "metadata": {
    "nodes": [
      {
        "id": "s1",
        "type": "jsTransform",
        "name": "转换",
        "debugMode": true,
        "configuration": {
          "jsScript": "metadata['test']='test02';metadata['index']=50;msgType='TEST_MSG_TYPE2';var msg2=JSON.parse(msg);msg2.aa='bb';return {'msg':msg2,'metadata':metadata,'msgType':msgType};"
        }
      }
    ],
    "connections": [
      {
        "fromId": "s1",
        "toId": "chain:rule01",
        "type": "Success"
      }
    ]
  }
}
`
