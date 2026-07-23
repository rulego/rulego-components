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
	server := ":9094" // Testing was conducted using fixed ports

	// Create a rule engine
	_, err := rulego.New("rule01", []byte(ruleChainFile))
	assert.Nil(t, err)

	// Create a WebSocket endpoint configuration
	config := WebsocketConfig{
		Server:    server,
		AllowCors: true,
	}

	// Create a WebSocket endpoint
	ep, err := endpoint.Registry.New(WebsocketType, rulego.NewConfig(), config)
	assert.Nil(t, err)

	websocketEndpoint := ep.(*FastHttpWebsocket)

	// Add routes
	router := endpoint.NewRouter().From("/ws/:userId").Transform(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// Handles websocket messages
		msg := exchange.In.GetMsg()
		msg.Type = "TEST"
		msg.SetData(fmt.Sprintf("echo: %s", msg.GetData()))
		exchange.Out.SetBody([]byte(msg.GetData()))
		return true
	}).To("chain:rule01").End()

	_, err = websocketEndpoint.AddRouter(router)
	assert.Nil(t, err)

	// Start the WebSocket server
	err = websocketEndpoint.Start()
	assert.Nil(t, err)

	// Wait for the server to start
	time.Sleep(200 * time.Millisecond)

	// Test the WebSocket connection
	t.Run("websocket connection test", func(t *testing.T) {
		// Create a WebSocket client connection
		u := url.URL{Scheme: "ws", Host: fmt.Sprintf("localhost%s", server), Path: "/ws/user123"}
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		assert.Nil(t, err)
		defer c.Close()

		// Send the message
		testMessage := "Hello WebSocket"
		err = c.WriteMessage(websocket.TextMessage, []byte(testMessage))
		assert.Nil(t, err)

		// Read the response
		_, message, err := c.ReadMessage()
		assert.Nil(t, err)
		expected := fmt.Sprintf("echo: %s", testMessage)
		assert.Equal(t, expected, string(message))
	})

	// Test binary messages
	t.Run("binary message test", func(t *testing.T) {
		u := url.URL{Scheme: "ws", Host: fmt.Sprintf("localhost%s", server), Path: "/ws/user456"}
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		assert.Nil(t, err)
		defer c.Close()

		// Send binary messages
		testData := []byte{0x01, 0x02, 0x03, 0x04}
		err = c.WriteMessage(websocket.BinaryMessage, testData)
		assert.Nil(t, err)

		// Read the response
		_, message, err := c.ReadMessage()
		assert.Nil(t, err)
		expected := fmt.Sprintf("echo: %s", string(testData))
		assert.Equal(t, expected, string(message))
	})

	// Cleanup
	err = websocketEndpoint.Close()
	assert.Nil(t, err)
}

func TestFastHttpWebsocketEndpointWithEvents(t *testing.T) {
	server := ":9095" // Testing was conducted using fixed ports

	// Create a rule engine
	_, err := rulego.New("rule01", []byte(ruleChainFile))
	assert.Nil(t, err)

	// Event counter – uses atomic operations to avoid data contention
	var connectCount, disconnectCount int64

	// Create a WebSocket endpoint configuration
	config := WebsocketConfig{
		Server:    server,
		AllowCors: true,
	}

	// Create a WebSocket endpoint
	ep, err := endpoint.Registry.New(WebsocketType, rulego.NewConfig(), config)
	assert.Nil(t, err)

	websocketEndpoint := ep.(*FastHttpWebsocket)

	// Set up the event handler
	websocketEndpoint.OnEvent = func(eventType string, params ...interface{}) {
		switch eventType {
		case endpointApi.EventConnect:
			atomic.AddInt64(&connectCount, 1)
		case endpointApi.EventDisconnect:
			atomic.AddInt64(&disconnectCount, 1)
		}
	}

	// Add routes
	router := endpoint.NewRouter().From("/ws").Transform(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		msg := exchange.In.GetMsg()
		msg.Type = "TEST"
		msg.SetData(fmt.Sprintf("processed: %s", msg.GetData()))
		exchange.Out.SetBody([]byte(msg.GetData()))
		return true
	}).To("chain:rule01").End()

	_, err = websocketEndpoint.AddRouter(router)
	assert.Nil(t, err)

	// Start the WebSocket server
	err = websocketEndpoint.Start()
	assert.Nil(t, err)

	// Wait for the server to start
	time.Sleep(200 * time.Millisecond)

	// Test connection and disconnect events
	t.Run("connection events test", func(t *testing.T) {
		u := url.URL{Scheme: "ws", Host: fmt.Sprintf("localhost%s", server), Path: "/ws"}
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		assert.Nil(t, err)

		// Send the message
		err = c.WriteMessage(websocket.TextMessage, []byte("test message"))
		assert.Nil(t, err)

		// Read the response
		_, message, err := c.ReadMessage()
		assert.Nil(t, err)
		assert.Equal(t, "processed: test message", string(message))

		// Close the connection
		c.Close()

		// Waiting for the event to be handled
		time.Sleep(100 * time.Millisecond)

		// Verify event counts
		assert.Equal(t, int64(1), atomic.LoadInt64(&connectCount))
		assert.Equal(t, int64(1), atomic.LoadInt64(&disconnectCount))
	})

	// Cleanup
	err = websocketEndpoint.Close()
	assert.Nil(t, err)
}

func TestFastHttpWebsocketEndpointParams(t *testing.T) {
	server := ":9096" // Testing was conducted using fixed ports

	// Create a rule engine
	_, err := rulego.New("rule01", []byte(ruleChainFile))
	assert.Nil(t, err)

	// Create a WebSocket endpoint configuration
	config := WebsocketConfig{
		Server:    server,
		AllowCors: true,
	}

	// Create a WebSocket endpoint
	ep, err := endpoint.Registry.New(WebsocketType, rulego.NewConfig(), config)
	assert.Nil(t, err)

	websocketEndpoint := ep.(*FastHttpWebsocket)

	// Add a route with parameters
	router := endpoint.NewRouter().From("/ws/:roomId/:userId").Transform(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		msg := exchange.In.GetMsg()
		// Obtain path parameters
		roomId := msg.Metadata.GetValue("roomId")
		userId := msg.Metadata.GetValue("userId")
		msg.SetData(fmt.Sprintf("room:%s,user:%s,data:%s", roomId, userId, msg.GetData()))
		exchange.Out.SetBody([]byte(msg.GetData()))
		return true
	}).To("chain:rule01").End()

	_, err = websocketEndpoint.AddRouter(router)
	assert.Nil(t, err)

	// Start the WebSocket server
	err = websocketEndpoint.Start()
	assert.Nil(t, err)

	// Wait for the server to start
	time.Sleep(200 * time.Millisecond)

	// Test path parameters
	t.Run("path parameters test", func(t *testing.T) {
		u := url.URL{Scheme: "ws", Host: fmt.Sprintf("localhost%s", server), Path: "/ws/room123/user456"}
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		assert.Nil(t, err)
		defer c.Close()

		// Send the message
		testMessage := "hello"
		err = c.WriteMessage(websocket.TextMessage, []byte(testMessage))
		assert.Nil(t, err)

		// Read the response
		_, message, err := c.ReadMessage()
		assert.Nil(t, err)
		expected := "room:room123,user:user456,data:hello"
		assert.Equal(t, expected, string(message))
	})

	// Cleanup
	err = websocketEndpoint.Close()
	assert.Nil(t, err)
}

// Rule chain configuration for testing
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
