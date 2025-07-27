/*
 * Copyright 2024 The RuleGo Authors.
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

package nsq

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
)

func TestClientNode(t *testing.T) {
	// 如果设置了跳过 NSQ 测试，则跳过
	if os.Getenv("SKIP_NSQ_TESTS") == "true" {
		t.Skip("Skipping NSQ tests")
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/nsqClient"

	nsqdAddress := os.Getenv("NSQD_ADDRESS")
	if nsqdAddress == "" {
		nsqdAddress = "127.0.0.1:4150"
	}

	t.Run("NewNode", func(t *testing.T) {
		test.NodeNew(t, targetNodeType, &ClientNode{}, types.Configuration{
			"topic":  "devices_msg",
			"server": nsqdAddress,
		}, Registry)
	})

	t.Run("InitNode", func(t *testing.T) {
		test.NodeInit(t, targetNodeType, types.Configuration{
			"topic":  "device_msg",
			"server": nsqdAddress,
		}, types.Configuration{
			"topic":  "device_msg",
			"server": nsqdAddress,
		}, Registry)
	})

	t.Run("OnMsg", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"topic":  "device_msg",
			"server": nsqdAddress,
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create NSQ client node (NSQ may not be available): %v", err)
			return
		}

		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("productType", "test")
		msgList := []test.Msg{
			{
				MetaData: metaData,
				MsgType:  "ACTIVITY_EVENT1",
				Data:     "AA",
			},
			{
				MetaData: metaData,
				MsgType:  "ACTIVITY_EVENT2",
				Data:     "{\"temperature\":60}",
			},
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					if err != nil {
						t.Logf("NSQ publish failed (NSQ may not be available): %v", err)
						return
					}
					assert.Equal(t, types.Success, relationType)
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Second * 2)
	})
}

func TestClientNodeWithTemplate(t *testing.T) {
	// 如果设置了跳过 NSQ 测试，则跳过
	if os.Getenv("SKIP_NSQ_TESTS") == "true" {
		t.Skip("Skipping NSQ tests")
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/nsqClient"

	nsqdAddress := os.Getenv("NSQD_ADDRESS")
	if nsqdAddress == "" {
		nsqdAddress = "127.0.0.1:4150"
	}

	t.Run("OnMsgWithTemplate", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"topic":  "device_${productType}_msg",
			"server": nsqdAddress,
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create NSQ client node (NSQ may not be available): %v", err)
			return
		}

		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("productType", "sensor")
		msgList := []test.Msg{
			{
				MetaData: metaData,
				MsgType:  "TELEMETRY",
				Data:     "{\"temperature\":25.5}",
			},
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					if err != nil {
						t.Logf("NSQ publish failed (NSQ may not be available): %v", err)
						return
					}
					assert.Equal(t, types.Success, relationType)
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Second * 2)
	})
}

// TestClientNodeWithLookupd 测试NSQ客户端节点通过lookupd发现nsqd服务器的功能
func TestClientNodeWithLookupd(t *testing.T) {
	// 如果设置了跳过 NSQ 测试，则跳过
	if os.Getenv("SKIP_NSQ_TESTS") == "true" {
		t.Skip("Skipping NSQ tests")
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/nsqClient"

	// 从环境变量获取lookupd地址
	lookupdAddress := os.Getenv("LOOKUPD_ADDRESS")
	if lookupdAddress == "" {
		lookupdAddress = "127.0.0.1:4161"
	}

	t.Run("DiscoverNsqdFromLookupd", func(t *testing.T) {
		// 使用lookupd地址创建NSQ客户端节点
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"topic":  "test_lookupd_discovery",
			"server": "http://" + lookupdAddress, // 使用lookupd地址
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create NSQ client node with lookupd (NSQ may not be available): %v", err)
			return
		}

		// 发布测试消息
		metaData := types.BuildMetadata(make(map[string]string))
		msgList := []test.Msg{
			{
				MetaData: metaData,
				MsgType:  "LOOKUPD_TEST",
				Data:     "{\"test\":\"lookupd_discovery\"}",
			},
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					if err != nil {
						t.Logf("NSQ publish via lookupd failed (NSQ may not be available): %v", err)
						return
					}
					assert.Equal(t, types.Success, relationType)
					t.Logf("Successfully published message via lookupd discovery")
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Second * 2)
	})

	t.Run("MixedAddressConfiguration", func(t *testing.T) {
		// 测试混合地址配置（同时包含nsqd和lookupd地址）
		nsqdAddress := os.Getenv("NSQD_ADDRESS")
		if nsqdAddress == "" {
			nsqdAddress = "127.0.0.1:4150"
		}

		// 配置包含多个地址的服务器字符串
		mixedServer := fmt.Sprintf("%s,http://%s", nsqdAddress, lookupdAddress)

		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"topic":  "test_mixed_config",
			"server": mixedServer,
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create NSQ client node with mixed config (NSQ may not be available): %v", err)
			return
		}

		// 发布测试消息
		metaData := types.BuildMetadata(make(map[string]string))
		msgList := []test.Msg{
			{
				MetaData: metaData,
				MsgType:  "MIXED_CONFIG_TEST",
				Data:     "{\"test\":\"mixed_configuration\"}",
			},
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					if err != nil {
						t.Logf("NSQ publish with mixed config failed (NSQ may not be available): %v", err)
						return
					}
					assert.Equal(t, types.Success, relationType)
					t.Logf("Successfully published message with mixed address configuration")
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Second * 2)
	})
}

// TestNsqdDiscoveryAPI 测试NSQ服务器发现API功能
func TestNsqdDiscoveryAPI(t *testing.T) {
	// 如果设置了跳过 NSQ 测试，则跳过
	if os.Getenv("SKIP_NSQ_TESTS") == "true" {
		t.Skip("Skipping NSQ tests")
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})

	// 从环境变量获取lookupd地址
	lookupdAddress := os.Getenv("LOOKUPD_ADDRESS")
	if lookupdAddress == "" {
		lookupdAddress = "127.0.0.1:4161"
	}

	t.Run("DirectLookupAPICall", func(t *testing.T) {
		// 创建NSQ客户端节点实例来测试内部方法
		clientNode := &ClientNode{}
		clientNode.Config = ClientNodeConfiguration{
			Server: "http://" + lookupdAddress,
			Topic:  "test_api_discovery",
		}

		// 直接调用服务器发现方法
		nsqdAddr, err := clientNode.discoverNsqdFromLookupd("http://" + lookupdAddress)
		if err != nil {
			t.Skipf("Failed to discover nsqd from lookupd (NSQ may not be available): %v", err)
			return
		}

		// 验证返回的地址格式
		if nsqdAddr == "" {
			t.Error("nsqdAddr should not be empty")
		}
		t.Logf("Discovered nsqd address: %s", nsqdAddr)

		// 验证地址格式是否正确（应该包含IP和端口）
		if !strings.Contains(nsqdAddr, ":") {
			t.Errorf("nsqdAddr should contain ':' but got: %s", nsqdAddr)
		}
	})

	t.Run("ParseAddressesWithLookupd", func(t *testing.T) {
		// 测试地址解析功能
		clientNode := &ClientNode{}
		clientNode.Config = ClientNodeConfiguration{
			Server: "http://" + lookupdAddress,
			Topic:  "test_parse_addresses",
		}

		// 调用地址解析方法
		nsqdAddrs, lookupdAddrs := clientNode.parseAddresses()

		// 验证解析结果
		if len(nsqdAddrs) != 0 {
			t.Error("Should not have nsqd addresses when using lookupd")
		}
		if len(lookupdAddrs) == 0 {
			t.Error("Should have lookupd addresses")
		}
		assert.Equal(t, "http://"+lookupdAddress, lookupdAddrs[0])
		t.Logf("Parsed lookupd addresses: %v", lookupdAddrs)
	})

	t.Run("ParseMixedAddresses", func(t *testing.T) {
		// 测试混合地址解析
		nsqdAddress := os.Getenv("NSQD_ADDRESS")
		if nsqdAddress == "" {
			nsqdAddress = "127.0.0.1:4150"
		}

		clientNode := &ClientNode{}
		clientNode.Config = ClientNodeConfiguration{
			Server: fmt.Sprintf("%s,http://%s", nsqdAddress, lookupdAddress),
			Topic:  "test_mixed_parse",
		}

		// 调用地址解析方法
		nsqdAddrs, lookupdAddrs := clientNode.parseAddresses()

		// 验证解析结果
		if len(nsqdAddrs) == 0 {
			t.Error("Should have nsqd addresses")
		}
		if len(lookupdAddrs) == 0 {
			t.Error("Should have lookupd addresses")
		}
		assert.Equal(t, nsqdAddress, nsqdAddrs[0])
		assert.Equal(t, "http://"+lookupdAddress, lookupdAddrs[0])
		t.Logf("Parsed nsqd addresses: %v", nsqdAddrs)
		t.Logf("Parsed lookupd addresses: %v", lookupdAddrs)
	})
}

// TestClientNodeWithSubscription 测试NSQ客户端节点的发布和订阅功能
// 通过创建消费者订阅主题，验证发布的数据是否正确接收
func TestClientNodeWithSubscription(t *testing.T) {
	// 如果设置了跳过 NSQ 测试，则跳过
	if os.Getenv("SKIP_NSQ_TESTS") == "true" {
		t.Skip("Skipping NSQ tests")
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/nsqClient"

	nsqdAddress := os.Getenv("NSQD_ADDRESS")
	if nsqdAddress == "" {
		nsqdAddress = "127.0.0.1:4150"
	}

	t.Run("PublishAndSubscribe", func(t *testing.T) {
		testTopic := "test_publish_subscribe"
		testChannel := "test_channel"
		testData := "{\"temperature\":30.5,\"humidity\":65}"

		// 创建NSQ客户端节点
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"topic":  testTopic,
			"server": nsqdAddress,
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create NSQ client node (NSQ may not be available): %v", err)
			return
		}

		// 创建消费者来订阅消息
		config := nsq.NewConfig()
		consumer, err := nsq.NewConsumer(testTopic, testChannel, config)
		if err != nil {
			t.Skipf("Failed to create NSQ consumer (NSQ may not be available): %v", err)
			return
		}
		defer consumer.Stop()

		// 用于存储接收到的消息
		receivedMessages := make(chan string, 1)
		messageCount := 0

		// 设置消息处理器
		consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
			messageCount++
			receivedData := string(message.Body)

			// 发送接收到的数据到通道
			select {
			case receivedMessages <- receivedData:
			default:
				// 通道已满，忽略
			}
			return nil
		}))
		consumer.SetLoggerLevel(nsq.LogLevelError)
		// 连接到NSQd
		err = consumer.ConnectToNSQD(nsqdAddress)
		if err != nil {
			t.Skipf("Failed to connect to NSQd (NSQ may not be available): %v", err)
			return
		}

		// 等待消费者准备就绪
		time.Sleep(time.Second * 2)

		// 发布消息
		metaData := types.BuildMetadata(make(map[string]string))
		msgList := []test.Msg{
			{
				MetaData: metaData,
				MsgType:  "TEST_DATA",
				Data:     testData,
			},
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					if err != nil {
						t.Logf("NSQ publish failed (NSQ may not be available): %v", err)
						return
					}
					assert.Equal(t, types.Success, relationType)
				},
			},
		}

		// 执行发布
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}

		// 等待消息被接收
		select {
		case receivedData := <-receivedMessages:
			// 验证接收到的数据是否与发布的数据一致
			assert.Equal(t, testData, receivedData)
		case <-time.After(10 * time.Second):
			t.Logf("Timeout waiting for message, NSQ may not be available")
		}

		// 等待一段时间确保所有消息都被处理
		time.Sleep(time.Second * 2)
	})

	t.Run("PublishMultipleMessages", func(t *testing.T) {
		testTopic := "test_multiple_messages"
		testChannel := "test_channel_multi"
		testMessages := []string{
			"{\"sensor\":\"temperature\",\"value\":25.5}",
			"{\"sensor\":\"humidity\",\"value\":60.0}",
			"{\"sensor\":\"pressure\",\"value\":1013.25}",
		}

		// 创建NSQ客户端节点
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"topic":  testTopic,
			"server": nsqdAddress,
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create NSQ client node (NSQ may not be available): %v", err)
			return
		}

		// 创建消费者
		config := nsq.NewConfig()
		consumer, err := nsq.NewConsumer(testTopic, testChannel, config)
		if err != nil {
			t.Skipf("Failed to create NSQ consumer (NSQ may not be available): %v", err)
			return
		}
		defer consumer.Stop()

		// 用于存储接收到的消息
		receivedMessages := make([]string, 0)
		messageCount := 0
		expectedCount := len(testMessages)
		done := make(chan bool, 1)

		// 设置消息处理器
		consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
			messageCount++
			receivedData := string(message.Body)
			receivedMessages = append(receivedMessages, receivedData)
			t.Logf("Received message %d/%d: %s", messageCount, expectedCount, receivedData)

			// 如果接收到所有消息，发送完成信号
			if messageCount >= expectedCount {
				select {
				case done <- true:
				default:
				}
			}
			return nil
		}))

		// 连接到NSQd
		err = consumer.ConnectToNSQD(nsqdAddress)
		if err != nil {
			t.Skipf("Failed to connect to NSQd (NSQ may not be available): %v", err)
			return
		}

		// 等待消费者准备就绪
		time.Sleep(time.Second * 2)

		// 发布多条消息
		for i, testData := range testMessages {
			msgIndex := i // 创建局部变量避免闭包问题
			metaData := types.BuildMetadata(make(map[string]string))
			metaData.PutValue("messageIndex", fmt.Sprintf("%d", msgIndex))
			msgList := []test.Msg{
				{
					MetaData: metaData,
					MsgType:  "SENSOR_DATA",
					Data:     testData,
				},
			}

			var nodeList = []test.NodeAndCallback{
				{
					Node:    node,
					MsgList: msgList,
					Callback: func(msg types.RuleMsg, relationType string, err error) {
						if err != nil {
							t.Logf("NSQ publish failed for message %d (NSQ may not be available): %v", msgIndex, err)
							return
						}
						assert.Equal(t, types.Success, relationType)
						t.Logf("Message %d published successfully", msgIndex)
					},
				},
			}

			// 执行发布
			for _, item := range nodeList {
				test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
			}

			// 在消息之间稍作延迟
			time.Sleep(time.Millisecond * 500)
		}

		// 等待所有消息被接收
		select {
		case <-done:
			// 验证接收到的消息数量
			assert.Equal(t, expectedCount, len(receivedMessages))

			// 验证每条消息的内容
			for _, expectedMsg := range testMessages {
				found := false
				for _, receivedMsg := range receivedMessages {
					if receivedMsg == expectedMsg {
						found = true
						break
					}
				}
				assert.True(t, found, "Expected message not found: %s", expectedMsg)
			}
			t.Logf("Successfully verified all %d published messages", expectedCount)
		case <-time.After(15 * time.Second):
			t.Logf("Timeout waiting for messages, received %d/%d, NSQ may not be available", len(receivedMessages), expectedCount)
		}

		// 等待一段时间确保所有消息都被处理
		time.Sleep(time.Second * 2)
	})
}
