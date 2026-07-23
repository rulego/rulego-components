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
	// If NSQ testing is set to skip, skip it
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
	// If NSQ testing is set to skip, skip it
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

// TestClientNodeWithLookupd Test the NSQ client node by discovering the NSQD server's functionality through lookupd
func TestClientNodeWithLookupd(t *testing.T) {
	// If NSQ testing is set to skip, skip it
	if os.Getenv("SKIP_NSQ_TESTS") == "true" {
		t.Skip("Skipping NSQ tests")
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/nsqClient"

	// Retrieves the lookupd address from the environment variable
	lookupdAddress := os.Getenv("LOOKUPD_ADDRESS")
	if lookupdAddress == "" {
		lookupdAddress = "127.0.0.1:4161"
	}

	t.Run("DiscoverNsqdFromLookupd", func(t *testing.T) {
		// Create NSQ client nodes using lookupd addresses
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"topic":  "test_lookupd_discovery",
			"server": "http://" + lookupdAddress, // Use lookupd addresses
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create NSQ client node with lookupd (NSQ may not be available): %v", err)
			return
		}

		// Release test information
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
		// Test hybrid address configuration (including both nsqd and lookupd addresses)
		nsqdAddress := os.Getenv("NSQD_ADDRESS")
		if nsqdAddress == "" {
			nsqdAddress = "127.0.0.1:4150"
		}

		// Configure server strings containing multiple addresses
		mixedServer := fmt.Sprintf("%s,http://%s", nsqdAddress, lookupdAddress)

		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"topic":  "test_mixed_config",
			"server": mixedServer,
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create NSQ client node with mixed config (NSQ may not be available): %v", err)
			return
		}

		// Release test information
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

// TestNsqdDiscoveryAPI tests NSQ server discovery API functionality
func TestNsqdDiscoveryAPI(t *testing.T) {
	// If NSQ testing is set to skip, skip it
	if os.Getenv("SKIP_NSQ_TESTS") == "true" {
		t.Skip("Skipping NSQ tests")
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})

	// Retrieves the lookupd address from the environment variable
	lookupdAddress := os.Getenv("LOOKUPD_ADDRESS")
	if lookupdAddress == "" {
		lookupdAddress = "127.0.0.1:4161"
	}

	t.Run("DirectLookupAPICall", func(t *testing.T) {
		// Create NSQ client node instances to test internal methods
		clientNode := &ClientNode{}
		clientNode.Config = ClientNodeConfiguration{
			Server: "http://" + lookupdAddress,
			Topic:  "test_api_discovery",
		}

		// Directly call the server discovery method
		nsqdAddr, err := clientNode.discoverNsqdFromLookupd("http://" + lookupdAddress)
		if err != nil {
			t.Skipf("Failed to discover nsqd from lookupd (NSQ may not be available): %v", err)
			return
		}

		// Verify the format of the returned address
		if nsqdAddr == "" {
			t.Error("nsqdAddr should not be empty")
		}
		t.Logf("Discovered nsqd address: %s", nsqdAddr)

		// Verify the address format is correct (it should include IP and port)
		if !strings.Contains(nsqdAddr, ":") {
			t.Errorf("nsqdAddr should contain ':' but got: %s", nsqdAddr)
		}
	})

	t.Run("ParseAddressesWithLookupd", func(t *testing.T) {
		// Test address resolution function
		clientNode := &ClientNode{}
		clientNode.Config = ClientNodeConfiguration{
			Server: "http://" + lookupdAddress,
			Topic:  "test_parse_addresses",
		}

		// Call address resolution methods
		nsqdAddrs, lookupdAddrs := clientNode.parseAddresses()

		// Verify the parsing results
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
		// Test mixed address resolution
		nsqdAddress := os.Getenv("NSQD_ADDRESS")
		if nsqdAddress == "" {
			nsqdAddress = "127.0.0.1:4150"
		}

		clientNode := &ClientNode{}
		clientNode.Config = ClientNodeConfiguration{
			Server: fmt.Sprintf("%s,http://%s", nsqdAddress, lookupdAddress),
			Topic:  "test_mixed_parse",
		}

		// Call address resolution methods
		nsqdAddrs, lookupdAddrs := clientNode.parseAddresses()

		// Verify the parsing results
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

// TestClientNodeWithSubscription: Tests the publishing and subscription functions of NSQ client nodes
// By creating consumer subscription topics, verify whether the published data is correctly received
func TestClientNodeWithSubscription(t *testing.T) {
	// If NSQ testing is set to skip, skip it
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

		// Create an NSQ client node
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"topic":  testTopic,
			"server": nsqdAddress,
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create NSQ client node (NSQ may not be available): %v", err)
			return
		}

		// Create consumers to subscribe to messages
		config := nsq.NewConfig()
		consumer, err := nsq.NewConsumer(testTopic, testChannel, config)
		if err != nil {
			t.Skipf("Failed to create NSQ consumer (NSQ may not be available): %v", err)
			return
		}
		defer consumer.Stop()

		// Used to store received messages
		receivedMessages := make(chan string, 1)
		messageCount := 0

		// Set up the message processor
		consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
			messageCount++
			receivedData := string(message.Body)

			// Send the received data to the channel
			select {
			case receivedMessages <- receivedData:
			default:
				// The channel is full, ignored
			}
			return nil
		}))
		consumer.SetLoggerLevel(nsq.LogLevelError)
		// Connect to NSQd
		err = consumer.ConnectToNSQD(nsqdAddress)
		if err != nil {
			t.Skipf("Failed to connect to NSQd (NSQ may not be available): %v", err)
			return
		}

		// Wait for consumers to be ready
		time.Sleep(time.Second * 2)

		// Release the news
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

		// Execution release
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}

		// Waiting for the message to be received
		select {
		case receivedData := <-receivedMessages:
			// Verify whether the received data matches the published data
			assert.Equal(t, testData, receivedData)
		case <-time.After(10 * time.Second):
			t.Logf("Timeout waiting for message, NSQ may not be available")
		}

		// Wait a while to ensure all messages are handled
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

		// Create an NSQ client node
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"topic":  testTopic,
			"server": nsqdAddress,
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create NSQ client node (NSQ may not be available): %v", err)
			return
		}

		// Create consumers
		config := nsq.NewConfig()
		consumer, err := nsq.NewConsumer(testTopic, testChannel, config)
		if err != nil {
			t.Skipf("Failed to create NSQ consumer (NSQ may not be available): %v", err)
			return
		}
		defer consumer.Stop()

		// Used to store received messages
		receivedMessages := make([]string, 0)
		messageCount := 0
		expectedCount := len(testMessages)
		done := make(chan bool, 1)

		// Set up the message processor
		consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
			messageCount++
			receivedData := string(message.Body)
			receivedMessages = append(receivedMessages, receivedData)
			t.Logf("Received message %d/%d: %s", messageCount, expectedCount, receivedData)

			// If all messages are received, send a completion signal
			if messageCount >= expectedCount {
				select {
				case done <- true:
				default:
				}
			}
			return nil
		}))

		// Connect to NSQd
		err = consumer.ConnectToNSQD(nsqdAddress)
		if err != nil {
			t.Skipf("Failed to connect to NSQd (NSQ may not be available): %v", err)
			return
		}

		// Wait for consumers to be ready
		time.Sleep(time.Second * 2)

		// Multiple announcements were released
		for i, testData := range testMessages {
			msgIndex := i // Creating local variables to avoid closure issues
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

			// Execution release
			for _, item := range nodeList {
				test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
			}

			// There is a slight delay between messages
			time.Sleep(time.Millisecond * 500)
		}

		// Wait for all messages to be received
		select {
		case <-done:
			// Verify the number of messages received
			assert.Equal(t, expectedCount, len(receivedMessages))

			// Verify the content of each message
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

		// Wait a while to ensure all messages are handled
		time.Sleep(time.Second * 2)
	})
}
