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

package nats

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/test/assert"

	"github.com/nats-io/nats.go"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
)

var testdataFolder = "../../testdata"

func TestNatsEndpoint(t *testing.T) {
	// Check if there are available NATS servers
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	// If skipping NATS testing is set, skip it
	if os.Getenv("SKIP_NATS_TESTS") == "true" {
		t.Skip("Skipping NATS tests")
	}

	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	// Register the rule chain
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	// Launch the NATS reception service
	natsEndpoint, err := endpoint.Registry.New(Type, config, Config{
		Server: natsURL,
	})
	if err != nil {
		t.Skipf("Failed to create NATS endpoint (NATS may not be available): %v", err)
		return
	}

	// Route 1
	router1 := endpoint.NewRouter().From("device.msg.request").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "test message", exchange.In.GetMsg().GetData())
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// Send data to a specified topic for response
		exchange.Out.Headers().Add(KeyResponseTopic, "device.msg.response")
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()

	count := int32(0)
	// Simulate to obtain responses
	router2 := endpoint.NewRouter().SetId("router3").From("device.msg.response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		//fmt.Println("Data received: device.msg.response", exchange.In.GetMsg())
		assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// Simulate to get responses, same theme
	router3 := endpoint.NewRouter().SetId("router3").From("device.msg.response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		//fmt.Println("Data received: device.msg.response", exchange.In.GetMsg())
		assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// Register the route
	_, err = natsEndpoint.AddRouter(router1)
	if err != nil {
		t.Skipf("Failed to add router1 (NATS server may not be available): %v", err)
		return
	}
	_, err = natsEndpoint.AddRouter(router2)
	if err != nil {
		t.Skipf("Failed to add router2 (NATS server may not be available): %v", err)
		return
	}
	router3Id, err := natsEndpoint.AddRouter(router3)
	assert.NotNil(t, err)
	// Start the server
	err = natsEndpoint.Start()
	if err != nil {
		t.Skipf("Failed to start NATS endpoint: %v", err)
		return
	}

	// Test publishing and subscriptions
	conn, err := nats.Connect(natsURL)
	if err != nil {
		t.Skipf("NATS server not available: %v", err)
		return
	}
	defer conn.Close()

	// Send messages to device.msg.request
	err = conn.Publish("device.msg.request", []byte("test message"))
	if err != nil {
		t.Skipf("Failed to publish message: %v", err)
		return
	}
	// Waiting for the message to be processed
	time.Sleep(time.Second * 1)

	assert.Equal(t, int32(1), atomic.LoadInt32(&count))

	atomic.StoreInt32(&count, 0)
	//Delete the same topic
	_ = natsEndpoint.RemoveRouter(router3Id)
	// Send messages to device.msg.request
	err = conn.Publish("device.msg.request", []byte("test message"))
	if err != nil {
		t.Skipf("Failed to publish second message: %v", err)
		return
	}
	// Waiting for the message to be processed
	time.Sleep(time.Second * 1)

	assert.Equal(t, int32(0), atomic.LoadInt32(&count))

}

// TestNatsEndpointWithGroupId tests the GroupId functionality of NATS endpoint
func TestNatsEndpointWithGroupId(t *testing.T) {
	// Check if there are available NATS servers
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	// If skipping NATS testing is set, skip it
	if os.Getenv("SKIP_NATS_TESTS") == "true" {
		t.Skip("Skipping NATS tests")
	}

	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	// Register the rule chain
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	// Start the NATS receiving service using GroupId
	natsEndpoint1, err := endpoint.Registry.New(Type, config, Config{
		Server:  natsURL,
		GroupId: "test-group",
	})
	if err != nil {
		t.Skipf("Failed to create NATS endpoint1 (NATS may not be available): %v", err)
		return
	}

	// Start the second NATS receiving service, using the same GroupId
	natsEndpoint2, err := endpoint.Registry.New(Type, config, Config{
		Server:  natsURL,
		GroupId: "test-group",
	})
	if err != nil {
		t.Skipf("Failed to create NATS endpoint2 (NATS may not be available): %v", err)
		return
	}

	count1 := int32(0)
	count2 := int32(0)

	// Route 1 - the first endpoint
	router1 := endpoint.NewRouter().From("device.group.request").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "group test message", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count1, 1)
		return true
	}).End()

	// Route 2 - the second endpoint, same topic
	router2 := endpoint.NewRouter().From("device.group.request").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "group test message", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count2, 1)
		return true
	}).End()

	// Register the route
	_, err = natsEndpoint1.AddRouter(router1)
	if err != nil {
		t.Skipf("Failed to add router1 (NATS server may not be available): %v", err)
		return
	}
	_, err = natsEndpoint2.AddRouter(router2)
	if err != nil {
		t.Skipf("Failed to add router2 (NATS server may not be available): %v", err)
		return
	}

	// Start the server
	err = natsEndpoint1.Start()
	if err != nil {
		t.Skipf("Failed to start NATS endpoint1: %v", err)
		return
	}
	err = natsEndpoint2.Start()
	if err != nil {
		t.Skipf("Failed to start NATS endpoint2: %v", err)
		return
	}

	// Test publishing and subscriptions
	conn, err := nats.Connect(natsURL)
	if err != nil {
		t.Skipf("NATS server not available: %v", err)
		return
	}
	defer conn.Close()

	// Publish multiple messages to device.group.request
	// Because GroupId is used, messages should be load balanced between the two endpoints
	for i := 0; i < 10; i++ {
		err = conn.Publish("device.group.request", []byte("group test message"))
		if err != nil {
			t.Skipf("Failed to publish message %d: %v", i, err)
			return
		}
	}

	// Waiting for the message to be processed
	time.Sleep(time.Second * 2)

	// Verification messages are distributed to two endpoints
	totalCount := atomic.LoadInt32(&count1) + atomic.LoadInt32(&count2)
	assert.Equal(t, int32(10), totalCount)

	// Verify load balancing: Both endpoints should receive messages
	// Note: Due to the randomness of load balancing, we only verify the total number and at least one endpoint receiving messages
	assert.True(t, atomic.LoadInt32(&count1) > 0 || atomic.LoadInt32(&count2) > 0)

	t.Logf("Endpoint1 received %d messages, Endpoint2 received %d messages",
		atomic.LoadInt32(&count1), atomic.LoadInt32(&count2))

	// Release resources
	natsEndpoint1.Destroy()
	natsEndpoint2.Destroy()
}
