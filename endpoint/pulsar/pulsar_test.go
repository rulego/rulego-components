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

package pulsar

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/test/assert"
)

var testdataFolder = "../../testdata"

func TestPulsarEndpoint(t *testing.T) {
	// Check if there are available Pulsar servers
	pulsarURL := os.Getenv("PULSAR_URL")
	if pulsarURL == "" {
		pulsarURL = "pulsar://localhost:6650"
	}

	// If you set Skip Pulsar testing, skip it
	if os.Getenv("SKIP_PULSAR_TESTS") == "true" {
		t.Skip("Skipping Pulsar tests")
	}

	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	// Register the rule chain
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	// Launch of Pulsar reception service
	pulsarEndpoint, err := endpoint.Registry.New(Type, config, Config{
		Server: pulsarURL,
	})
	if err != nil {
		t.Skipf("Failed to create Pulsar endpoint (Pulsar may not be available): %v", err)
		return
	}

	// Route 1
	router1 := endpoint.NewRouter().From("device-msg-request").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "test message", exchange.In.GetMsg().GetData())
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// Send data to a specified topic for response
		exchange.Out.Headers().Add(KeyResponseTopic, "device-msg-response")
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()

	count := int32(0)
	// Simulate to obtain responses
	router2 := endpoint.NewRouter().SetId("router2").From("device-msg-response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		//fmt.Println("Data received: device-msg-response", exchange.In.GetMsg())
		assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// Simulate to get responses, same theme
	router3 := endpoint.NewRouter().SetId("router3").From("device-msg-response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		//fmt.Println("Data received: device-msg-response", exchange.In.GetMsg())
		assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// Register the route
	_, err = pulsarEndpoint.AddRouter(router1, "subscription1")
	if err != nil {
		t.Skipf("Failed to add router1 (Pulsar server may not be available): %v", err)
		return
	}
	_, err = pulsarEndpoint.AddRouter(router2, "subscription2")
	if err != nil {
		t.Skipf("Failed to add router2 (Pulsar server may not be available): %v", err)
		return
	}
	router3Id, err := pulsarEndpoint.AddRouter(router3, "subscription2")
	assert.NotNil(t, err)

	// Start the server
	err = pulsarEndpoint.Start()
	if err != nil {
		t.Skipf("Failed to start Pulsar endpoint: %v", err)
		return
	}

	// Waiting for consumers to connect
	time.Sleep(time.Second * 3)

	// Test release announcement
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               pulsarURL,
		ConnectionTimeout: 30 * time.Second,
		OperationTimeout:  30 * time.Second,
	})
	if err != nil {
		t.Skipf("Pulsar server not available: %v", err)
		return
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "device-msg-request",
	})
	if err != nil {
		t.Skipf("Failed to create producer: %v", err)
		return
	}
	defer producer.Close()

	// Send the message to device-msg-request
	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: []byte("test message"),
	})
	if err != nil {
		t.Skipf("Failed to publish message: %v", err)
		return
	}
	// Waiting for the message to be processed
	time.Sleep(time.Second * 5)

	assert.Equal(t, int32(1), atomic.LoadInt32(&count))

	atomic.StoreInt32(&count, 0)
	// Verification of router3 failed, but router3Id should return routerId
	assert.Equal(t, "router3", router3Id)

	// Sending messages again to device-msg-request, it should only be handled by router2
	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: []byte("test message"),
	})
	if err != nil {
		t.Skipf("Failed to publish second message: %v", err)
		return
	}
	// Waiting for the message to be processed
	time.Sleep(time.Second * 5)

	// Because router3 failed to add messages, only router2 handles messages, so count should be 1
	assert.Equal(t, int32(1), atomic.LoadInt32(&count))

	// Cleanup
	pulsarEndpoint.Destroy()
}

func TestPulsarEndpointWithProperties(t *testing.T) {
	// Check if there are available Pulsar servers
	pulsarURL := os.Getenv("PULSAR_URL")
	if pulsarURL == "" {
		pulsarURL = "pulsar://localhost:6650"
	}

	// If you set Skip Pulsar testing, skip it
	if os.Getenv("SKIP_PULSAR_TESTS") == "true" {
		t.Skip("Skipping Pulsar tests")
	}

	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	// Register the rule chain
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	// Launch of Pulsar reception service
	pulsarEndpoint, err := endpoint.Registry.New(Type, config, Config{
		Server: pulsarURL,
	})
	if err != nil {
		t.Skipf("Failed to create Pulsar endpoint (Pulsar may not be available): %v", err)
		return
	}

	count := int32(0)
	// Route
	router := endpoint.NewRouter().From("test-topic").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		msg := exchange.In.GetMsg()
		assert.Equal(t, "test message with properties", msg.GetData())
		// Check custom properties
		assert.Equal(t, "testValue", msg.Metadata.GetValue("customKey"))
		assert.Equal(t, "device123", msg.Metadata.GetValue("deviceId"))
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// Register the route
	_, err = pulsarEndpoint.AddRouter(router, "test-subscription")
	if err != nil {
		t.Skipf("Failed to add router (Pulsar server may not be available): %v", err)
		return
	}

	// Start the server
	err = pulsarEndpoint.Start()
	if err != nil {
		t.Skipf("Failed to start Pulsar endpoint: %v", err)
		return
	}

	// Waiting for consumers to connect
	time.Sleep(time.Second * 3)

	// Test posts with attributes
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               pulsarURL,
		ConnectionTimeout: 30 * time.Second,
		OperationTimeout:  30 * time.Second,
	})
	if err != nil {
		t.Skipf("Pulsar server not available: %v", err)
		return
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "test-topic",
	})
	if err != nil {
		t.Skipf("Failed to create producer: %v", err)
		return
	}
	defer producer.Close()

	// Release messages with attributes
	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: []byte("test message with properties"),
		Properties: map[string]string{
			"customKey": "testValue",
			"deviceId":  "device123",
		},
		Key: "messageKey123",
	})
	if err != nil {
		t.Skipf("Failed to publish message: %v", err)
		return
	}
	// Waiting for the message to be processed
	time.Sleep(time.Second * 5)

	assert.Equal(t, int32(1), atomic.LoadInt32(&count))

	// Cleanup
	pulsarEndpoint.Destroy()
}
