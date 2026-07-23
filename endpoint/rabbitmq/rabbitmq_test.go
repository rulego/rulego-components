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

package rabbitmq

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/test/assert"

	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
)

var testdataFolder = "../../testdata"

const (
	exchange      = "rulego.topic.test"
	topicRequest  = "device.msg.request"
	topicResponse = "device.msg.response"
)

func TestEndpoint(t *testing.T) {
	// Obtain the RabbitMQ server address from the environment variable
	server := os.Getenv("RABBITMQ_URL")
	if server == "" {
		server = "amqp://guest:guest@localhost:5672/"
	}

	// If you set to skip the RabbitMQ test, skip it
	if os.Getenv("SKIP_RABBITMQ_TESTS") == "true" {
		t.Skip("Skipping RabbitMQ tests")
	}

	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	// Register the rule chain
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	// Start the Enpoint receiving service
	ep, err := endpoint.Registry.New(Type, config, Config{
		Server:   server,
		Exchange: exchange,
	})
	if err != nil {
		t.Skipf("Failed to create RabbitMQ endpoint (RabbitMQ may not be available): %v", err)
		return
	}

	// Route 1
	router1 := endpoint.NewRouter().From(topicRequest).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "test message", exchange.In.GetMsg().GetData())
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// Send data to a specified topic for response
		exchange.Out.Headers().Add(KeyResponseTopic, topicResponse)
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()

	count := int32(0)
	// Simulate to obtain responses
	router2 := endpoint.NewRouter().SetId("router3").From(topicResponse).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		//fmt.Println("Data received: device.msg.response", exchange.In.GetMsg())
		assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// Simulate to get responses, same theme
	router3 := endpoint.NewRouter().SetId("router3").From(topicResponse).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// Register the route
	_, err = ep.AddRouter(router1)
	if err != nil {
		t.Skipf("Failed to add router1 (RabbitMQ server may not be available): %v", err)
		return
	}
	_, err = ep.AddRouter(router2)
	if err != nil {
		t.Skipf("Failed to add router2 (RabbitMQ server may not be available): %v", err)
		return
	}
	router3Id, err := ep.AddRouter(router3)
	assert.NotNil(t, err)
	// Start the server
	err = ep.Start()
	if err != nil {
		t.Skipf("Failed to start RabbitMQ endpoint: %v", err)
		return
	}

	// Test publishing and subscriptions
	conn, err := amqp.Dial(server)
	if err != nil {
		t.Skipf("RabbitMQ server not available: %v", err)
		return
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		t.Skipf("Failed to create channel: %v", err)
		return
	}
	defer channel.Close()

	// Send messages to device.msg.request
	err = channel.Publish(
		exchange,     // Released to the switch
		topicRequest, // Router key
		false,        // Indicates whether messages must be routed to at least one queue
		false,        // Whether to request messages to be received by consumers immediately
		amqp.Publishing{
			ContentType:     ContentTypeJson,
			ContentEncoding: KeyUTF8,
			Body:            []byte("test message"),
		})
	if err != nil {
		t.Skipf("Failed to publish message: %v", err)
		return
	}
	// Waiting for the message to be processed
	time.Sleep(time.Second * 1)

	assert.Equal(t, int32(1), atomic.LoadInt32(&count))

	atomic.StoreInt32(&count, 0)
	//Delete the same topic
	_ = ep.RemoveRouter(router3Id)
	// Send messages to device.msg.request
	err = channel.Publish(
		exchange,     // Released to the switch
		topicRequest, // Router key
		false,        // Indicates whether messages must be routed to at least one queue
		false,        // Whether to request messages to be received by consumers immediately
		amqp.Publishing{
			ContentType:     ContentTypeJson,
			ContentEncoding: KeyUTF8,
			Body:            []byte("test message"),
		})
	if err != nil {
		t.Skipf("Failed to publish second message: %v", err)
		return
	}
	// Waiting for the message to be processed
	time.Sleep(time.Second * 1)

	assert.Equal(t, int32(0), atomic.LoadInt32(&count))
}
