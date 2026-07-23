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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/test/assert"
)

var testdataFolder = "../../testdata"

func TestNsqEndpoint(t *testing.T) {
	// Check if NSQ servers are available
	nsqdAddress := os.Getenv("NSQD_ADDRESS")
	if nsqdAddress == "" {
		nsqdAddress = "127.0.0.1:4150"
	}

	lookupdAddress := os.Getenv("LOOKUPD_ADDRESS")
	if lookupdAddress == "" {
		lookupdAddress = "127.0.0.1:4161"
	}

	// If NSQ testing is set to skip, skip it
	if os.Getenv("SKIP_NSQ_TESTS") == "true" {
		t.Skip("Skipping NSQ tests")
	}

	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	// Register the rule chain
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	// Launch NSQ reception service
	nsqEndpoint, err := endpoint.Registry.New(Type, config, Config{
		Server: nsqdAddress,
	})
	if err != nil {
		t.Skipf("Failed to create NSQ endpoint (NSQ may not be available): %v", err)
		return
	}

	// Route 1
	router1 := endpoint.NewRouter().From("device_msg_request").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "test message", exchange.In.GetMsg().GetData())
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// Send data to a specified topic for response
		exchange.Out.Headers().Add(KeyResponseTopic, "device_msg_response")
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()

	count := int32(0)
	// Simulate to obtain responses
	router2 := endpoint.NewRouter().SetId("router2").From("device_msg_response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		fmt.Println("接收到数据：device_msg_response", exchange.In.GetMsg())
		assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// Simulate to get responses, same theme
	router3 := endpoint.NewRouter().SetId("router3").From("device_msg_response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		fmt.Println("接收到数据：device_msg_response", exchange.In.GetMsg())
		assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// Register the route
	_, err = nsqEndpoint.AddRouter(router1, "channel1")
	if err != nil {
		t.Skipf("Failed to add router1 (NSQ server may not be available): %v", err)
		return
	}
	_, err = nsqEndpoint.AddRouter(router2, "channel2")
	if err != nil {
		t.Skipf("Failed to add router2 (NSQ server may not be available): %v", err)
		return
	}
	router3Id, err := nsqEndpoint.AddRouter(router3, "channel2")
	assert.Nil(t, err)

	// Testing repeatedly adds the same routing ID, which should cause errors
	_, err = nsqEndpoint.AddRouter(router3, "channel2")
	assert.NotNil(t, err)
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("Expected error to contain 'already exists', got: %v", err)
	}

	// Start the server
	err = nsqEndpoint.Start()
	if err != nil {
		t.Skipf("Failed to start NSQ endpoint: %v", err)
		return
	}

	// Waiting for consumers to connect
	time.Sleep(time.Second * 2)

	// Test release announcement
	producerConfig := nsq.NewConfig()
	fmt.Println("producerConfig", producerConfig)
	producer, err := nsq.NewProducer(nsqdAddress, producerConfig)
	if err != nil {
		t.Skipf("NSQ server not available: %v", err)
		return
	}
	// Disable NSQ internal log output
	producer.SetLogger(log.New(io.Discard, "", 0), nsq.LogLevelError)
	defer producer.Stop()

	// Release the news to device_msg_request
	err = producer.Publish("device_msg_request", []byte("test message"))
	if err != nil {
		t.Skipf("Failed to publish message: %v", err)
		return
	}
	// Waiting for the message to be processed
	time.Sleep(time.Second * 3)

	// Since router2 and router3 listen for different channels, only router2 handles device_msg_response messages
	assert.Equal(t, int32(1), atomic.LoadInt32(&count))

	atomic.StoreInt32(&count, 0)
	// Delete router3
	_ = nsqEndpoint.RemoveRouter(router3Id)
	// Release the news to device_msg_request
	err = producer.Publish("device_msg_request", []byte("test message"))
	if err != nil {
		t.Skipf("Failed to publish second message: %v", err)
		return
	}
	// Waiting for the message to be processed
	time.Sleep(time.Second * 3)

	// After deleting router3, only router2 still handles messages
	assert.Equal(t, int32(1), atomic.LoadInt32(&count))

	// Cleanup
	nsqEndpoint.Destroy()
}

func TestNsqEndpointWithNsqd(t *testing.T) {
	// Check if NSQ servers are available
	nsqdAddress := os.Getenv("NSQD_ADDRESS")
	if nsqdAddress == "" {
		nsqdAddress = "127.0.0.1:4150"
	}

	// If NSQ testing is set to skip, skip it
	if os.Getenv("SKIP_NSQ_TESTS") == "true" {
		t.Skip("Skipping NSQ tests")
	}

	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	// Register the rule chain
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	// Start NSQ reception service (NSQD only)
	nsqEndpoint, err := endpoint.Registry.New(Type, config, Config{
		Server: nsqdAddress,
	})
	if err != nil {
		t.Skipf("Failed to create NSQ endpoint (NSQ may not be available): %v", err)
		return
	}

	count := int32(0)
	// Route
	router := endpoint.NewRouter().From("test_topic").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "test message", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// Register the route
	_, err = nsqEndpoint.AddRouter(router)
	if err != nil {
		t.Skipf("Failed to add router (NSQ server may not be available): %v", err)
		return
	}

	// Start the server
	err = nsqEndpoint.Start()
	if err != nil {
		t.Skipf("Failed to start NSQ endpoint: %v", err)
		return
	}

	// Waiting for consumers to connect
	time.Sleep(time.Second * 2)

	// Test release announcement
	producerConfig := nsq.NewConfig()
	producer, err := nsq.NewProducer(nsqdAddress, producerConfig)
	if err != nil {
		t.Skipf("NSQ server not available: %v", err)
		return
	}
	// Disable NSQ internal log output
	producer.SetLogger(log.New(io.Discard, "", 0), nsq.LogLevelError)
	defer producer.Stop()

	// Release the news
	err = producer.Publish("test_topic", []byte("test message"))
	if err != nil {
		t.Skipf("Failed to publish message: %v", err)
		return
	}
	// Waiting for the message to be processed
	time.Sleep(time.Second * 3)

	assert.Equal(t, int32(1), atomic.LoadInt32(&count))

	// Cleanup
	nsqEndpoint.Destroy()
}

func TestDiscoverNsqdProducersFromLookupds_Fallback(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer bad.Close()

	goodBody, _ := json.Marshal(map[string]any{
		"producers": []map[string]any{
			{"broadcast_address": "a.example", "tcp_port": 4150},
		},
	})
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(goodBody)
	}))
	defer good.Close()

	addrs, err := discoverNsqdProducersFromLookupds([]string{bad.URL, good.URL})
	if err != nil {
		t.Fatal(err)
	}
	if len(addrs) != 1 || addrs[0] != "a.example:4150" {
		t.Fatalf("got %v", addrs)
	}
}

func TestBuildReachableProducers_None(t *testing.T) {
	cfg := nsq.NewConfig()
	_, err := buildReachableProducers([]string{"127.0.0.1:1", "127.0.0.1:2"}, cfg)
	if err == nil {
		t.Fatal("expected error for unreachable nsqd")
	}
	if !strings.Contains(err.Error(), "no reachable nsqd") {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestRoundRobinProducers_EmptyPool(t *testing.T) {
	empty := &roundRobinProducers{prods: nil}
	if err := empty.Publish("t", []byte("x")); err == nil {
		t.Fatal("expected error for empty pool")
	}
}
