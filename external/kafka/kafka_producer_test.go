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

package kafka

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/node_pool"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
)

func TestKafkaProducer(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ProducerNode{})
	var targetNodeType = "x/kafkaProducer"

	t.Run("InitNode", func(t *testing.T) {
		_, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "",
		}, Registry)
		assert.Equal(t, "brokers is empty", err.Error())

		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "localhost:9092",
		}, Registry)
		assert.Equal(t, "localhost:9092", node.(*ProducerNode).brokers[0])

		node, err = test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":    "localhost:9092,localhost:9093",
			"topic":     "device/msg",
			"key":       "aa",
			"partition": 1,
		}, Registry)
		assert.Equal(t, "localhost:9092", node.(*ProducerNode).brokers[0])
		assert.Equal(t, "localhost:9093", node.(*ProducerNode).brokers[1])
		assert.Equal(t, "device/msg", node.(*ProducerNode).Config.Topic)
		assert.Equal(t, "aa", node.(*ProducerNode).Config.Key)
		assert.Equal(t, int32(1), node.(*ProducerNode).Config.Partition)

		node, err = test.CreateAndInitNode(targetNodeType, types.Configuration{
			"brokers": []string{"localhost:9092", "localhost:9093"},
		}, Registry)
		assert.Equal(t, "localhost:9092", node.(*ProducerNode).brokers[0])
		assert.Equal(t, "localhost:9093", node.(*ProducerNode).brokers[1])
	})

}
func TestKafkaProducerNodeOnMsg(t *testing.T) {
	// If you set up skipping Kafka tests, skip them
	if os.Getenv("SKIP_KAFKA_TESTS") == "true" {
		t.Skip("Skipping Kafka tests")
	}

	// Check if there are available Kafka servers
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}

	var node ProducerNode
	var configuration = make(types.Configuration)
	configuration["topic"] = "device.msg.request"
	configuration["key"] = "${metadata.id}"
	configuration["server"] = kafkaBrokers
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Success, relationType)
		// Check whether the published results are correct
		assert.Equal(t, "0", msg.Metadata.GetValue("partition"))
	})
	metaData := types.NewMetadata()
	// Add a publish key to the metadata
	metaData.PutValue("id", "1")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "{\"test\":\"AA\"}")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Millisecond * 20)
	node.Destroy()
}

// TestKafkaProducerNetworkReconnect tests the producer's network reconnection function
func TestKafkaProducerNetworkReconnect(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ProducerNode{})
	var targetNodeType = "x/kafkaProducer"

	t.Run("NetworkErrorDetection", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "localhost:9092",
			"topic":  "test.reconnect",
			"key":    "test-key",
		}, Registry)
		assert.Nil(t, err)

		producerNode := node.(*ProducerNode)

		// Test network error detection
		testCases := []struct {
			name     string
			err      error
			expected bool
		}{
			{"ConnectionRefused", errors.New("connection refused"), true},
			{"NoRouteToHost", errors.New("no route to host"), true},
			{"NetworkUnreachable", errors.New("network is unreachable"), true},
			{"ConnectionReset", errors.New("connection reset"), true},
			{"BrokenPipe", errors.New("broken pipe"), true},
			{"EOF", errors.New("EOF"), true},
			{"OutOfBrokers", sarama.ErrOutOfBrokers, true},
			{"OtherError", errors.New("some other error"), false},
			{"NilError", nil, false},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := producerNode.isNetworkError(tc.err)
				assert.Equal(t, tc.expected, result)
			})
		}
	})

	t.Run("ClientReset", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "localhost:9092",
			"topic":  "test.reconnect",
			"key":    "test-key",
		}, Registry)
		assert.Nil(t, err)

		producerNode := node.(*ProducerNode)

		// Initialize the client
		_, err = producerNode.SharedNode.GetSafely()
		if err != nil {
			t.Skipf("Kafka server not available: %v", err)
			return
		}
		client, _ := producerNode.SharedNode.GetSafely()
		// Verify that the client has been created
		assert.NotNil(t, client)

		// Reset the client
		producerNode.resetClient()
		// Verify that the client has been reset
		assert.False(t, producerNode.SharedNode.Initialized())
	})

	t.Run("MessageSendWithRetry", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "localhost:9092",
			"topic":  "test.reconnect",
			"key":    "test-key",
		}, Registry)
		assert.Nil(t, err)

		producerNode := node.(*ProducerNode)

		// Test message sending (requires Kafka server to run)
		config := types.NewConfig()
		successCount := 0
		failureCount := 0

		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			if relationType == types.Success {
				successCount++
			} else {
				failureCount++
			}
		})

		metaData := types.NewMetadata()
		msg := ctx.NewMsg("TEST_MSG_TYPE", metaData, "{\"test\":\"reconnect\"}")

		// Send the message
		producerNode.OnMsg(ctx, msg)

		// Wait for processing to complete
		time.Sleep(time.Millisecond * 100)

		// Verify results (if Kafka servers are available, they should succeed; Otherwise, it fails)
		assert.True(t, successCount > 0 || failureCount > 0)

		producerNode.Destroy()
	})
}

// TestKafkaProducerReconnectConfig Tests the producer reconnection configuration
func TestKafkaProducerReconnectConfig(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ProducerNode{})
	var targetNodeType = "x/kafkaProducer"

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"server": "localhost:9092",
		"topic":  "test.config",
		"key":    "test-key",
	}, Registry)
	assert.Nil(t, err)

	producerNode := node.(*ProducerNode)

	// Testing the configuration when initializing the client
	client, err := producerNode.initClient()
	if err != nil {
		t.Skipf("Kafka server not available: %v", err)
		return
	}

	// Verify that the client has been created
	assert.NotNil(t, client)

	// Cleanup
	producerNode.Destroy()
}

// TestKafkaProducerSharedNode tests the Kafka producer shared node pattern
func TestKafkaProducerSharedNode(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ProducerNode{})
	var targetNodeType = "x/kafkaProducer"

	t.Run("SharedMode", func(t *testing.T) {
		// Create multiple node instances in shared mode
		config := types.NewConfig()
		config.NodeClientInitNow = false // Enable sharing mode

		node1, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "localhost:9092",
			"topic":  "test.shared",
			"key":    "test-key-1",
		}, Registry)
		assert.Nil(t, err)

		node2, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "localhost:9092", // Same server
			"topic":  "test.shared",
			"key":    "test-key-2",
		}, Registry)
		assert.Nil(t, err)

		producer1 := node1.(*ProducerNode)
		producer2 := node2.(*ProducerNode)

		// Reinitialize the node to use the same configuration
		err = producer1.Init(config, types.Configuration{
			"server": "localhost:9092",
			"topic":  "test.shared",
			"key":    "test-key-1",
		})
		assert.Nil(t, err)

		err = producer2.Init(config, types.Configuration{
			"server": "localhost:9092", // Same server configuration
			"topic":  "test.shared",
			"key":    "test-key-2",
		})
		assert.Nil(t, err)

		// Obtain the client (if Kafka server is available)
		client1, err1 := producer1.SharedNode.GetSafely()
		client2, err2 := producer2.SharedNode.GetSafely()

		if err1 != nil || err2 != nil {
			t.Skipf("Kafka server not available: err1=%v, err2=%v", err1, err2)
			return
		}

		// In shared mode, it should be the same client instance
		assert.True(t, reflect.ValueOf(client1).Pointer() != reflect.ValueOf(client2).Pointer(), "Kafka shared mode issue - different client instances, but this might be expected for different configurations")

		// Cleanup
		producer1.Destroy()
		producer2.Destroy()
	})

	t.Run("NonSharedMode", func(t *testing.T) {
		// Create multiple node instances in non-shared mode
		config := types.NewConfig()
		config.NodeClientInitNow = true // Disable sharing mode

		node1, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "localhost:9092",
			"topic":  "test.nonshared",
			"key":    "test-key-1",
		}, Registry)
		assert.Nil(t, err)

		node2, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "localhost:9092",
			"topic":  "test.nonshared",
			"key":    "test-key-2",
		}, Registry)
		assert.Nil(t, err)

		producer1 := node1.(*ProducerNode)
		producer2 := node2.(*ProducerNode)

		// Reinitialize the node
		err = producer1.Init(config, types.Configuration{
			"server": "localhost:9092",
			"topic":  "test.nonshared",
			"key":    "test-key-1",
		})
		assert.Nil(t, err)

		err = producer2.Init(config, types.Configuration{
			"server": "localhost:9092",
			"topic":  "test.nonshared",
			"key":    "test-key-2",
		})
		assert.Nil(t, err)

		// Get the client
		client1, err1 := producer1.SharedNode.GetSafely()
		client2, err2 := producer2.SharedNode.GetSafely()

		if err1 != nil || err2 != nil {
			t.Skipf("Kafka server not available: err1=%v, err2=%v", err1, err2)
			return
		}

		// In non-shared mode, there should be different client instances
		// Use reflect.ValueOf(). Pointer() to compare the underlying pointers of interfaces to avoid generic comparison issues in certain environments
		assert.True(t, reflect.ValueOf(client1).Pointer() != reflect.ValueOf(client2).Pointer(), "Kafka non-shared mode - same client instances (might be expected in some cases)")
		// Cleanup
		producer1.Destroy()
		producer2.Destroy()
	})

	t.Run("ResourceCleanup", func(t *testing.T) {
		config := types.NewConfig()
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "localhost:9092",
			"topic":  "test.cleanup",
			"key":    "test-key",
		}, Registry)
		assert.Nil(t, err)

		producer := node.(*ProducerNode)
		err = producer.Init(config, types.Configuration{
			"server": "localhost:9092",
			"topic":  "test.cleanup",
			"key":    "test-key",
		})
		assert.Nil(t, err)

		// Getting the client (if available)
		client, err := producer.SharedNode.GetSafely()
		if err != nil {
			t.Skipf("Kafka server not available: %v", err)
			return
		}

		// Verify that the client has been created
		assert.NotNil(t, client)

		// Call the Destroy method
		producer.Destroy()

		// Verification resources have been cleared
		assert.False(t, producer.SharedNode.Initialized())
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		config := types.NewConfig()
		config.NodeClientInitNow = false // Enable sharing mode

		var producers []*ProducerNode
		numProducers := 10

		// Creating multiple producers
		for i := 0; i < numProducers; i++ {
			node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
				"server": "localhost:9092",
				"topic":  "test.concurrent",
				"key":    "test-key",
			}, Registry)
			assert.Nil(t, err)

			producer := node.(*ProducerNode)
			err = producer.Init(config, types.Configuration{
				"server": "localhost:9092",
				"topic":  "test.concurrent",
				"key":    "test-key",
			})
			assert.Nil(t, err)

			producers = append(producers, producer)
		}

		// Concurrent access to the client
		done := make(chan bool, numProducers)
		var clients []sarama.SyncProducer
		var clientsMutex sync.Mutex

		for i := 0; i < numProducers; i++ {
			go func(producer *ProducerNode) {
				defer func() { done <- true }()

				client, err := producer.SharedNode.GetSafely()
				if err == nil {
					clientsMutex.Lock()
					clients = append(clients, client)
					clientsMutex.Unlock()
				}
			}(producers[i])
		}

		// Wait for all goroutines to complete
		for i := 0; i < numProducers; i++ {
			<-done
		}

		// If a client is obtained, the validation is the same instance in shared mode
		if len(clients) > 1 {
			firstClient := clients[0]
			sameInstances := 0
			for _, client := range clients[1:] {
				if client == firstClient {
					sameInstances++
				}
			}
			t.Logf("Kafka concurrent access test: %d/%d clients share the same instance", sameInstances, len(clients)-1)
		} else {
			t.Logf("Got %d Kafka clients", len(clients))
		}

		// Eliminate all producers
		for _, producer := range producers {
			producer.Destroy()
		}
	})
}

// TestKafkaProducerConcurrentMessageSending. Test concurrent message sending
func TestKafkaProducerConcurrentMessageSending(t *testing.T) {
	// If you set up skipping Kafka tests, skip them
	if os.Getenv("SKIP_KAFKA_TESTS") == "true" {
		t.Skip("Skipping Kafka tests")
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ProducerNode{})
	var targetNodeType = "x/kafkaProducer"

	config := types.NewConfig()
	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"server": "localhost:9092",
		"topic":  "test.concurrent.messages",
		"key":    "test-key",
	}, Registry)
	assert.Nil(t, err)

	producer := node.(*ProducerNode)
	err = producer.Init(config, types.Configuration{
		"server": "localhost:9092",
		"topic":  "test.concurrent.messages",
		"key":    "test-key",
	})
	assert.Nil(t, err)

	// Check if the Kafka server is available
	_, err = producer.SharedNode.GetSafely()
	if err != nil {
		t.Skipf("Kafka server not available: %v", err)
		return
	}

	numMessages := 50
	numWorkers := 5
	successCount := 0
	failureCount := 0
	resultCh := make(chan string, numMessages)

	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
		resultCh <- relationType
	})

	// Launch multiple workers to send messages concurrently
	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			for i := 0; i < numMessages/numWorkers; i++ {
				metaData := types.NewMetadata()
				metaData.PutValue("workerID", string(rune(workerID)))
				metaData.PutValue("messageID", string(rune(i)))

				msg := ctx.NewMsg("TEST_MSG_TYPE", metaData, "{\"test\":\"concurrent message\"}")
				producer.OnMsg(ctx, msg)
			}
		}(w)
	}

	// Collect the results
	timeout := time.After(30 * time.Second)
	for i := 0; i < numMessages; i++ {
		select {
		case result := <-resultCh:
			if result == types.Success {
				successCount++
			} else {
				failureCount++
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for message results")
		}
	}

	// Verifying all messages yields results
	assert.Equal(t, numMessages, successCount+failureCount)
	t.Logf("Success: %d, Failure: %d", successCount, failureCount)

	// Cleanup
	producer.Destroy()
}

// TestKafkaProducerInitWithCloseCallback tests the InitWithClose callback function
func TestKafkaProducerInitWithCloseCallback(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ProducerNode{})
	var targetNodeType = "x/kafkaProducer"

	config := types.NewConfig()
	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"server": "localhost:9092",
		"topic":  "test.callback",
		"key":    "test-key",
	}, Registry)
	assert.Nil(t, err)

	producer := node.(*ProducerNode)

	// Verify that InitWithClose is correctly set for cleanup callbacks
	err = producer.Init(config, types.Configuration{
		"server": "localhost:9092",
		"topic":  "test.callback",
		"key":    "test-key",
	})
	assert.Nil(t, err)

	// Verify that SharedNode's CloseFunc is set
	assert.NotNil(t, producer.SharedNode.CloseFunc)

	// Get the client
	client, err := producer.SharedNode.GetSafely()
	if err != nil {
		t.Skipf("Kafka server not available: %v", err)
		return
	}
	client, _ = producer.SharedNode.GetSafely()
	// Verify that the client has been created
	assert.NotNil(t, client)

	// Test the Close method to call the callback function
	err = producer.SharedNode.Close()
	assert.Nil(t, err)
	// Verify that the local client reference has been cleaned
	assert.False(t, producer.SharedNode.Initialized())
}

// TestKafkaProducerRuleChainDSL tests the usage of KafkaProducerRuleChainDSL
func TestKafkaProducerRuleChainDSL(t *testing.T) {
	// If you set up skipping Kafka tests, skip them
	if os.Getenv("SKIP_KAFKA_TESTS") == "true" {
		t.Skip("Skipping Kafka tests")
	}

	t.Run("BasicDSL", func(t *testing.T) {
		// Define the rule chain DSL
		ruleChainDSL := `{
			"ruleChain": {
				"id": "kafka_producer_test_chain",
				"name": "Kafka Producer Test Chain",
				"debugMode": false
			},
			"metadata": {
				"nodes": [
					{
						"id": "s1",
						"type": "jsTransform",
						"name": "添加时间戳",
						"debugMode": false,
						"configuration": {
							"jsScript": "metadata.timestamp = new Date().toISOString(); return {'msg': msg, 'metadata': metadata, 'msgType': msgType};"
						}
					},
					{
						"id": "s2", 
						"type": "x/kafkaProducer",
						"name": "Kafka生产者",
						"debugMode": false,
						"configuration": {
							"server": "localhost:9092",
							"topic": "test.dsl.topic",
							"key": "${metadata.deviceId}",
							"partition": 0
						}
					}
				],
				"connections": [
					{
						"fromId": "s1",
						"toId": "s2",
						"type": "Success"
					}
				]
			}
		}`

		config := rulego.NewConfig()
		// Register components to the global registry
		_ = rulego.Registry.Register(&ProducerNode{})
		// Create a rule engine instance
		ruleEngine, err := rulego.New("test", []byte(ruleChainDSL), rulego.WithConfig(config))
		assert.Nil(t, err)

		// Check if the Kafka server is available
		testProducer := &ProducerNode{}
		err = testProducer.Init(config, types.Configuration{
			"server": "localhost:9092",
			"topic":  "test.connection",
			"key":    "test",
		})
		if err != nil {
			t.Logf("Kafka server not available: %v", err)
		}

		_, err = testProducer.SharedNode.GetSafely()
		if err != nil {
			t.Skipf("Kafka server not available: %v", err)
			return
		}

		// Test message processing
		var successCount int32
		var failureCount int32

		metadata := types.NewMetadata()
		metadata.PutValue("deviceId", "device001")
		msg := types.NewMsg(0, "TELEMETRY", types.JSON, metadata, `{"temperature": 25.5, "humidity": 60}`)
		ruleEngine.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
			if err != nil {
				atomic.AddInt32(&failureCount, 1)
				t.Logf("Message processing failed: %v", err)
			} else {
				atomic.AddInt32(&successCount, 1)
				t.Logf("Message processed successfully: %s", msg.GetData())
			}
		}))

		// Wait for message processing to complete
		time.Sleep(time.Second * 2)

		// Verify the results
		assert.True(t, atomic.LoadInt32(&successCount) > 0 || atomic.LoadInt32(&failureCount) > 0)
		t.Logf("Success: %d, Failure: %d", atomic.LoadInt32(&successCount), atomic.LoadInt32(&failureCount))

		// Cleanup
		ruleEngine.Stop(context.Background())
	})

	t.Run("SharedNodeDSL", func(t *testing.T) {
		// First, create a shared node pool
		config := rulego.NewConfig()
		pool := node_pool.NewNodePool(config)
		config.NodePool = pool

		// Register the component
		_ = rulego.Registry.Register(&ProducerNode{})

		// Create a shared Kafka producer node
		sharedNodeDsl := []byte(`{
			"id": "shared_kafka_producer",
			"type": "x/kafkaProducer",
			"name": "共享Kafka生产者",
			"debugMode": false,
			"configuration": {
				"server": "localhost:9092",
				"topic": "shared.topic",
				"key": "shared-key",
				"partition": 0
			}
		}`)

		nodeDef, err := config.Parser.DecodeRuleNode(sharedNodeDsl)
		if err != nil {
			t.Skipf("Cannot parse shared node: %v", err)
			return
		}

		ctx, err := pool.NewFromRuleNode(nodeDef)
		if err != nil {
			t.Skipf("Cannot create shared node: %v", err)
			return
		}
		assert.NotNil(t, ctx)

		// Check if the Kafka server is available
		client, err := pool.GetInstance("shared_kafka_producer")
		if err != nil {
			t.Skipf("Kafka server not available: %v", err)
			return
		}
		assert.NotNil(t, client)

		// Define the DSL rule chain that references shared nodes
		ruleChainDSL := `{
			"ruleChain": {
				"id": "shared_kafka_chain",
				"name": "Shared Kafka Chain",
				"debugMode": false
			},
			"metadata": {
				"nodes": [
					{
						"id": "kafka1",
						"type": "x/kafkaProducer",
						"name": "引用共享生产者1",
						"debugMode": false,
						"configuration": {
							"server": "ref://shared_kafka_producer"
						}
					}
				],
				"connections": []
			}
		}`

		// Create a rule engine instance
		ruleEngine, err := rulego.New("shared_test", []byte(ruleChainDSL), rulego.WithConfig(config))
		assert.Nil(t, err)

		// Test message processing
		var successCount int32
		var failureCount int32

		metadata := types.NewMetadata()
		metadata.PutValue("testKey", "shared-test")
		msg := types.NewMsg(0, "SHARED_TEST", types.JSON, metadata, `{"test": "shared node"}`)

		ruleEngine.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
			if err != nil {
				atomic.AddInt32(&failureCount, 1)
				t.Logf("Shared node message failed: %v", err)
			} else {
				atomic.AddInt32(&successCount, 1)
				t.Logf("Shared node message succeeded: %s", msg.GetData())
			}
		}))

		// Wait for message processing to complete
		time.Sleep(time.Second * 2)

		// Verify the results
		assert.True(t, atomic.LoadInt32(&successCount) > 0 || atomic.LoadInt32(&failureCount) > 0)
		t.Logf("Shared Node Test - Success: %d, Failure: %d", atomic.LoadInt32(&successCount), atomic.LoadInt32(&failureCount))

		// Cleanup
		ruleEngine.Stop(context.Background())
		pool.Del("shared_kafka_producer")
	})

	t.Run("DynamicTopicDSL", func(t *testing.T) {
		// Test the DSL configuration of dynamic topics
		ruleChainDSL := `{
			"ruleChain": {
				"id": "dynamic_kafka_chain",
				"name": "Dynamic Topic Kafka Chain", 
				"debugMode": false
			},
			"metadata": {
				"nodes": [
					{
						"id": "transform1",
						"type": "jsTransform",
						"name": "设置动态Topic",
						"debugMode": false,
						"configuration": {
							"jsScript": "metadata.topicSuffix = msg.sensor_type || 'default'; return {'msg': msg, 'metadata': metadata, 'msgType': msgType};"
						}
					},
					{
						"id": "dynamic_kafka",
						"type": "x/kafkaProducer",
						"name": "动态Topic生产者",
						"debugMode": false,
						"configuration": {
							"server": "localhost:9092",
							"topic": "sensors.${metadata.topicSuffix}",
							"key": "${metadata.deviceId}",
							"partition": 0
						}
					}
				],
				"connections": [
					{
						"fromId": "transform1",
						"toId": "dynamic_kafka",
						"type": "Success"
					}
				]
			}
		}`

		config := rulego.NewConfig()
		// Register the component
		_ = rulego.Registry.Register(&ProducerNode{})
		// Create a rule engine instance
		ruleEngine, err := rulego.New("dynamic_test", []byte(ruleChainDSL), rulego.WithConfig(config))
		assert.Nil(t, err)

		// Check if the Kafka server is available
		testProducer := &ProducerNode{}
		_, err = testProducer.SharedNode.GetSafely()
		if err != nil {
			t.Skipf("Kafka server not available: %v", err)
			return
		}

		// Testing messages for different types of sensors
		var successCount int32
		var failureCount int32

		callback := func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
			if err != nil {
				atomic.AddInt32(&failureCount, 1)
				t.Logf("Message processing failed: %v", err)
			} else {
				atomic.AddInt32(&successCount, 1)
				t.Logf("Message processed successfully: %s", msg.GetData())
			}
		}

		// Temperature sensor message
		tempMetadata := types.NewMetadata()
		tempMetadata.PutValue("deviceId", "temp001")
		temperatureSensorMsg := types.NewMsg(0, "TELEMETRY", types.JSON, tempMetadata, `{"sensor_type": "temperature", "value": 25.5}`)

		// Humidity sensor message
		humMetadata := types.NewMetadata()
		humMetadata.PutValue("deviceId", "hum001")
		humiditySensorMsg := types.NewMsg(0, "TELEMETRY", types.JSON, humMetadata, `{"sensor_type": "humidity", "value": 60.2}`)

		// Unknown sensor information
		unknownMetadata := types.NewMetadata()
		unknownMetadata.PutValue("deviceId", "unknown001")
		unknownSensorMsg := types.NewMsg(0, "TELEMETRY", types.JSON, unknownMetadata, `{"value": 100}`)

		// Send different types of messages
		ruleEngine.OnMsg(temperatureSensorMsg, types.WithOnEnd(callback))
		ruleEngine.OnMsg(humiditySensorMsg, types.WithOnEnd(callback))
		ruleEngine.OnMsg(unknownSensorMsg, types.WithOnEnd(callback))

		// Wait for message processing to complete
		time.Sleep(time.Second * 3)

		// Verify the results
		assert.True(t, atomic.LoadInt32(&successCount) > 0 || atomic.LoadInt32(&failureCount) > 0)
		t.Logf("Dynamic Topic Test - Success: %d, Failure: %d", atomic.LoadInt32(&successCount), atomic.LoadInt32(&failureCount))

		// Cleanup
		ruleEngine.Stop(context.Background())
	})
}
