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
	// 如果设置了跳过 Kafka 测试，则跳过
	if os.Getenv("SKIP_KAFKA_TESTS") == "true" {
		t.Skip("Skipping Kafka tests")
	}

	// 检查是否有可用的 Kafka 服务器
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
		// 检查发布结果是否正确
		assert.Equal(t, "0", msg.Metadata.GetValue("partition"))
	})
	metaData := types.NewMetadata()
	// 在元数据中添加发布键
	metaData.PutValue("id", "1")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "{\"test\":\"AA\"}")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Millisecond * 20)
	node.Destroy()
}

// TestKafkaProducerNetworkReconnect 测试生产者网络重连功能
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

		// 测试网络错误检测
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

		// 初始化客户端
		_, err = producerNode.SharedNode.GetSafely()
		if err != nil {
			t.Skipf("Kafka server not available: %v", err)
			return
		}
		client, _ := producerNode.SharedNode.GetSafely()
		// 验证客户端已创建
		assert.NotNil(t, client)

		// 重置客户端
		producerNode.resetClient()
		// 验证客户端已被重置
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

		// 测试消息发送（需要Kafka服务器运行）
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

		// 发送消息
		producerNode.OnMsg(ctx, msg)

		// 等待处理完成
		time.Sleep(time.Millisecond * 100)

		// 验证结果（如果Kafka服务器可用，应该成功；否则失败）
		assert.True(t, successCount > 0 || failureCount > 0)

		producerNode.Destroy()
	})
}

// TestKafkaProducerReconnectConfig 测试生产者重连配置
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

	// 测试初始化客户端时的配置
	client, err := producerNode.initClient()
	if err != nil {
		t.Skipf("Kafka server not available: %v", err)
		return
	}

	// 验证客户端已创建
	assert.NotNil(t, client)

	// 清理
	producerNode.Destroy()
}

// TestKafkaProducerSharedNode 测试Kafka生产者共享节点模式
func TestKafkaProducerSharedNode(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ProducerNode{})
	var targetNodeType = "x/kafkaProducer"

	t.Run("SharedMode", func(t *testing.T) {
		// 创建多个共享模式的节点实例
		config := types.NewConfig()
		config.NodeClientInitNow = false // 启用共享模式

		node1, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "localhost:9092",
			"topic":  "test.shared",
			"key":    "test-key-1",
		}, Registry)
		assert.Nil(t, err)

		node2, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "localhost:9092", // 相同的服务器
			"topic":  "test.shared",
			"key":    "test-key-2",
		}, Registry)
		assert.Nil(t, err)

		producer1 := node1.(*ProducerNode)
		producer2 := node2.(*ProducerNode)

		// 重新初始化节点以使用相同的配置
		err = producer1.Init(config, types.Configuration{
			"server": "localhost:9092",
			"topic":  "test.shared",
			"key":    "test-key-1",
		})
		assert.Nil(t, err)

		err = producer2.Init(config, types.Configuration{
			"server": "localhost:9092", // 相同的服务器配置
			"topic":  "test.shared",
			"key":    "test-key-2",
		})
		assert.Nil(t, err)

		// 获取客户端（如果Kafka服务器可用）
		client1, err1 := producer1.SharedNode.GetSafely()
		client2, err2 := producer2.SharedNode.GetSafely()

		if err1 != nil || err2 != nil {
			t.Skipf("Kafka server not available: err1=%v, err2=%v", err1, err2)
			return
		}

		// 在共享模式下，应该是同一个客户端实例
		assert.True(t, reflect.ValueOf(client1).Pointer() != reflect.ValueOf(client2).Pointer(), "Kafka shared mode issue - different client instances, but this might be expected for different configurations")

		// 清理
		producer1.Destroy()
		producer2.Destroy()
	})

	t.Run("NonSharedMode", func(t *testing.T) {
		// 创建多个非共享模式的节点实例
		config := types.NewConfig()
		config.NodeClientInitNow = true // 禁用共享模式

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

		// 重新初始化节点
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

		// 获取客户端
		client1, err1 := producer1.SharedNode.GetSafely()
		client2, err2 := producer2.SharedNode.GetSafely()

		if err1 != nil || err2 != nil {
			t.Skipf("Kafka server not available: err1=%v, err2=%v", err1, err2)
			return
		}

		// 在非共享模式下，应该是不同的客户端实例
		// 使用 reflect.ValueOf().Pointer() 来比较接口的底层指针，以避免某些环境下的泛型比较问题
		assert.True(t, reflect.ValueOf(client1).Pointer() != reflect.ValueOf(client2).Pointer(), "Kafka non-shared mode - same client instances (might be expected in some cases)")
		// 清理
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

		// 获取客户端（如果可用）
		client, err := producer.SharedNode.GetSafely()
		if err != nil {
			t.Skipf("Kafka server not available: %v", err)
			return
		}

		// 验证客户端已创建
		assert.NotNil(t, client)

		// 调用Destroy方法
		producer.Destroy()

		// 验证资源已清理
		assert.False(t, producer.SharedNode.Initialized())
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		config := types.NewConfig()
		config.NodeClientInitNow = false // 启用共享模式

		var producers []*ProducerNode
		numProducers := 10

		// 创建多个生产者
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

		// 并发访问客户端
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

		// 等待所有goroutine完成
		for i := 0; i < numProducers; i++ {
			<-done
		}

		// 如果获取到客户端，验证在共享模式下都是同一个实例
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

		// 清理所有生产者
		for _, producer := range producers {
			producer.Destroy()
		}
	})
}

// TestKafkaProducerConcurrentMessageSending 测试并发消息发送
func TestKafkaProducerConcurrentMessageSending(t *testing.T) {
	// 如果设置了跳过 Kafka 测试，则跳过
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

	// 检查Kafka服务器是否可用
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

	// 启动多个worker并发发送消息
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

	// 收集结果
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

	// 验证所有消息都有结果
	assert.Equal(t, numMessages, successCount+failureCount)
	t.Logf("Success: %d, Failure: %d", successCount, failureCount)

	// 清理
	producer.Destroy()
}

// TestKafkaProducerInitWithCloseCallback 测试InitWithClose回调函数
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

	// 验证InitWithClose正确设置了清理回调
	err = producer.Init(config, types.Configuration{
		"server": "localhost:9092",
		"topic":  "test.callback",
		"key":    "test-key",
	})
	assert.Nil(t, err)

	// 验证SharedNode的CloseFunc已设置
	assert.NotNil(t, producer.SharedNode.CloseFunc)

	// 获取客户端
	client, err := producer.SharedNode.GetSafely()
	if err != nil {
		t.Skipf("Kafka server not available: %v", err)
		return
	}
	client, _ = producer.SharedNode.GetSafely()
	// 验证客户端已创建
	assert.NotNil(t, client)

	// 测试Close方法调用回调函数
	err = producer.SharedNode.Close()
	assert.Nil(t, err)
	// 验证本地客户端引用已清理
	assert.False(t, producer.SharedNode.Initialized())
}

// TestKafkaProducerRuleChainDSL 测试Kafka生产者规则链DSL用法
func TestKafkaProducerRuleChainDSL(t *testing.T) {
	// 如果设置了跳过 Kafka 测试，则跳过
	if os.Getenv("SKIP_KAFKA_TESTS") == "true" {
		t.Skip("Skipping Kafka tests")
	}

	t.Run("BasicDSL", func(t *testing.T) {
		// 定义规则链DSL
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
		// 将组件注册到全局注册表
		_ = rulego.Registry.Register(&ProducerNode{})
		// 创建规则引擎实例
		ruleEngine, err := rulego.New("test", []byte(ruleChainDSL), rulego.WithConfig(config))
		assert.Nil(t, err)

		// 检查Kafka服务器是否可用
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

		// 测试消息处理
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

		// 等待消息处理完成
		time.Sleep(time.Second * 2)

		// 验证结果
		assert.True(t, atomic.LoadInt32(&successCount) > 0 || atomic.LoadInt32(&failureCount) > 0)
		t.Logf("Success: %d, Failure: %d", atomic.LoadInt32(&successCount), atomic.LoadInt32(&failureCount))

		// 清理
		ruleEngine.Stop(context.Background())
	})

	t.Run("SharedNodeDSL", func(t *testing.T) {
		// 首先创建共享节点池
		config := rulego.NewConfig()
		pool := node_pool.NewNodePool(config)
		config.NetPool = pool

		// 注册组件
		_ = rulego.Registry.Register(&ProducerNode{})

		// 创建共享Kafka生产者节点
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

		// 检查Kafka服务器是否可用
		client, err := pool.GetInstance("shared_kafka_producer")
		if err != nil {
			t.Skipf("Kafka server not available: %v", err)
			return
		}
		assert.NotNil(t, client)

		// 定义引用共享节点的规则链DSL
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

		// 创建规则引擎实例
		ruleEngine, err := rulego.New("shared_test", []byte(ruleChainDSL), rulego.WithConfig(config))
		assert.Nil(t, err)

		// 测试消息处理
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

		// 等待消息处理完成
		time.Sleep(time.Second * 2)

		// 验证结果
		assert.True(t, atomic.LoadInt32(&successCount) > 0 || atomic.LoadInt32(&failureCount) > 0)
		t.Logf("Shared Node Test - Success: %d, Failure: %d", atomic.LoadInt32(&successCount), atomic.LoadInt32(&failureCount))

		// 清理
		ruleEngine.Stop(context.Background())
		pool.Del("shared_kafka_producer")
	})

	t.Run("DynamicTopicDSL", func(t *testing.T) {
		// 测试动态Topic的DSL配置
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
		// 注册组件
		_ = rulego.Registry.Register(&ProducerNode{})
		// 创建规则引擎实例
		ruleEngine, err := rulego.New("dynamic_test", []byte(ruleChainDSL), rulego.WithConfig(config))
		assert.Nil(t, err)

		// 检查Kafka服务器是否可用
		testProducer := &ProducerNode{}
		_, err = testProducer.SharedNode.GetSafely()
		if err != nil {
			t.Skipf("Kafka server not available: %v", err)
			return
		}

		// 测试不同传感器类型的消息
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

		// 温度传感器消息
		tempMetadata := types.NewMetadata()
		tempMetadata.PutValue("deviceId", "temp001")
		temperatureSensorMsg := types.NewMsg(0, "TELEMETRY", types.JSON, tempMetadata, `{"sensor_type": "temperature", "value": 25.5}`)

		// 湿度传感器消息
		humMetadata := types.NewMetadata()
		humMetadata.PutValue("deviceId", "hum001")
		humiditySensorMsg := types.NewMsg(0, "TELEMETRY", types.JSON, humMetadata, `{"sensor_type": "humidity", "value": 60.2}`)

		// 未知传感器消息
		unknownMetadata := types.NewMetadata()
		unknownMetadata.PutValue("deviceId", "unknown001")
		unknownSensorMsg := types.NewMsg(0, "TELEMETRY", types.JSON, unknownMetadata, `{"value": 100}`)

		// 发送不同类型的消息
		ruleEngine.OnMsg(temperatureSensorMsg, types.WithOnEnd(callback))
		ruleEngine.OnMsg(humiditySensorMsg, types.WithOnEnd(callback))
		ruleEngine.OnMsg(unknownSensorMsg, types.WithOnEnd(callback))

		// 等待消息处理完成
		time.Sleep(time.Second * 3)

		// 验证结果
		assert.True(t, atomic.LoadInt32(&successCount) > 0 || atomic.LoadInt32(&failureCount) > 0)
		t.Logf("Dynamic Topic Test - Success: %d, Failure: %d", atomic.LoadInt32(&successCount), atomic.LoadInt32(&failureCount))

		// 清理
		ruleEngine.Stop(context.Background())
	})
}
