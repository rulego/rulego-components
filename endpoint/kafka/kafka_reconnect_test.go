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
	"github.com/IBM/sarama"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/test/assert"
	"sync"
	"testing"
	"time"
)

// TestKafkaEndpointReconnect 测试Kafka endpoint重连功能
func TestKafkaEndpointReconnect(t *testing.T) {
	config := rulego.NewConfig(types.WithDefaultPool())

	t.Run("ConsumerReconnectConfig", func(t *testing.T) {
		// 测试消费者重连配置
		kafkaEndpoint, err := endpoint.Registry.New(Type, config, Config{
			Server:  "localhost:9092",
			GroupId: "test-reconnect",
		})
		assert.Nil(t, err)

		// 验证配置
		kafkaEp := kafkaEndpoint.(*Kafka)
		assert.Equal(t, "localhost:9092", kafkaEp.brokers[0])
		assert.Equal(t, "test-reconnect", kafkaEp.Config.GroupId)

		kafkaEndpoint.Destroy()
	})

	t.Run("ConsumerWithRetryMechanism", func(t *testing.T) {
		// 创建Kafka endpoint
		kafkaEndpoint, err := endpoint.Registry.New(Type, config, Config{
			Server:  "localhost:9092",
			GroupId: "test-retry",
		})
		if err != nil {
			t.Skipf("Failed to create Kafka endpoint: %v", err)
			return
		}

		kafkaEp := kafkaEndpoint.(*Kafka)
		messageReceived := false
		var mu sync.Mutex

		// 创建路由
		router := endpoint.NewRouter().From("test.retry.topic").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
			mu.Lock()
			messageReceived = true
			mu.Unlock()
			assert.Equal(t, "test retry message", exchange.In.GetMsg().GetData())
			return true
		}).End()

		// 添加路由
		_, err = kafkaEndpoint.AddRouter(router)
		if err != nil {
			t.Skipf("Failed to add router (Kafka server may not be available): %v", err)
			return
		}

		// 启动服务
		err = kafkaEndpoint.Start()
		if err != nil {
			t.Skipf("Failed to start Kafka endpoint: %v", err)
			return
		}

		// 等待消费者启动
		time.Sleep(time.Second)

		// 发送测试消息
		producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
		if err != nil {
			t.Skipf("Failed to create producer: %v", err)
			return
		}
		defer producer.Close()

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: "test.retry.topic",
			Value: sarama.StringEncoder("test retry message"),
		})
		if err != nil {
			t.Skipf("Failed to send message: %v", err)
			return
		}

		// 等待消息处理
		time.Sleep(time.Second * 2)

		// 验证消息是否被接收
		mu.Lock()
		received := messageReceived
		mu.Unlock()
		assert.True(t, received)

		// 测试路由移除
		err = kafkaEndpoint.RemoveRouter(router.GetId())
		assert.Nil(t, err)

		// 验证handlers中的消费者已被移除
		time.Sleep(time.Millisecond * 100)
		assert.Equal(t, 0, len(kafkaEp.handlers))

		kafkaEndpoint.Destroy()
	})

	t.Run("MultipleRoutersManagement", func(t *testing.T) {
		// 测试多个路由的管理
		kafkaEndpoint, err := endpoint.Registry.New(Type, config, Config{
			Server:  "localhost:9092",
			GroupId: "test-multiple",
		})
		if err != nil {
			t.Skipf("Failed to create Kafka endpoint: %v", err)
			return
		}

		kafkaEp := kafkaEndpoint.(*Kafka)

		// 创建多个路由
		router1 := endpoint.NewRouter().From("topic1").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
			return true
		}).End()

		router2 := endpoint.NewRouter().From("topic2").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
			return true
		}).End()

		// 添加路由
		id1, err := kafkaEndpoint.AddRouter(router1)
		if err != nil {
			t.Skipf("Failed to add router1: %v", err)
			return
		}

		id2, err := kafkaEndpoint.AddRouter(router2)
		if err != nil {
			t.Skipf("Failed to add router2: %v", err)
			return
		}

		// 验证路由已添加
		assert.Equal(t, 2, len(kafkaEp.handlers))
		assert.NotNil(t, kafkaEp.handlers[id1])
		assert.NotNil(t, kafkaEp.handlers[id2])

		// 移除一个路由
		err = kafkaEndpoint.RemoveRouter(id1)
		assert.Nil(t, err)

		// 等待清理完成
		time.Sleep(time.Millisecond * 100)
		assert.Equal(t, 1, len(kafkaEp.handlers))
		assert.Nil(t, kafkaEp.handlers[id1])
		assert.NotNil(t, kafkaEp.handlers[id2])

		kafkaEndpoint.Destroy()
	})
}

// TestKafkaEndpointErrorHandling 测试错误处理
func TestKafkaEndpointErrorHandling(t *testing.T) {
	config := rulego.NewConfig(types.WithDefaultPool())

	t.Run("InvalidBrokerHandling", func(t *testing.T) {
		// 测试无效broker的处理
		kafkaEndpoint, err := endpoint.Registry.New(Type, config, Config{
			Server:  "invalid-broker:9092",
			GroupId: "test-invalid",
		})
		assert.Nil(t, err)

		// 创建路由
		router := endpoint.NewRouter().From("test.invalid.topic").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
			return true
		}).End()

		// 尝试添加路由（应该失败）
		_, err = kafkaEndpoint.AddRouter(router)
		assert.NotNil(t, err)

		kafkaEndpoint.Destroy()
	})

	t.Run("DuplicateRouterHandling", func(t *testing.T) {
		// 测试重复路由的处理
		kafkaEndpoint, err := endpoint.Registry.New(Type, config, Config{
			Server:  "localhost:9092",
			GroupId: "test-duplicate",
		})
		if err != nil {
			t.Skipf("Failed to create Kafka endpoint: %v", err)
			return
		}

		// 创建相同的路由
		router1 := endpoint.NewRouter().From("duplicate.topic").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
			return true
		}).End()

		router2 := endpoint.NewRouter().From("duplicate.topic").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
			return true
		}).End()

		// 添加第一个路由
		_, err = kafkaEndpoint.AddRouter(router1)
		if err != nil {
			t.Skipf("Failed to add first router: %v", err)
			return
		}

		// 尝试添加重复路由（应该失败）
		_, err = kafkaEndpoint.AddRouter(router2)
		assert.NotNil(t, err)

		kafkaEndpoint.Destroy()
	})
}

// TestKafkaConsumerHandler 测试消费者处理器
func TestKafkaConsumerHandler(t *testing.T) {
	config := rulego.NewConfig(types.WithDefaultPool())

	t.Run("MessageProcessing", func(t *testing.T) {
		// 创建Kafka endpoint
		kafkaEndpoint, err := endpoint.Registry.New(Type, config, Config{
			Server:  "localhost:9092",
			GroupId: "test-handler",
		})
		if err != nil {
			t.Skipf("Failed to create Kafka endpoint: %v", err)
			return
		}

		_ = kafkaEndpoint.(*Kafka)
		processedMessages := make([]string, 0)
		var mu sync.Mutex

		// 创建路由
		router := endpoint.NewRouter().From("test.handler.topic").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
			mu.Lock()
			processedMessages = append(processedMessages, exchange.In.GetMsg().GetData())
			mu.Unlock()
			return true
		}).End()

		// 添加路由
		_, err = kafkaEndpoint.AddRouter(router)
		if err != nil {
			t.Skipf("Failed to add router: %v", err)
			return
		}

		// 启动服务
		err = kafkaEndpoint.Start()
		if err != nil {
			t.Skipf("Failed to start endpoint: %v", err)
			return
		}

		// 等待消费者启动
		time.Sleep(time.Second)

		// 发送多条消息
		producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
		if err != nil {
			t.Skipf("Failed to create producer: %v", err)
			return
		}
		defer producer.Close()

		messages := []string{"message1", "message2", "message3"}
		for _, msg := range messages {
			_, _, err = producer.SendMessage(&sarama.ProducerMessage{
				Topic: "test.handler.topic",
				Value: sarama.StringEncoder(msg),
			})
			if err != nil {
				t.Skipf("Failed to send message: %v", err)
				return
			}
		}

		// 等待消息处理
		time.Sleep(time.Second * 3)

		// 验证消息处理
		mu.Lock()
		processed := make([]string, len(processedMessages))
		copy(processed, processedMessages)
		mu.Unlock()

		assert.True(t, len(processed) >= len(messages))
		for _, expectedMsg := range messages {
			found := false
			for _, processedMsg := range processed {
				if processedMsg == expectedMsg {
					found = true
					break
				}
			}
			assert.True(t, found, "Message %s not found in processed messages", expectedMsg)
		}

		kafkaEndpoint.Destroy()
	})
}
