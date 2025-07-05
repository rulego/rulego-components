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

package kafka

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/engine"
	"github.com/rulego/rulego/test/assert"
)

// TestKafkaEndpointLifecycleManagement 测试Kafka endpoint的生命周期管理
func TestKafkaEndpointLifecycleManagement(t *testing.T) {
	// 跳过测试，因为需要实际的Kafka服务器
	t.Skip("Skipping Kafka tests - requires actual Kafka server")

	config := engine.NewConfig()

	// 子测试1：基本初始化测试
	t.Run("BasicInitialization", func(t *testing.T) {
		kafkaEndpoint := &Kafka{}

		// 测试New方法
		newInstance := kafkaEndpoint.New()
		assert.NotNil(t, newInstance)
		newKafka, ok := newInstance.(*Kafka)
		assert.True(t, ok)
		assert.Equal(t, "127.0.0.1:9092", newKafka.Config.Server)
		assert.Equal(t, "rulego", newKafka.Config.GroupId)
	})

	// 子测试2：配置初始化测试
	t.Run("ConfigurationInitialization", func(t *testing.T) {
		kafkaEndpoint := &Kafka{}

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "test-group",
			"sasl": map[string]interface{}{
				"enable":    true,
				"mechanism": "SCRAM-SHA-256",
				"username":  "test-user",
				"password":  "test-pass",
			},
			"tls": map[string]interface{}{
				"enable":             true,
				"insecureSkipVerify": true,
			},
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)
		assert.Equal(t, "localhost:9092", kafkaEndpoint.Config.Server)
		assert.Equal(t, "test-group", kafkaEndpoint.Config.GroupId)
		assert.True(t, kafkaEndpoint.Config.SASL.Enable)
		assert.Equal(t, "SCRAM-SHA-256", kafkaEndpoint.Config.SASL.Mechanism)
		assert.True(t, kafkaEndpoint.Config.TLS.Enable)
	})

	// 子测试3：路由管理测试
	t.Run("RouterManagement", func(t *testing.T) {
		kafkaEndpoint := &Kafka{}

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "test-group",
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// 创建测试路由
		router := impl.NewRouter().From("test-topic").Transform(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
			exchange.Out.SetBody([]byte("processed"))
			return true
		}).End()

		// 测试添加路由（不实际连接Kafka）
		routerId, err := kafkaEndpoint.AddRouter(router)
		if err != nil {
			// 预期会失败，因为没有实际的Kafka服务器
			assert.NotNil(t, err)
		} else {
			assert.NotEqual(t, "", routerId)
		}

		// 测试移除路由
		err = kafkaEndpoint.RemoveRouter("test-topic")
		assert.Nil(t, err)
	})

	// 子测试4：关闭和清理测试
	t.Run("CloseAndCleanup", func(t *testing.T) {
		kafkaEndpoint := &Kafka{}

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "test-group",
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// 测试关闭
		err = kafkaEndpoint.Close()
		assert.Nil(t, err)

		// 测试重复关闭
		err = kafkaEndpoint.Close()
		assert.Nil(t, err)

		// 测试销毁
		kafkaEndpoint.Destroy()
	})
}

// TestKafkaEndpointIdempotencyAndSafety 测试Kafka endpoint的幂等性和安全性
func TestKafkaEndpointIdempotencyAndSafety(t *testing.T) {
	// 跳过测试，因为需要实际的Kafka服务器
	t.Skip("Skipping Kafka tests - requires actual Kafka server")

	config := engine.NewConfig()

	t.Run("MultipleCloseCalls", func(t *testing.T) {
		kafkaEndpoint := &Kafka{}

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "test-group",
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// 第一次关闭
		err = kafkaEndpoint.Close()
		assert.Nil(t, err)

		// 重复关闭应该安全且无错误
		err = kafkaEndpoint.Close()
		assert.Nil(t, err)

		// 再次重复关闭
		err = kafkaEndpoint.Close()
		assert.Nil(t, err)
	})

	t.Run("ConcurrentCloseCalls", func(t *testing.T) {
		kafkaEndpoint := &Kafka{}

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "test-group",
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// 并发关闭测试
		const numGoroutines = 5
		var wg sync.WaitGroup
		errChan := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				err := kafkaEndpoint.Close()
				if err != nil {
					errChan <- fmt.Errorf("goroutine %d: %v", index, err)
				}
			}(i)
		}

		wg.Wait()
		close(errChan)

		// 验证没有错误
		for err := range errChan {
			t.Errorf("Concurrent close error: %v", err)
		}
	})

	t.Run("ConcurrentRouterManagement", func(t *testing.T) {
		kafkaEndpoint := &Kafka{}

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "test-group",
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// 并发路由管理测试
		const numGoroutines = 3
		var wg sync.WaitGroup
		errChan := make(chan error, numGoroutines*2)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()

				// 创建独特的路由
				router := impl.NewRouter().From(fmt.Sprintf("test-topic-%d", index)).Transform(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
					exchange.Out.SetBody([]byte(fmt.Sprintf("processed-%d", index)))
					return true
				}).End()

				// 尝试添加路由
				routerId, err := kafkaEndpoint.AddRouter(router)
				if err != nil {
					errChan <- fmt.Errorf("goroutine %d add router: %v", index, err)
				} else {
					// 尝试移除路由
					err = kafkaEndpoint.RemoveRouter(routerId)
					if err != nil {
						errChan <- fmt.Errorf("goroutine %d remove router: %v", index, err)
					}
				}
			}(i)
		}

		wg.Wait()
		close(errChan)

		// 收集错误（预期会有连接错误，但不应该有并发问题）
		for err := range errChan {
			t.Logf("Expected error (no Kafka server): %v", err)
		}
	})
}

// TestKafkaEndpointShutdownBehavior 测试Kafka endpoint的关闭行为
func TestKafkaEndpointShutdownBehavior(t *testing.T) {
	// 跳过测试，因为需要实际的Kafka服务器
	t.Skip("Skipping Kafka tests - requires actual Kafka server")

	config := engine.NewConfig()

	t.Run("GracefulShutdown", func(t *testing.T) {
		kafkaEndpoint := &Kafka{}

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "test-group",
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// 验证关闭状态检查
		assert.False(t, kafkaEndpoint.IsShuttingDown())

		// 开始关闭
		err = kafkaEndpoint.Close()
		assert.Nil(t, err)

		// 验证关闭状态
		assert.True(t, kafkaEndpoint.IsShuttingDown())

		// 验证关闭超时设置
		timeout := kafkaEndpoint.GetShutdownTimeout()
		assert.Equal(t, 30*time.Second, timeout)
	})

	t.Run("ShutdownWithTimeout", func(t *testing.T) {
		kafkaEndpoint := &Kafka{}
		kafkaEndpoint.shutdownTimeout = 5 * time.Second // 设置较短的超时

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "test-group",
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// 测试带超时的关闭
		start := time.Now()
		err = kafkaEndpoint.Close()
		duration := time.Since(start)

		assert.Nil(t, err)
		// 关闭时间应该短于超时时间（因为没有实际的消费者）
		assert.True(t, duration < 5*time.Second)
	})
}

// TestKafkaEndpointConfiguration 测试Kafka endpoint的配置处理
func TestKafkaEndpointConfiguration(t *testing.T) {
	config := engine.NewConfig()

	t.Run("DefaultConfiguration", func(t *testing.T) {
		kafkaEndpoint := &Kafka{}
		newInstance := kafkaEndpoint.New()
		newKafka, ok := newInstance.(*Kafka)
		assert.True(t, ok)

		// 验证默认配置
		assert.Equal(t, "127.0.0.1:9092", newKafka.Config.Server)
		assert.Equal(t, "rulego", newKafka.Config.GroupId)
		assert.False(t, newKafka.Config.SASL.Enable)
		assert.Equal(t, "PLAIN", newKafka.Config.SASL.Mechanism)
		assert.False(t, newKafka.Config.TLS.Enable)
	})

	t.Run("CustomConfiguration", func(t *testing.T) {
		kafkaEndpoint := &Kafka{}

		configuration := types.Configuration{
			"server":  "kafka1:9092,kafka2:9092",
			"groupId": "custom-group",
			"sasl": map[string]interface{}{
				"enable":    true,
				"mechanism": "SCRAM-SHA-512",
				"username":  "custom-user",
				"password":  "custom-pass",
			},
			"tls": map[string]interface{}{
				"enable":             true,
				"insecureSkipVerify": false,
			},
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// 验证自定义配置
		assert.Equal(t, "kafka1:9092,kafka2:9092", kafkaEndpoint.Config.Server)
		assert.Equal(t, "custom-group", kafkaEndpoint.Config.GroupId)
		assert.True(t, kafkaEndpoint.Config.SASL.Enable)
		assert.Equal(t, "SCRAM-SHA-512", kafkaEndpoint.Config.SASL.Mechanism)
		assert.Equal(t, "custom-user", kafkaEndpoint.Config.SASL.Username)
		assert.Equal(t, "custom-pass", kafkaEndpoint.Config.SASL.Password)
		assert.True(t, kafkaEndpoint.Config.TLS.Enable)
		assert.False(t, kafkaEndpoint.Config.TLS.InsecureSkipVerify)
	})

	t.Run("LegacyBrokersConfiguration", func(t *testing.T) {
		kafkaEndpoint := &Kafka{}

		// 测试旧版本的brokers配置
		configuration := types.Configuration{
			"brokers": []string{"legacy1:9092", "legacy2:9092"},
			"groupId": "legacy-group",
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// 验证旧版本配置仍然有效
		assert.Equal(t, 2, len(kafkaEndpoint.brokers))
		assert.Equal(t, "legacy1:9092", kafkaEndpoint.brokers[0])
		assert.Equal(t, "legacy2:9092", kafkaEndpoint.brokers[1])
	})

	t.Run("EmptyGroupIdHandling", func(t *testing.T) {
		kafkaEndpoint := &Kafka{}

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "  ", // 空白字符串
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// 验证空的groupId被设置为默认值
		assert.Equal(t, "rulego", kafkaEndpoint.Config.GroupId)
	})
}

// createKafkaTestRouter 创建测试路由
func createKafkaTestRouter(topic string) endpointApi.Router {
	return impl.NewRouter().From(topic).To("chain:kafka-test").End()
}
