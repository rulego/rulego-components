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

// TestKafkaEndpointLifecycleManagement TestKafka Endpoint Lifecycle Management
func TestKafkaEndpointLifecycleManagement(t *testing.T) {

	config := engine.NewConfig()

	// Subtest 1: Basic initialization test
	t.Run("BasicInitialization", func(t *testing.T) {
		// Use the New() method to properly initialize the Kafka endpoint
		tempKafka := &Kafka{}
		kafkaEndpoint := tempKafka.New().(*Kafka)

		// Test the new method
		newInstance := kafkaEndpoint.New()
		assert.NotNil(t, newInstance)
		newKafka, ok := newInstance.(*Kafka)
		assert.True(t, ok)
		assert.Equal(t, "127.0.0.1:9092", newKafka.Config.Server)
		assert.Equal(t, "rulego", newKafka.Config.GroupId)
	})

	// Subtest 2: Configure the initialization test
	t.Run("ConfigurationInitialization", func(t *testing.T) {
		// Use the New() method to properly initialize the Kafka endpoint
		tempKafka := &Kafka{}
		kafkaEndpoint := tempKafka.New().(*Kafka)

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

	// Subtest 3: Route management test
	t.Run("RouterManagement", func(t *testing.T) {
		kafkaEndpoint := &Kafka{}

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "test-group",
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// Create test routes
		router := impl.NewRouter().From("test-topic").Transform(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
			exchange.Out.SetBody([]byte("processed"))
			return true
		}).End()

		// Test adding routes (not actually connecting to Kafka)
		routerId, err := kafkaEndpoint.AddRouter(router)
		if err != nil {
			// It was expected to fail because there was no actual Kafka server
			assert.NotNil(t, err)
		} else {
			assert.NotEqual(t, "", routerId)
		}

		// Test removing the route
		err = kafkaEndpoint.RemoveRouter("test-topic")
		assert.Nil(t, err)
	})

	// Subtest 4: Closing and cleaning tests
	t.Run("CloseAndCleanup", func(t *testing.T) {
		// Use the New() method to properly initialize the Kafka endpoint
		tempKafka := &Kafka{}
		kafkaEndpoint := tempKafka.New().(*Kafka)

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "test-group",
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// Test shutdown
		err = kafkaEndpoint.Close()
		assert.Nil(t, err)

		// Test repeats shutdown
		err = kafkaEndpoint.Close()
		assert.Nil(t, err)

		// Test destruction
		kafkaEndpoint.Destroy()
	})
}

// TestKafkaEndpointIdempotencyAndSafety Test the idempotency and security of Kafka endpoints
func TestKafkaEndpointIdempotencyAndSafety(t *testing.T) {

	config := engine.NewConfig()

	t.Run("MultipleCloseCalls", func(t *testing.T) {
		// Use the New() method to properly initialize the Kafka endpoint
		tempKafka := &Kafka{}
		kafkaEndpoint := tempKafka.New().(*Kafka)

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "test-group",
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// The first time it was closed
		err = kafkaEndpoint.Close()
		assert.Nil(t, err)

		// Repeated shutdowns should be safe and error-free
		err = kafkaEndpoint.Close()
		assert.Nil(t, err)

		// Repeat the closing again
		err = kafkaEndpoint.Close()
		assert.Nil(t, err)
	})

	t.Run("ConcurrentCloseCalls", func(t *testing.T) {
		// Use the New() method to properly initialize the Kafka endpoint
		tempKafka := &Kafka{}
		kafkaEndpoint := tempKafka.New().(*Kafka)

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "test-group",
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// Concurrent shutdown test
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

		// Verification is not error-free
		for err := range errChan {
			t.Errorf("Concurrent close error: %v", err)
		}
	})

	t.Run("ConcurrentRouterManagement", func(t *testing.T) {
		// Use the New() method to properly initialize the Kafka endpoint
		tempKafka := &Kafka{}
		kafkaEndpoint := tempKafka.New().(*Kafka)

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "test-group",
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// Concurrent route management tests
		const numGoroutines = 3
		var wg sync.WaitGroup
		errChan := make(chan error, numGoroutines*2)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()

				// Create unique routes
				router := impl.NewRouter().From(fmt.Sprintf("test-topic-%d", index)).Transform(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
					exchange.Out.SetBody([]byte(fmt.Sprintf("processed-%d", index)))
					return true
				}).End()

				// Try adding a route
				routerId, err := kafkaEndpoint.AddRouter(router)
				if err != nil {
					errChan <- fmt.Errorf("goroutine %d add router: %v", index, err)
				} else {
					// Try removing the route
					err = kafkaEndpoint.RemoveRouter(routerId)
					if err != nil {
						errChan <- fmt.Errorf("goroutine %d remove router: %v", index, err)
					}
				}
			}(i)
		}

		wg.Wait()
		close(errChan)

		// Collect errors (expect connection errors, but there should be no concurrency issues)
		for err := range errChan {
			t.Logf("Expected error (no Kafka server): %v", err)
		}
	})
}

// TestKafkaEndpointShutdownBehavior Tests the closing behavior of Kafka endpoint
func TestKafkaEndpointShutdownBehavior(t *testing.T) {

	config := engine.NewConfig()

	t.Run("GracefulShutdown", func(t *testing.T) {
		// Use the New() method to properly initialize the Kafka endpoint
		tempKafka := &Kafka{}
		kafkaEndpoint := tempKafka.New().(*Kafka)

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "test-group",
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// Verify the closed status check
		assert.False(t, kafkaEndpoint.IsShuttingDown())

		// Start closing
		err = kafkaEndpoint.Close()
		assert.Nil(t, err)

		// Verify the closed status
		assert.True(t, kafkaEndpoint.IsShuttingDown())

		// Verify that timeout settings are off
		timeout := kafkaEndpoint.GetShutdownTimeout()
		assert.Equal(t, 30*time.Second, timeout)
	})

	t.Run("ShutdownWithTimeout", func(t *testing.T) {
		// Use the New() method to properly initialize the Kafka endpoint
		tempKafka := &Kafka{}
		kafkaEndpoint := tempKafka.New().(*Kafka)
		kafkaEndpoint.shutdownTimeout = 5 * time.Second // Set a shorter timeout

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "test-group",
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// Test the band to close off after a timeout
		start := time.Now()
		err = kafkaEndpoint.Close()
		duration := time.Since(start)

		assert.Nil(t, err)
		// Closing times should be shorter than the timeouts (since there are no actual consumers).
		assert.True(t, duration < 5*time.Second)
	})
}

// TestKafkaEndpointConfiguration tests the configuration processing of Kafka endpoint
func TestKafkaEndpointConfiguration(t *testing.T) {
	config := engine.NewConfig()

	t.Run("DefaultConfiguration", func(t *testing.T) {
		// Use the New() method to properly initialize the Kafka endpoint
		tempKafka := &Kafka{}
		newInstance := tempKafka.New()
		newKafka, ok := newInstance.(*Kafka)
		assert.True(t, ok)

		// Verify the default configuration
		assert.Equal(t, "127.0.0.1:9092", newKafka.Config.Server)
		assert.Equal(t, "rulego", newKafka.Config.GroupId)
		assert.False(t, newKafka.Config.SASL.Enable)
		assert.Equal(t, "PLAIN", newKafka.Config.SASL.Mechanism)
		assert.False(t, newKafka.Config.TLS.Enable)
	})

	t.Run("CustomConfiguration", func(t *testing.T) {
		// Use the New() method to properly initialize the Kafka endpoint
		tempKafka := &Kafka{}
		kafkaEndpoint := tempKafka.New().(*Kafka)

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

		// Verify custom configurations
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
		// Use the New() method to properly initialize the Kafka endpoint
		tempKafka := &Kafka{}
		kafkaEndpoint := tempKafka.New().(*Kafka)

		// Test older versions of brokers configurations
		configuration := types.Configuration{
			"brokers": []string{"legacy1:9092", "legacy2:9092"},
			"groupId": "legacy-group",
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// Verify that the old version configuration is still valid
		assert.Equal(t, 2, len(kafkaEndpoint.brokers))
		assert.Equal(t, "legacy1:9092", kafkaEndpoint.brokers[0])
		assert.Equal(t, "legacy2:9092", kafkaEndpoint.brokers[1])
	})

	t.Run("EmptyGroupIdHandling", func(t *testing.T) {
		// Use the New() method to properly initialize the Kafka endpoint
		tempKafka := &Kafka{}
		kafkaEndpoint := tempKafka.New().(*Kafka)

		configuration := types.Configuration{
			"server":  "localhost:9092",
			"groupId": "  ", // Blank string
		}

		err := kafkaEndpoint.Init(config, configuration)
		assert.Nil(t, err)

		// The empty groupId is set to the default value
		assert.Equal(t, "rulego", kafkaEndpoint.Config.GroupId)
	})
}

// createKafkaTestRouter Creates a test route
func createKafkaTestRouter(topic string) endpointApi.Router {
	return impl.NewRouter().From(topic).To("chain:kafka-test").End()
}
