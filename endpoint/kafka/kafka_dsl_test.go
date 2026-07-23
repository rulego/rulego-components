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
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/action"
	"github.com/rulego/rulego/engine"
	"github.com/rulego/rulego/test/assert"
)

// TestKafkaDSLEndpoint tests start Kafka endpoints using DSL, perform dynamic routing, and hot updates
// This test demonstrates how to achieve the following functionality using the ruleEngine.ReloadSelf() method:
// 1. Initialize Kafka endpoint and routing configuration
// 2. Dynamically add new routes (alarm routes and log routes) via ReloadSelf()
// 3. Delete specified routes via ReloadSelf() (remove alert routes, keep other routes)
// 4. Complete hot update via ReloadSelf() (replacing all routing and processing nodes)
//
// Advantages of the ReloadSelf() method:
// - Supports complete DSL configuration updates, including endpoints, routers, nodes, etc
// - Automatically handle resource creation, updates, and cleanup
// - Ensure the atomicity of configuration updates to avoid intermediate states
// - Supports complex routing topology changes
func TestKafkaDSLEndpoint(t *testing.T) {
	// A counter used to verify message reception
	var sensorMsgCount, deviceMsgCount int32

	// Register sensor data verification functions
	action.Functions.Register("validateSensorData", func(ctx types.RuleContext, msg types.RuleMsg) {
		// Increase the counter
		atomic.AddInt32(&sensorMsgCount, 1)

		// Verify message data
		data := msg.GetData()
		if len(data) == 0 {
			ctx.TellFailure(msg, fmt.Errorf("sensor data is empty"))
			return
		}

		// Parse JSON data
		var sensorData map[string]interface{}
		if err := json.Unmarshal([]byte(data), &sensorData); err != nil {
			ctx.TellFailure(msg, fmt.Errorf("failed to parse sensor data: %v", err))
			return
		}

		// Verify the required fields
		if _, ok := sensorData["sensorId"]; !ok {
			ctx.TellFailure(msg, fmt.Errorf("missing sensorId field"))
			return
		}

		// Add validation tags to metadata
		msg.Metadata.PutValue("validated", "true")
		msg.Metadata.PutValue("validatedBy", "validateSensorData")
		msg.Metadata.PutValue("processedAt", time.Now().Format(time.RFC3339))

		// Keep working on it
		ctx.TellNext(msg, "validated")
	})

	// Register the device status verification function
	action.Functions.Register("validateDeviceStatus", func(ctx types.RuleContext, msg types.RuleMsg) {
		// Increase the counter
		atomic.AddInt32(&deviceMsgCount, 1)

		// Verify message data
		data := msg.GetData()
		if len(data) == 0 {
			ctx.TellFailure(msg, fmt.Errorf("device status data is empty"))
			return
		}

		// Parse JSON data
		var deviceData map[string]interface{}
		if err := json.Unmarshal([]byte(data), &deviceData); err != nil {
			ctx.TellFailure(msg, fmt.Errorf("failed to parse device data: %v", err))
			return
		}

		// Verify the required fields
		if _, ok := deviceData["deviceId"]; !ok {
			ctx.TellFailure(msg, fmt.Errorf("missing deviceId field"))
			return
		}

		if _, ok := deviceData["status"]; !ok {
			ctx.TellFailure(msg, fmt.Errorf("missing status field"))
			return
		}

		// Add validation tags to metadata
		msg.Metadata.PutValue("validated", "true")
		msg.Metadata.PutValue("validatedBy", "validateDeviceStatus")
		msg.Metadata.PutValue("processedAt", time.Now().Format(time.RFC3339))

		// Keep working on it
		ctx.TellNext(msg, "validated")
	})

	// Create an initial DSL configuration
	initialDSL := `{
		"ruleChain": {
			"id": "kafka_dsl_test",
			"name": "Kafka DSL Test Chain",
			"root": true,
			"debugMode": true
		},
		"metadata": {
			"endpoints": [
				{
					"id": "kafka_endpoint_1",
					"type": "kafka",
					"name": "Kafka Consumer",
					"configuration": {
						"server": "localhost:9092",
						"groupId": "test-group",
						"autoOffsetReset": "earliest",
						"autoCommit": true
					},
					"routers": [
						{
							"id": "sensor_data_router",
							"from": {
								"path": "sensor.data"
							},
							"to": {
								"path": "kafka_dsl_test:sensor_processor"
							}
						},
						{
							"id": "device_status_router",
							"from": {
								"path": "device.status"
							},
							"to": {
								"path": "kafka_dsl_test:device_processor"
							}
						}
					]
				}
			],
			"nodes": [
				{
					"id": "sensor_processor",
					"type": "jsTransform",
					"name": "传感器数据处理器",
					"configuration": {
						"jsScript": "var result = {\n  type: 'sensor_processed',\n  originalData: msg,\n  processedAt: new Date().toISOString(),\n  topic: metadata.topic,\n  partition: metadata.partition,\n  offset: metadata.offset\n};\nmetadata.topic = 'processed.sensor.data';\nreturn {'msg': result, 'metadata': metadata, 'msgType': 'SENSOR_PROCESSED'};"
					},
					"debugMode": true
				},
				{
					"id": "device_processor",
					"type": "jsTransform",
					"name": "设备状态处理器",
					"configuration": {
						"jsScript": "var result = {\n  type: 'device_status_processed',\n  deviceId: msg.deviceId,\n  status: msg.status,\n  processedAt: new Date().toISOString(),\n  topic: metadata.topic,\n  partition: metadata.partition\n};\nmetadata.topic = 'processed.device.status';\nreturn {'msg': result, 'metadata': metadata, 'msgType': 'DEVICE_STATUS_PROCESSED'};"
					},
					"debugMode": true
				},
				{
					"id": "sensor_validator",
					"type": "functions",
					"name": "传感器数据验证器",
					"configuration": {
						"functionName": "validateSensorData"
					},
					"debugMode": true
				},
				{
					"id": "device_validator",
					"type": "functions",
					"name": "设备状态验证器",
					"configuration": {
						"functionName": "validateDeviceStatus"
					},
					"debugMode": true
				}
			],
			"connections": [
				{
					"fromId": "sensor_processor",
					"toId": "sensor_validator",
					"type": "Success"
				},
				{
					"fromId": "device_processor",
					"toId": "device_validator",
					"type": "Success"
				}
			]
		}
	}`

	// Create rule engine configurations
	config := rulego.NewConfig(
		types.WithDefaultPool(),
		types.WithOnDebug(func(chainId, flowType string, nodeId string, msg types.RuleMsg, relationType string, err error) {
			//t.Logf("[Kafka Debugging] Chain: %s, Node: %s, Relation: %s, Message: %s", chainId, nodeId, relationType, msg.GetData())
			// Add parameters to assert verification for debugging callbacks
			assert.True(t, len(chainId) > 0, "chainId should not be empty")
			assert.True(t, len(nodeId) > 0, "nodeId should not be empty")
		}),
	)

	// Use DSL to create a rule chain containing embedded endpoints
	ruleEngine, err := rulego.New("kafka_dsl_test", []byte(initialDSL), engine.WithConfig(config))
	assert.Nil(t, err)
	assert.NotNil(t, ruleEngine)

	// Waiting for Kafka consumers to get started – CI environments take longer
	time.Sleep(time.Second * 10)

	// Create Kafka producers for testing and verifying connections
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Retry.Max = 5
	saramaConfig.Producer.Retry.Backoff = time.Second
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, saramaConfig)
	assert.Nil(t, err)
	defer producer.Close()

	// The extra wait ensures consumers are fully prepared to receive messages
	time.Sleep(time.Second * 3)

	// Test the initial route
	t.Run("TestInitialRoutes", func(t *testing.T) {
		// Reset the counter
		atomic.StoreInt32(&sensorMsgCount, 0)
		atomic.StoreInt32(&deviceMsgCount, 0)

		// Note: This test is processed by the Kafka message triggering the rule chain, so there is no need to call OnMsg directly

		// Send sensor data messages
		sensorData := map[string]interface{}{
			"sensorId":    "temp001",
			"temperature": 25.5,
			"humidity":    60.2,
			"timestamp":   time.Now().Unix(),
		}
		sensorJSON, _ := json.Marshal(sensorData)
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: "sensor.data",
			Value: sarama.StringEncoder(sensorJSON),
		})
		assert.Nil(t, err)

		// Send device status messages
		deviceStatus := map[string]interface{}{
			"deviceId": "device001",
			"status":   "online",
			"lastSeen": time.Now().Unix(),
		}
		deviceJSON, _ := json.Marshal(deviceStatus)
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: "device.status",
			Value: sarama.StringEncoder(deviceJSON),
		})
		assert.Nil(t, err)

		// Wait for message processing to complete
		time.Sleep(time.Second * 3)

		// Verify whether the functions node has correctly processed the message
		assert.True(t, atomic.LoadInt32(&sensorMsgCount) > 0, "传感器验证函数应该被调用")
		assert.True(t, atomic.LoadInt32(&deviceMsgCount) > 0, "设备状态验证函数应该被调用")

		t.Logf("Sensor message processing frequency: %d", atomic.LoadInt32(&sensorMsgCount))
		t.Logf("Device status message processing frequency: %d", atomic.LoadInt32(&deviceMsgCount))

	})

	// Dynamically add new routes - update DSL configurations via ReloadSelf
	t.Run("AddDynamicRoutes", func(t *testing.T) {
		//var alertCount, logCount int32

		// Create a DSL configuration containing the new route
		expandedDSL := `{
			"ruleChain": {
				"id": "kafka_dsl_test",
				"name": "Kafka DSL Test Chain - Expanded",
				"root": true,
				"debugMode": true
			},
			"metadata": {
				"endpoints": [
					{
						"id": "kafka_endpoint_1",
						"type": "kafka",
						"name": "Kafka Consumer - Expanded",
						"configuration": {
							"server": "localhost:9092",
							"groupId": "test-group",
							"autoOffsetReset": "earliest",
							"autoCommit": true
						},
						"routers": [
							{
								"id": "sensor_data_router",
								"from": {
									"path": "sensor.data"
								},
								"to": {
									"path": "kafka_dsl_test:sensor_processor"
								}
							},
							{
								"id": "device_status_router",
								"from": {
									"path": "device.status"
								},
								"to": {
									"path": "kafka_dsl_test:device_processor"
								}
							},
							{
								"id": "alert_router",
								"from": {
									"path": "system.alert"
								},
								"to": {
									"path": "kafka_dsl_test:alert_processor"
								}
							},
							{
								"id": "log_router",
								"from": {
									"path": "application.log"
								},
								"to": {
									"path": "kafka_dsl_test:log_processor"
								}
							}
						]
					}
				],
				"nodes": [
					{
						"id": "sensor_processor",
						"type": "jsTransform",
						"name": "传感器数据处理器",
						"configuration": {
							"jsScript": "var data = JSON.parse(msg);\nvar result = {\n  type: 'sensor_processed',\n  originalData: data,\n  processedAt: new Date().toISOString(),\n  topic: metadata.topic,\n  partition: metadata.partition,\n  offset: metadata.offset\n};\nmetadata.topic = 'processed.sensor.data';\nreturn {'msg': JSON.stringify(result), 'metadata': metadata, 'msgType': 'SENSOR_PROCESSED'};"
						},
						"debugMode": true
					},
					{
						"id": "device_processor",
						"type": "jsTransform",
						"name": "设备状态处理器",
						"configuration": {
							"jsScript": "var status = JSON.parse(msg);\nvar result = {\n  type: 'device_status_processed',\n  deviceId: status.deviceId,\n  status: status.status,\n  processedAt: new Date().toISOString(),\n  topic: metadata.topic,\n  partition: metadata.partition\n};\nmetadata.topic = 'processed.device.status';\nreturn {'msg': JSON.stringify(result), 'metadata': metadata, 'msgType': 'DEVICE_STATUS_PROCESSED'};"
						},
						"debugMode": true
					},
					{
						"id": "alert_processor",
						"type": "jsTransform",
						"name": "告警处理器",
						"configuration": {
							"jsScript": "var alert = JSON.parse(msg);\nvar result = {\n  type: 'alert_processed',\n  level: alert.level,\n  message: alert.message,\n  source: alert.source,\n  processedAt: new Date().toISOString(),\n  topic: metadata.topic\n};\nmetadata.topic = 'processed.system.alert';\nreturn {'msg': JSON.stringify(result), 'metadata': metadata, 'msgType': 'ALERT_PROCESSED'};"
						},
						"debugMode": true
					},
					{
						"id": "log_processor",
						"type": "jsTransform",
						"name": "日志处理器",
						"configuration": {
							"jsScript": "var log = JSON.parse(msg);\nvar result = {\n  type: 'log_processed',\n  level: log.level,\n  message: log.message,\n  userId: log.userId,\n  processedAt: new Date().toISOString(),\n  topic: metadata.topic\n};\nmetadata.topic = 'processed.application.log';\nreturn {'msg': JSON.stringify(result), 'metadata': metadata, 'msgType': 'LOG_PROCESSED'};"
						},
						"debugMode": true
					}
				],
				"connections": []
			}
		}`

		// Add a new route via the ReloadSelf method
		err := ruleEngine.ReloadSelf([]byte(expandedDSL))
		assert.Nil(t, err)

		time.Sleep(time.Second * 2)

		// Test the newly added alarm route
		alertMessage := map[string]interface{}{
			"level":     "critical",
			"message":   "System overload detected",
			"source":    "monitoring-service",
			"timestamp": time.Now().Unix(),
		}
		alertJSON, _ := json.Marshal(alertMessage)
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: "system.alert",
			Value: sarama.StringEncoder(alertJSON),
		})
		assert.Nil(t, err)

		// Test the newly added log route
		logMessage := map[string]interface{}{
			"level":     "info",
			"message":   "User login successful",
			"userId":    "user123",
			"timestamp": time.Now().Unix(),
		}
		logJSON, _ := json.Marshal(logMessage)
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: "application.log",
			Value: sarama.StringEncoder(logJSON),
		})
		assert.Nil(t, err)

		time.Sleep(time.Second * 3)

	})

	// Test route deletion - Remove the route via ReloadSelf
	t.Run("RemoveRoute", func(t *testing.T) {
		// Create a DSL configuration after deleting the alert route (retaining only the original route and log route).
		reducedDSL := `{
			"ruleChain": {
				"id": "kafka_dsl_test",
				"name": "Kafka DSL Test Chain - Reduced",
				"root": true,
				"debugMode": true
			},
			"metadata": {
				"endpoints": [
					{
						"id": "kafka_endpoint_1",
						"type": "kafka",
						"name": "Kafka Consumer - Reduced",
						"configuration": {
							"server": "localhost:9092",
							"groupId": "test-group",
							"autoOffsetReset": "earliest",
							"autoCommit": true
						},
						"routers": [
							{
								"id": "sensor_data_router",
								"from": {
									"path": "sensor.data"
								},
								"to": {
									"path": "kafka_dsl_test:sensor_processor"
								}
							},
							{
								"id": "device_status_router",
								"from": {
									"path": "device.status"
								},
								"to": {
									"path": "kafka_dsl_test:device_processor"
								}
							},
							{
								"id": "log_router",
								"from": {
									"path": "application.log"
								},
								"to": {
									"path": "kafka_dsl_test:log_processor"
								}
							}
						]
					}
				],
				"nodes": [
					{
						"id": "sensor_processor",
						"type": "jsTransform",
						"name": "传感器数据处理器",
						"configuration": {
							"jsScript": "var data = JSON.parse(msg);\nvar result = {\n  type: 'sensor_processed',\n  originalData: data,\n  processedAt: new Date().toISOString(),\n  topic: metadata.topic,\n  partition: metadata.partition,\n  offset: metadata.offset\n};\nmetadata.topic = 'processed.sensor.data';\nreturn {'msg': JSON.stringify(result), 'metadata': metadata, 'msgType': 'SENSOR_PROCESSED'};"
						},
						"debugMode": true
					},
					{
						"id": "device_processor",
						"type": "jsTransform",
						"name": "设备状态处理器",
						"configuration": {
							"jsScript": "var status = JSON.parse(msg);\nvar result = {\n  type: 'device_status_processed',\n  deviceId: status.deviceId,\n  status: status.status,\n  processedAt: new Date().toISOString(),\n  topic: metadata.topic,\n  partition: metadata.partition\n};\nmetadata.topic = 'processed.device.status';\nreturn {'msg': JSON.stringify(result), 'metadata': metadata, 'msgType': 'DEVICE_STATUS_PROCESSED'};"
						},
						"debugMode": true
					},
					{
						"id": "log_processor",
						"type": "jsTransform",
						"name": "日志处理器",
						"configuration": {
							"jsScript": "var log = JSON.parse(msg);\nvar result = {\n  type: 'log_processed',\n  level: log.level,\n  message: log.message,\n  userId: log.userId,\n  processedAt: new Date().toISOString(),\n  topic: metadata.topic\n};\nmetadata.topic = 'processed.application.log';\nreturn {'msg': JSON.stringify(result), 'metadata': metadata, 'msgType': 'LOG_PROCESSED'};"
						},
						"debugMode": true
					}
				],
				"connections": []
			}
		}`

		// Delete alert routes via ReloadSelf
		err := ruleEngine.ReloadSelf([]byte(reducedDSL))
		assert.Nil(t, err)

		time.Sleep(time.Second * 2)

		// Sending an alert message should not be processed (because the alert route has been deleted)
		alertMessage := map[string]interface{}{
			"level":     "warning",
			"message":   "This alert should not be processed",
			"timestamp": time.Now().Unix(),
		}
		alertJSON, _ := json.Marshal(alertMessage)
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: "system.alert",
			Value: sarama.StringEncoder(alertJSON),
		})
		assert.Nil(t, err)

		// Sending log messages should handle normally (because log routing still exists).
		logMessage := map[string]interface{}{
			"level":     "info",
			"message":   "This log should be processed",
			"userId":    "user456",
			"timestamp": time.Now().Unix(),
		}
		logJSON, _ := json.Marshal(logMessage)
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: "application.log",
			Value: sarama.StringEncoder(logJSON),
		})
		assert.Nil(t, err)

		time.Sleep(time.Second * 2)
	})

	// Test hot update – reload the entire DSL configuration
	t.Run("HotReload", func(t *testing.T) {
		// Create the updated DSL configuration
		updatedDSL := `{
			"ruleChain": {
				"id": "kafka_dsl_test",
				"name": "Kafka DSL Test Chain - Updated",
				"root": true,
				"debugMode": true
			},
			"metadata": {
				"endpoints": [
					{
						"id": "kafka_endpoint_1",
						"type": "kafka",
						"name": "Kafka Consumer - Updated",
						"configuration": {
							"server": "localhost:9092",
							"groupId": "test-group-v2",
							"autoOffsetReset": "latest",
							"autoCommit": true
						},
						"routers": [
							{
								"id": "updated_router",
								"from": {
									"path": "updated.topic"
								},
								"to": {
									"path": "kafka_dsl_test:updated_processor"
								}
							}
						]
					}
				],
				"nodes": [
					{
						"id": "updated_processor",
						"type": "jsTransform",
						"name": "更新后的处理器",
						"configuration": {
							"jsScript": "var data = JSON.parse(msg);\nvar result = {\n  type: 'updated_processed',\n  version: '2.0',\n  originalData: data,\n  processedAt: new Date().toISOString(),\n  topic: metadata.topic\n};\nmetadata.topic = 'processed.updated.topic';\nreturn {'msg': JSON.stringify(result), 'metadata': metadata, 'msgType': 'UPDATED_PROCESSED'};"
						},
						"debugMode": true
					}
				],
				"connections": []
			}
		}`

		// Perform hot updates
		err := ruleEngine.ReloadSelf([]byte(updatedDSL))
		assert.Nil(t, err)

		time.Sleep(time.Second * 2)

		// Test the updated route
		updatedMessage := map[string]interface{}{
			"message":   "This is an updated message",
			"version":   "2.0",
			"timestamp": time.Now().Unix(),
		}
		updatedJSON, _ := json.Marshal(updatedMessage)
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: "updated.topic",
			Value: sarama.StringEncoder(updatedJSON),
		})
		assert.Nil(t, err)

		// Verify that the old route no longer works — sending to the original topic should not be processed
		oldTopicMessage := map[string]interface{}{
			"sensorId":    "temp002",
			"temperature": 30.0,
			"timestamp":   time.Now().Unix(),
		}
		oldTopicJSON, _ := json.Marshal(oldTopicMessage)
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: "sensor.data", // This routing no longer exists in the updated DSL
			Value: sarama.StringEncoder(oldTopicJSON),
		})
		assert.Nil(t, err)

		time.Sleep(time.Second * 3)

	})

	// Release resources
	ruleEngine.Stop(context.Background())

	// Clean up registered functions
	action.Functions.UnRegister("validateSensorData")
	action.Functions.UnRegister("validateDeviceStatus")
}

// TestKafkaDSLWithMultipleConsumers tests the multi-consumer features in the Kafka DSL configuration
func TestKafkaDSLWithMultipleConsumers(t *testing.T) {
	// Multi-consumer DSL configuration
	multiConsumerDSL := `{
		"ruleChain": {
			"id": "kafka_multi_consumer_test",
			"name": "Kafka Multi Consumer Test Chain",
			"root": true,
			"debugMode": true
		},
		"metadata": {
			"endpoints": [
				{
					"id": "kafka_consumer_1",
					"type": "kafka",
					"name": "Kafka Consumer 1",
					"configuration": {
						"server": "localhost:9092",
						"groupId": "consumer-group-1",
						"autoOffsetReset": "earliest"
					},
					"routers": [
						{
							"id": "high_priority_router",
							"from": {
								"path": "high.priority"
							},
							"to": {
								"path": "kafka_multi_consumer_test:high_priority_processor"
							}
						}
					]
				},
				{
					"id": "kafka_consumer_2",
					"type": "kafka",
					"name": "Kafka Consumer 2",
					"configuration": {
						"server": "localhost:9092",
						"groupId": "consumer-group-2",
						"autoOffsetReset": "earliest"
					},
					"routers": [
						{
							"id": "low_priority_router",
							"from": {
								"path": "low.priority"
							},
							"to": {
								"path": "kafka_multi_consumer_test:low_priority_processor"
							}
						}
					]
				}
			],
			"nodes": [
				{
					"id": "high_priority_processor",
					"type": "jsTransform",
					"name": "高优先级处理器",
					"configuration": {
						"jsScript": "var data = JSON.parse(msg);\nvar result = {\n  type: 'high_priority_processed',\n  priority: 'HIGH',\n  data: data,\n  processedAt: new Date().toISOString()\n};\nreturn {'msg': JSON.stringify(result), 'metadata': metadata, 'msgType': 'HIGH_PRIORITY_PROCESSED'};"
					},
					"debugMode": true
				},
				{
					"id": "low_priority_processor",
					"type": "jsTransform",
					"name": "低优先级处理器",
					"configuration": {
						"jsScript": "var data = JSON.parse(msg);\nvar result = {\n  type: 'low_priority_processed',\n  priority: 'LOW',\n  data: data,\n  processedAt: new Date().toISOString()\n};\nreturn {'msg': JSON.stringify(result), 'metadata': metadata, 'msgType': 'LOW_PRIORITY_PROCESSED'};"
					},
					"debugMode": true
				}
			],
			"connections": []
		}
	}`

	// Create rule engine configurations
	config := rulego.NewConfig(
		types.WithDefaultPool(),
		types.WithOnDebug(func(chainId, flowType string, nodeId string, msg types.RuleMsg, relationType string, err error) {
			//t.Logf("[Multi-Consumer Debugging] Chain: %s, Node: %s, Message: %s", chainId, nodeId, msg.GetData())
		}),
	)

	// Create a rule engine
	ruleEngine, err := rulego.New("kafka_multi_consumer_test", []byte(multiConsumerDSL), engine.WithConfig(config))
	assert.Nil(t, err)

	// Wait for multiple consumers to launch
	time.Sleep(time.Second * 3)

	// Release resources
	ruleEngine.Stop(context.Background())
}
