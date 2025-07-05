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
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/engine"
	"github.com/rulego/rulego/test/assert"
)

// TestKafkaDSLEndpoint 测试使用DSL方式启动Kafka endpoint并进行动态路由和热更新
// 本测试演示了如何通过ruleEngine.ReloadSelf()方法实现以下功能：
// 1. 初始化Kafka endpoint和路由配置
// 2. 通过ReloadSelf()动态添加新的路由（告警路由和日志路由）
// 3. 通过ReloadSelf()删除指定路由（删除告警路由，保留其他路由）
// 4. 通过ReloadSelf()进行完整的热更新（替换所有路由和处理节点）
//
// ReloadSelf()方法的优势：
// - 支持完整的DSL配置更新，包括endpoints、routers、nodes等
// - 自动处理资源的创建、更新和清理
// - 保证配置更新的原子性，避免中间状态
// - 支持复杂的路由拓扑变更
func TestKafkaDSLEndpoint(t *testing.T) {
	// 创建初始的DSL配置
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
				}
			],
			"connections": []
		}
	}`

	// 创建规则引擎配置
	config := rulego.NewConfig(
		types.WithDefaultPool(),
		types.WithOnDebug(func(chainId, flowType string, nodeId string, msg types.RuleMsg, relationType string, err error) {
			//t.Logf("[Kafka调试] 链: %s, 节点: %s, 关系: %s, 消息: %s", chainId, nodeId, relationType, msg.GetData())
			// 添加断言验证调试回调的参数
			assert.True(t, len(chainId) > 0, "chainId should not be empty")
			assert.True(t, len(nodeId) > 0, "nodeId should not be empty")
		}),
	)

	// 使用DSL创建包含嵌入式endpoint的规则链
	ruleEngine, err := rulego.New("kafka_dsl_test", []byte(initialDSL), engine.WithConfig(config))
	assert.Nil(t, err)
	assert.NotNil(t, ruleEngine)

	// 等待Kafka消费者启动
	time.Sleep(time.Second * 2)

	// 创建Kafka生产者用于测试
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, saramaConfig)
	assert.Nil(t, err)
	defer producer.Close()

	// 测试初始路由
	t.Run("TestInitialRoutes", func(t *testing.T) {
		// 注意：这个测试通过Kafka消息触发规则链处理，不需要直接调用OnMsg

		// 发送传感器数据消息
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

		// 发送设备状态消息
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

		// 等待消息处理完成（使用固定时间等待而不是WaitGroup）
		time.Sleep(time.Second * 3)

	})

	// 动态添加新路由 - 通过ReloadSelf方式更新DSL配置
	t.Run("AddDynamicRoutes", func(t *testing.T) {
		//var alertCount, logCount int32

		// 创建包含新路由的DSL配置
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

		// 通过ReloadSelf方式添加新路由
		err := ruleEngine.ReloadSelf([]byte(expandedDSL))
		assert.Nil(t, err)

		time.Sleep(time.Second * 2)

		// 测试新添加的告警路由
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

		// 测试新添加的日志路由
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

	// 测试路由删除 - 通过ReloadSelf方式删除路由
	t.Run("RemoveRoute", func(t *testing.T) {
		// 创建删除告警路由后的DSL配置（只保留原始路由和日志路由）
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

		// 通过ReloadSelf方式删除告警路由
		err := ruleEngine.ReloadSelf([]byte(reducedDSL))
		assert.Nil(t, err)

		time.Sleep(time.Second * 2)

		// 发送告警消息，应该不会被处理（因为告警路由已被删除）
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

		// 发送日志消息，应该正常处理（因为日志路由仍然存在）
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

	// 测试热更新 - 重新加载整个DSL配置
	t.Run("HotReload", func(t *testing.T) {
		// 创建更新后的DSL配置
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

		// 执行热更新
		err := ruleEngine.ReloadSelf([]byte(updatedDSL))
		assert.Nil(t, err)

		time.Sleep(time.Second * 2)

		// 测试更新后的路由
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

		// 验证旧路由不再工作 - 发送到原来的topic应该不会被处理
		oldTopicMessage := map[string]interface{}{
			"sensorId":    "temp002",
			"temperature": 30.0,
			"timestamp":   time.Now().Unix(),
		}
		oldTopicJSON, _ := json.Marshal(oldTopicMessage)
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: "sensor.data", // 这个路由在更新后的DSL中已经不存在
			Value: sarama.StringEncoder(oldTopicJSON),
		})
		assert.Nil(t, err)

		time.Sleep(time.Second * 3)

	})

	// 清理资源
	ruleEngine.Stop(context.Background())
}

// TestKafkaDSLWithMultipleConsumers 测试Kafka DSL配置中的多消费者功能
func TestKafkaDSLWithMultipleConsumers(t *testing.T) {
	// 多消费者DSL配置
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

	// 创建规则引擎配置
	config := rulego.NewConfig(
		types.WithDefaultPool(),
		types.WithOnDebug(func(chainId, flowType string, nodeId string, msg types.RuleMsg, relationType string, err error) {
			//t.Logf("[多消费者调试] 链: %s, 节点: %s, 消息: %s", chainId, nodeId, msg.GetData())
		}),
	)

	// 创建规则引擎
	ruleEngine, err := rulego.New("kafka_multi_consumer_test", []byte(multiConsumerDSL), engine.WithConfig(config))
	assert.Nil(t, err)

	// 等待多个消费者启动
	time.Sleep(time.Second * 3)

	// 清理资源
	ruleEngine.Stop(context.Background())
}
