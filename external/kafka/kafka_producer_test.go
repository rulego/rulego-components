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
	"errors"
	"github.com/IBM/sarama"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
	"testing"
	"time"
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
	var node ProducerNode
	var configuration = make(types.Configuration)
	configuration["topic"] = "device.msg.request"
	configuration["key"] = "${metadata.id}"
	configuration["server"] = "localhost:9092"
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
		_, err = producerNode.SharedNode.Get()
		if err != nil {
			t.Skipf("Kafka server not available: %v", err)
			return
		}

		// 验证客户端已创建
		assert.NotNil(t, producerNode.client)

		// 重置客户端
		producerNode.resetClient()

		// 验证客户端已被重置
		assert.Nil(t, producerNode.client)
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

//func TestKafkaProducerAlways(t *testing.T) {
//	Registry := &types.SafeComponentSlice{}
//	Registry.Add(&ProducerNode{})
//	var targetNodeType = "x/kafkaProducer"
//	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
//		"server": "localhost:9092",
//		"topic":  "test.handler.topic",
//		"key":    "test-key",
//	}, Registry)
//	assert.Nil(t, err)
//
//	producerNode := node.(*ProducerNode)
//
//	// 测试消息发送（需要Kafka服务器运行）
//	config := types.NewConfig()
//	successCount := 0
//	failureCount := 0
//
//	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
//		if relationType == types.Success {
//			successCount++
//		} else {
//			failureCount++
//		}
//	})
//
//	metaData := types.NewMetadata()
//	var i = 0
//	for {
//		// 发送消息
//		msg := ctx.NewMsg("TEST_MSG_TYPE", metaData, fmt.Sprintf("{\"test\":\"reconnect\",\"index\":%d}", i))
//		producerNode.OnMsg(ctx, msg)
//		time.Sleep(time.Millisecond * 1000)
//		i++
//	}
//
//	producerNode.Destroy()
//}
