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

package rabbitmq

import (
	"os"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
)

func TestRabbitMQClient(t *testing.T) {
	// 如果设置了跳过 RabbitMQ 测试，则跳过
	if os.Getenv("SKIP_RABBITMQ_TESTS") == "true" {
		t.Skip("Skipping RabbitMQ tests")
	}

	// 检查是否有可用的 RabbitMQ 服务器
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		rabbitmqURL = "amqp://guest:guest@localhost:5672/"
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/rabbitmqClient"

	t.Run("InitNode", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":   rabbitmqURL,
			"exchange": "test_exchange",
			"key":      "test.route",
		}, Registry)
		assert.Nil(t, err)
		assert.NotNil(t, node)

		clientNode := node.(*ClientNode)
		assert.Equal(t, rabbitmqURL, clientNode.Config.Server)
		assert.Equal(t, "test_exchange", clientNode.Config.Exchange)
		assert.Equal(t, "test.route", clientNode.Config.Key)
	})

	t.Run("SendMessage", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":   rabbitmqURL,
			"exchange": "test_exchange",
			"key":      "test.route",
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create RabbitMQ node: %v", err)
		}

		config := types.NewConfig()
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			if err != nil {
				t.Logf("RabbitMQ operation result: %s, error: %v", relationType, err)
			} else {
				assert.Equal(t, types.Success, relationType)
			}
		})

		metaData := types.NewMetadata()
		metaData.PutValue("exchange", "test_exchange")
		metaData.PutValue("routeKey", "test.route")

		msg := ctx.NewMsg("TEST_MSG_TYPE", metaData, "{\"test\":\"message\"}")

		clientNode := node.(*ClientNode)
		clientNode.OnMsg(ctx, msg)

		// 等待消息处理
		time.Sleep(time.Millisecond * 100)

		clientNode.Destroy()
	})
}

func TestRabbitMQClientConfig(t *testing.T) {
	// 如果设置了跳过 RabbitMQ 测试，则跳过
	if os.Getenv("SKIP_RABBITMQ_TESTS") == "true" {
		t.Skip("Skipping RabbitMQ tests")
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/rabbitmqClient"

	//t.Run("EmptyServerConfig", func(t *testing.T) {
	//	_, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
	//		"server": "",
	//	}, Registry)
	//	assert.NotNil(t, err)
	//})

	t.Run("InvalidServerConfig", func(t *testing.T) {
		_, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "invalid://server:9999",
		}, Registry)
		// 应该能创建节点，但连接会失败
		assert.Nil(t, err)
	})
}
