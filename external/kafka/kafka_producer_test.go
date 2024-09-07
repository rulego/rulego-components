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
