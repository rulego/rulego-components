/*
 * Copyright 2026 The RuleGo Authors.
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
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/node_pool"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"

	externalkafka "github.com/rulego/rulego-components/external/kafka"
)

// sharedTestBroker 返回测试用 broker，可用 KAFKA_BROKERS 覆盖（与其他 kafka 测试一致）
func sharedTestBroker() string {
	if b := os.Getenv("KAFKA_BROKERS"); b != "" {
		return b
	}
	return "localhost:9092"
}

// skipIfNoKafka 探测 broker，不可达则跳过（本机无 kafka 时不阻断单元测试）
func skipIfNoKafka(t *testing.T, broker string) {
	t.Helper()
	cfg := sarama.NewConfig()
	cfg.Net.DialTimeout = time.Second
	client, err := sarama.NewClient([]string{broker}, cfg)
	if err != nil {
		t.Skipf("kafka broker %s not available: %v", broker, err)
		return
	}
	_ = client.Close()
}

func newSaramaProducer(t *testing.T, broker string) sarama.SyncProducer {
	t.Helper()
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Retry.Max = 3
	prod, err := sarama.NewSyncProducer([]string{broker}, cfg)
	assert.Nil(t, err)
	return prod
}

// newSaramaConsumer 订阅主题分区0（OffsetNewest），返回消息通道与清理函数；
// 必须在产生消息前建立，否则 OffsetNewest 会错过已写入的消息
func newSaramaConsumer(t *testing.T, broker, topic string) (<-chan *sarama.ConsumerMessage, func()) {
	t.Helper()
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	client, err := sarama.NewClient([]string{broker}, cfg)
	assert.Nil(t, err)
	consumer, err := sarama.NewConsumerFromClient(client)
	assert.Nil(t, err)

	pc, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	assert.Nil(t, err)
	cleanup := func() {
		_ = pc.Close()
		_ = consumer.Close()
		_ = client.Close()
	}
	return pc.Messages(), cleanup
}

// TestKafkaEndpointSharedConnection 验证 endpoint/kafka 经全局节点池共享连接：
// owner 端点建连入池，ref:// 借用端点复用同一 SharedConn 的 producer 与 brokers，
// 借用端各自独立消费组仍可正常收发；借用方关闭不影响池源连接。
func TestKafkaEndpointSharedConnection(t *testing.T) {
	broker := sharedTestBroker()
	skipIfNoKafka(t, broker)

	config := rulego.NewConfig()
	pool := node_pool.NewNodePool(config)
	config.NodePool = pool
	defer pool.Stop()

	// 池源：owner 端点（建 producer 入池）
	var ownerDef types.EndpointDsl
	assert.Nil(t, json.Unmarshal([]byte(`{
		"id": "shared_kafka_owner",
		"type": "endpoint/kafka",
		"name": "共享kafka连接",
		"configuration": {"server": "`+broker+`", "groupId": "shared_owner_group"}
	}`), &ownerDef))
	ownerCtx, err := pool.NewFromEndpoint(ownerDef)
	assert.Nil(t, err)
	assert.NotNil(t, ownerCtx)

	// 池源实例即 *SharedConn
	inst, err := pool.GetInstance("shared_kafka_owner")
	assert.Nil(t, err)

	// 借用端点：server=ref:// 池源，独立消费组
	borrower, err := endpoint.Registry.New(Type, config, Config{
		Server:  "ref://shared_kafka_owner",
		GroupId: "shared_borrower_group",
	})
	assert.Nil(t, err)
	kafkaBorrower := borrower.(*Kafka)
	assert.True(t, kafkaBorrower.SharedNode.IsFromPool())

	var got int32
	reqTopic := "shared.endpoint.test.request"
	respTopic := "shared.endpoint.test.response"
	router := endpoint.NewRouter().From(reqTopic).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		atomic.StoreInt32(&got, 1)
		// 响应经共享 producer 发回
		exchange.Out.Headers().Add(KeyResponseTopic, respTopic)
		exchange.Out.SetBody([]byte("shared pong"))
		return true
	}).End()
	_, err = borrower.AddRouter(router)
	assert.Nil(t, err)
	assert.Nil(t, borrower.Start())

	// 等待消费组 rebalance
	time.Sleep(3 * time.Second)

	// 先订阅响应主题（OffsetNewest 必须在响应产生前建立），再发请求
	respMsgs, respCleanup := newSaramaConsumer(t, broker, respTopic)
	defer respCleanup()

	prod := newSaramaProducer(t, broker)
	defer prod.Close()
	_, _, err = prod.SendMessage(&sarama.ProducerMessage{
		Topic: reqTopic,
		Value: sarama.StringEncoder("shared ping"),
	})
	assert.Nil(t, err)

	select {
	case msg := <-respMsgs:
		assert.Equal(t, "shared pong", string(msg.Value))
	case <-time.After(30 * time.Second):
		t.Errorf("timeout waiting response on topic %s (consumer received=%v)", respTopic, atomic.LoadInt32(&got) == 1)
	}
	assert.Equal(t, int32(1), atomic.LoadInt32(&got))

	// 借用方关闭：不关闭池源连接（ref:// 借用方 Close 为 no-op）
	_ = kafkaBorrower.Close()
	time.Sleep(time.Second)
	inst2, err := pool.GetInstance("shared_kafka_owner")
	assert.Nil(t, err)
	assert.Equal(t, inst, inst2) // 池源实例不受借用方关闭影响
}

// TestKafkaEndpointBorrowFromProducer 验证反向共享：
// x/kafkaProducer 节点作为池源建连，endpoint/kafka 经 ref:// 借用其 SharedConn，
// 消费组用 producer 池条目携带的 brokers 独立建连后正常消费。
func TestKafkaEndpointBorrowFromProducer(t *testing.T) {
	broker := sharedTestBroker()
	skipIfNoKafka(t, broker)

	config := rulego.NewConfig()
	pool := node_pool.NewNodePool(config)
	config.NodePool = pool
	defer pool.Stop()

	// 池源：x/kafkaProducer 节点（NewFromRuleNode 要求组件实现 SharedNode）
	var producerDef types.RuleNode
	assert.Nil(t, json.Unmarshal([]byte(`{
		"id": "shared_kafka_producer",
		"type": "x/kafkaProducer",
		"name": "共享kafka生产者",
		"configuration": {"server": "`+broker+`", "topic": "shared.producer.ignored"}
	}`), &producerDef))
	producerCtx, err := pool.NewFromRuleNode(producerDef)
	assert.Nil(t, err)
	assert.NotNil(t, producerCtx)

	// endpoint 借用 producer 池条目
	ep, err := endpoint.Registry.New(Type, config, Config{
		Server:  "ref://shared_kafka_producer",
		GroupId: "shared_from_producer_group",
	})
	assert.Nil(t, err)

	got := make(chan struct{}, 1)
	topic := "shared.producer.borrow.test"
	router := endpoint.NewRouter().From(topic).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		select {
		case got <- struct{}{}:
		default:
		}
		return true
	}).End()
	_, err = ep.AddRouter(router)
	assert.Nil(t, err)
	assert.Nil(t, ep.Start())

	time.Sleep(3 * time.Second)

	prod := newSaramaProducer(t, broker)
	defer prod.Close()
	_, _, err = prod.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("borrow ping"),
	})
	assert.Nil(t, err)

	select {
	case <-got:
		// 借用 producer 池连接的端点正常消费
	case <-time.After(30 * time.Second):
		t.Errorf("timeout: endpoint borrowing producer pool connection did not receive message")
	}
}

// TestKafkaProducerBorrowFromEndpoint 验证 endpoint/kafka 作为池源、
// x/kafkaProducer 节点经 ref:// 借用其 SharedConn 发送消息。
func TestKafkaProducerBorrowFromEndpoint(t *testing.T) {
	broker := sharedTestBroker()
	skipIfNoKafka(t, broker)

	config := rulego.NewConfig()
	pool := node_pool.NewNodePool(config)
	config.NodePool = pool
	defer pool.Stop()

	// 池源：endpoint/kafka
	var epDef types.EndpointDsl
	assert.Nil(t, json.Unmarshal([]byte(`{
		"id": "shared_kafka_ep_src",
		"type": "endpoint/kafka",
		"name": "共享kafka端点",
		"configuration": {"server": "`+broker+`", "groupId": "shared_ep_src_group"}
	}`), &epDef))
	epCtx, err := pool.NewFromEndpoint(epDef)
	assert.Nil(t, err)
	assert.NotNil(t, epCtx)

	// 借用方：x/kafkaProducer 节点，server 引用 endpoint 池源
	sendTopic := "shared.ep.to.producer.test"
	var producerNode externalkafka.ProducerNode
	err = producerNode.Init(config, types.Configuration{
		"server": "ref://shared_kafka_ep_src",
		"topic":  sendTopic,
	})
	assert.Nil(t, err)
	assert.True(t, producerNode.SharedNode.IsFromPool())
	defer producerNode.Destroy()

	// 先订阅目标主题再发送
	msgs, cleanup := newSaramaConsumer(t, broker, sendTopic)
	defer cleanup()

	success := make(chan struct{}, 1)
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Success, relationType)
		select {
		case success <- struct{}{}:
		default:
		}
	})
	msg := ctx.NewMsg("TEST_MSG", types.NewMetadata(), "producer borrow ping")
	producerNode.OnMsg(ctx, msg)

	select {
	case <-success:
	case <-time.After(15 * time.Second):
		t.Errorf("producer node borrowing endpoint pool connection did not report success")
	}
	select {
	case m := <-msgs:
		assert.Equal(t, "producer borrow ping", string(m.Value))
	case <-time.After(15 * time.Second):
		t.Errorf("timeout waiting message on topic %s", sendTopic)
	}
}
