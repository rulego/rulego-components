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
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/action"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/engine"
	"github.com/rulego/rulego/node_pool"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"

	kafkaclient "github.com/rulego/rulego-components/external/kafka"
)

// TestKafkaEndpointsSharedPoolIndependentConsumerGroups verifies that two
// endpoints borrowing the same pool entry still consume independently:
// the shared unit is the producer connection only, while each endpoint dials
// its own consumer group from the pool entry's brokers. Both groups receive
// the full stream (identical groupIds would instead share partitions).
func TestKafkaEndpointsSharedPoolIndependentConsumerGroups(t *testing.T) {
	broker := sharedTestBroker()
	skipIfNoKafka(t, broker)

	config := rulego.NewConfig()
	pool := node_pool.NewNodePool(config)
	config.NodePool = pool
	defer pool.Stop()

	// Pool source: an x/kafkaProducer node builds the shared connection.
	var producerDef types.RuleNode
	assert.Nil(t, json.Unmarshal([]byte(`{
		"id": "chain_ref_pool_producer",
		"type": "x/kafkaProducer",
		"name": "pool producer",
		"configuration": {"server": "`+broker+`", "topic": "chain.ref.pool.ignored"}
	}`), &producerDef))
	_, err := pool.NewFromRuleNode(producerDef)
	assert.Nil(t, err)

	// Two borrower endpoints, distinct groupIds, each observing its own stream.
	gotA := make(chan struct{}, 1)
	gotB := make(chan struct{}, 1)
	topic := "chain.ref.pool.independent"
	mkBorrower := func(group string, got chan struct{}) *Kafka {
		ep, err := endpoint.Registry.New(Type, config, Config{
			Server:  "ref://chain_ref_pool_producer",
			GroupId: uniqueGroupId(group),
		})
		assert.Nil(t, err)
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
		return ep.(*Kafka)
	}
	epA := mkBorrower("chain_ref_group_a", gotA)
	epB := mkBorrower("chain_ref_group_b", gotB)
	defer func() { epA.Destroy(); epB.Destroy() }()

	// Both borrowers resolve to the same pool entry with the same brokers.
	connA, err := epA.SharedNode.GetSafely()
	assert.Nil(t, err)
	connB, err := epB.SharedNode.GetSafely()
	assert.Nil(t, err)
	if connA != connB {
		t.Fatal("both borrowers should resolve to the same pool entry")
	}
	if len(connA.Brokers) == 0 || connA.Brokers[0] != broker {
		t.Fatalf("pool entry brokers = %v, want [%s]", connA.Brokers, broker)
	}

	// OffsetNewest misses messages sent before rebalance completes: resend until
	// both independent consumer groups have each seen the message.
	time.Sleep(3 * time.Second)
	prod := newSaramaProducer(t, broker)
	defer prod.Close()
	deadline := time.After(30 * time.Second)
	retry := time.NewTicker(2 * time.Second)
	defer retry.Stop()
	send := func() {
		_, _, err := prod.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder("independent ping"),
		})
		assert.Nil(t, err)
	}
	send()
	gotAFlag, gotBFlag := false, false
	for !(gotAFlag && gotBFlag) {
		select {
		case <-gotA:
			gotAFlag = true
		case <-gotB:
			gotBFlag = true
		case <-retry.C:
			send()
		case <-deadline:
			t.Fatalf("timeout: groupA received=%v groupB received=%v", gotAFlag, gotBFlag)
		}
	}
}

// TestKafkaChainRefProducerBorrowsEndpoint verifies the canvas-level ref://
// feature end to end: an x/kafkaProducer node on a chain borrows the chain's
// own endpoint/kafka (registered by EndpointAspect into the chain resource
// directory), publishes a message, and the endpoint's independent consumer
// group still delivers it to the routed chain node.
func TestKafkaChainRefProducerBorrowsEndpoint(t *testing.T) {
	broker := sharedTestBroker()
	skipIfNoKafka(t, broker)

	var consumed int32
	action.Functions.Register("kafkaChainRefMarker", func(ctx types.RuleContext, msg types.RuleMsg) {
		atomic.StoreInt32(&consumed, 1)
		ctx.TellSuccess(msg)
	})

	topic := "chain.ref.producer.borrow"
	chainId := "test_kafka_chain_ref"
	chain := `{
	  "ruleChain": {"id": "` + chainId + `", "name": "kafka chain-scoped ref", "root": true},
	  "metadata": {
	    "firstNodeIndex": 0,
	    "endpoints": [
	      {"id": "ep_kafka", "type": "endpoint/kafka",
	       "configuration": {"server": "` + broker + `", "groupId": "` + uniqueGroupId("chain_ref_ep_group") + `"},
	       "routers": [{"from": {"path": "` + topic + `"}, "to": {"path": "` + chainId + `:marker"}}]}
	    ],
	    "nodes": [
	      {"id": "marker", "type": "functions", "configuration": {"functionName": "kafkaChainRefMarker"}},
	      {"id": "prod", "type": "x/kafkaProducer", "configuration": {"server": "ref://ep_kafka", "topic": "` + topic + `"}}
	    ],
	    "connections": []
	  }
	}`

	config := rulego.NewConfig(types.WithEndpointEnabled(true))
	eng, err := rulego.New(chainId, []byte(chain), engine.WithConfig(config))
	assert.Nil(t, err)
	defer func() {
		eng.Stop(context.Background())
		action.Functions.UnRegister("kafkaChainRefMarker")
	}()
	ruleEng, ok := eng.(*engine.RuleEngine)
	if !ok {
		t.Fatalf("engine type %T not *engine.RuleEngine", eng)
	}
	// The endpoint must be registered by EndpointAspect for the ref to resolve.
	if _, found := ruleEng.RootRuleChainCtx().Resources().Lookup("ep_kafka"); !found {
		t.Fatal("ep_kafka not registered in chain resources")
	}

	// Drive the producer node directly; retry because OffsetNewest drops
	// messages published before the consumer group finishes rebalancing.
	nc, found := ruleEng.RootRuleChainCtx().GetNodeById(types.RuleNodeId{Id: "prod"})
	if !found {
		t.Fatal("node prod not found")
	}
	producerNode, ok := nc.(*engine.RuleNodeCtx).Node.(*kafkaclient.ProducerNode)
	if !ok {
		t.Fatalf("node prod type %T not *kafkaclient.ProducerNode", nc.(*engine.RuleNodeCtx).Node)
	}
	success := make(chan struct{}, 1)
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
		if relationType == types.Success {
			select {
			case success <- struct{}{}:
			default:
			}
		}
	})
	deadline := time.After(30 * time.Second)
	retry := time.NewTicker(2 * time.Second)
	defer retry.Stop()
	send := func() {
		msg := ctx.NewMsg("TEST_MSG", types.NewMetadata(), "chain ref ping")
		producerNode.OnMsg(ctx, msg)
	}
	time.Sleep(3 * time.Second)
	send()
	for atomic.LoadInt32(&consumed) != 1 {
		select {
		case <-success:
			// published ok, keep waiting for the endpoint consumer
		case <-retry.C:
			send()
		case <-deadline:
			t.Fatalf("timeout: endpoint consumer did not receive the borrowed producer's message (consumed=%d)", atomic.LoadInt32(&consumed))
		}
	}
}
