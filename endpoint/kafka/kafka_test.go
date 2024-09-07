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
	"os"
	"sync"
	"testing"
	"time"
)

var testdataFolder = "../../testdata"

func TestKafkaEndpointInit(t *testing.T) {
	config := rulego.NewConfig(types.WithDefaultPool())
	_, err := endpoint.Registry.New(Type, config, Config{
		Server:  "",
		GroupId: "test01",
	})
	assert.Equal(t, "brokers is empty", err.Error())

	ep, err := endpoint.Registry.New(Type, config, Config{
		Server: "localhost:9092",
	})
	assert.Equal(t, "rulego", ep.(*Kafka).Config.GroupId)

	ep, err = endpoint.Registry.New(Type, config, Config{
		Server: "localhost:9092,localhost:9093",
	})
	assert.Equal(t, "localhost:9092", ep.(*Kafka).brokers[0])
	assert.Equal(t, "localhost:9093", ep.(*Kafka).brokers[1])

	ep, err = endpoint.Registry.New(Type, config, types.Configuration{
		"brokers": []string{"localhost:9092", "localhost:9093"},
	})
	assert.Equal(t, "localhost:9092", ep.(*Kafka).brokers[0])
	assert.Equal(t, "localhost:9093", ep.(*Kafka).brokers[1])
}

func TestKafkaEndpoint(t *testing.T) {

	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	//注册规则链
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	//启动kafka接收服务
	kafkaEndpoint, err := endpoint.Registry.New(Type, config, Config{
		Server:  "localhost:9092",
		GroupId: "test01",
	})
	//路由1
	router1 := endpoint.NewRouter().From("device.msg.request").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		//fmt.Println("接收到数据：device.msg.request", exchange.In.GetMsg())
		assert.Equal(t, "test message", exchange.In.GetMsg().Data)
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		//往指定主题发送数据，用于响应
		exchange.Out.Headers().Add(KeyResponseTopic, "device.msg.response")
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()

	//模拟获取响应
	router2 := endpoint.NewRouter().From("device.msg.response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		//fmt.Println("接收到数据：device.msg.response", exchange.In.GetMsg())
		assert.Equal(t, "this is response", exchange.In.GetMsg().Data)
		return true
	}).End()

	router3 := endpoint.NewRouter().From("device.msg.response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		//fmt.Println("接收到数据：device.msg.response", exchange.In.GetMsg())
		assert.Equal(t, "this is response", exchange.In.GetMsg().Data)
		return true
	}).End()

	//注册路由
	_, err = kafkaEndpoint.AddRouter(router1)
	_, err = kafkaEndpoint.AddRouter(router2)
	if err != nil {
		panic(err)
	}
	_, err = kafkaEndpoint.AddRouter(router3)
	assert.NotNil(t, err)
	//并启动服务
	err = kafkaEndpoint.Start()
	if err != nil {
		panic(err)
	}

	// 测试发布和订阅
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		t.Fatal("Failed to start Sarama producer:", err)
	}
	defer producer.Close()

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		t.Fatal("Failed to start Sarama consumer:", err)
	}
	defer consumer.Close()
	var wg sync.WaitGroup
	wg.Add(1)

	go func(g *sync.WaitGroup) {
		// 创建消费者来读取 device.msg.response
		partitionConsumer, err := consumer.ConsumePartition("device.msg.response", 0, sarama.OffsetNewest)
		if err != nil {
			t.Fatal("Failed to start consumer for response topic:", err)
		}
		defer partitionConsumer.Close()

		// 等待并验证响应
		select {
		case msg := <-partitionConsumer.Messages():
			assert.Equal(t, "this is response", string(msg.Value))
			g.Done()
		case <-time.After(5 * time.Second):
			g.Done()
			t.Fatal("Failed to receive message within the timeout period")
		}
	}(&wg)

	time.Sleep(time.Second)
	// 发布消息到 device.msg.request
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: "device.msg.request",
		Value: sarama.StringEncoder("test message"),
	})
	if err != nil {
		t.Fatal("Failed to send message:", err)
	}
	wg.Wait()
	kafkaEndpoint.Destroy()
}
