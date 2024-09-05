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

package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/test/assert"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
)

var testdataFolder = "../../testdata"

const (
	server        = "amqp://guest:guest@8.134.32.225:5672/"
	exchange      = "rulego.topic.test"
	topicRequest  = "device.msg.request"
	topicResponse = "device.msg.response"
)

func TestEndpoint(t *testing.T) {
	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	// 注册规则链
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	// 启动enpoint接收服务
	ep, err := endpoint.Registry.New(Type, config, Config{
		Server:   server,
		Exchange: exchange,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 路由1
	router1 := endpoint.NewRouter().From(topicRequest).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "test message", exchange.In.GetMsg().Data)
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// 往指定主题发送数据，用于响应
		exchange.Out.Headers().Add(KeyResponseTopic, topicResponse)
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()

	count := int32(0)
	// 模拟获取响应
	router2 := endpoint.NewRouter().SetId("router3").From(topicResponse).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		//fmt.Println("接收到数据：device.msg.response", exchange.In.GetMsg())
		assert.Equal(t, "this is response", exchange.In.GetMsg().Data)
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// 模拟获取响应,相同主题
	router3 := endpoint.NewRouter().SetId("router3").From(topicResponse).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "this is response", exchange.In.GetMsg().Data)
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// 注册路由
	_, err = ep.AddRouter(router1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ep.AddRouter(router2)
	if err != nil {
		t.Fatal(err)
	}
	router3Id, err := ep.AddRouter(router3)
	assert.NotNil(t, err)
	// 启动服务
	err = ep.Start()
	if err != nil {
		t.Fatal(err)
	}

	// 测试发布和订阅
	conn, err := amqp.Dial(server)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()

	// 发布消息到device.msg.request
	err = channel.Publish(
		exchange,     // 发布到的交换机
		topicRequest, // 路由键
		false,        // 表示是否要求消息必须被路由到至少一个队列
		false,        // 是否要求消息立即被消费者接收
		amqp.Publishing{
			ContentType:     ContentTypeJson,
			ContentEncoding: KeyUTF8,
			Body:            []byte("test message"),
		})
	if err != nil {
		t.Fatal(err)
	}
	// 等待消息处理
	time.Sleep(time.Second * 1)

	assert.Equal(t, int32(1), count)

	count = 0
	//删除一个相同的主题
	_ = ep.RemoveRouter(router3Id)
	// 发布消息到device.msg.request
	err = channel.Publish(
		exchange,     // 发布到的交换机
		topicRequest, // 路由键
		false,        // 表示是否要求消息必须被路由到至少一个队列
		false,        // 是否要求消息立即被消费者接收
		amqp.Publishing{
			ContentType:     ContentTypeJson,
			ContentEncoding: KeyUTF8,
			Body:            []byte("test message"),
		})
	if err != nil {
		t.Fatal(err)
	}
	// 等待消息处理
	time.Sleep(time.Second * 1)

	assert.Equal(t, int32(0), count)

}
