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
	"fmt"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/test/assert"
	"os"
	"os/signal"
	"syscall"
	"testing"
)

var testdataFolder = "../../testdata"

func TestKafkaEndpoint(t *testing.T) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	//注册规则链
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	//启动kafka接收服务
	kafkaEndpoint, err := endpoint.New(Type, config, Config{
		Brokers: []string{"localhost:9092"},
	})
	//路由1
	router1 := endpoint.NewRouter().From("device.msg.request").Process(func(router *endpoint.Router, exchange *endpoint.Exchange) bool {

		fmt.Println("接收到数据：device.msg.request", exchange.In.GetMsg())
		return true
	}).To("chain:default").Process(func(router *endpoint.Router, exchange *endpoint.Exchange) bool {
		//往指定主题发送数据，用于响应
		exchange.Out.Headers().Add("topic", "device.msg.response")
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()

	//模拟获取响应
	router2 := endpoint.NewRouter().From("device.msg.response").Process(func(router *endpoint.Router, exchange *endpoint.Exchange) bool {

		fmt.Println("接收到数据：device.msg.response", exchange.In.GetMsg())
		assert.Equal(t, "this is response", exchange.In.GetMsg().Data)
		return true
	}).End()

	//注册路由
	_, err = kafkaEndpoint.AddRouter(router1)
	_, err = kafkaEndpoint.AddRouter(router2)
	if err != nil {
		panic(err)
	}
	//并启动服务
	err = kafkaEndpoint.Start()
	if err != nil {
		panic(err)
	}
	<-c
}
