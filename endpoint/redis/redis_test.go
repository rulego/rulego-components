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

package redis

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/test/assert"

	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
)

var testdataFolder = "../../testdata"
var redisServer = "127.0.0.1:6379"

func TestRedisEndpoint(t *testing.T) {
	// 检查是否有可用的 Redis 服务器
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "127.0.0.1:6379"
	}

	// 如果设置了跳过 Redis 测试，则跳过
	if os.Getenv("SKIP_REDIS_TESTS") == "true" {
		t.Skip("Skipping Redis tests")
	}

	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	// 注册规则链
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	// 启动redis接收服务
	ep, err := endpoint.Registry.New(Type, config, Config{
		Server: redisURL,
	})
	if err != nil {
		t.Skipf("Failed to create Redis endpoint (Redis may not be available): %v", err)
	}
	// 关闭端点，避免 -count>1 时残留订阅干扰后续轮次
	defer func() {
		if c, ok := ep.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	count := int32(0)
	// 路由1
	router1 := endpoint.NewRouter().SetId("router1").From("device.msg.request,device.msg.response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		atomic.AddInt32(&count, 1)
		if exchange.In.Headers().Get("topic") == "device.msg.response" {
			assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
			return false
		}
		assert.Equal(t, "test message", exchange.In.GetMsg().GetData())
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// 往指定主题发送数据，用于响应
		exchange.Out.Headers().Add(KeyResponseTopic, "device.msg.response")
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()
	//重复路由，无法注册
	router2 := endpoint.NewRouter().SetId("router1").From("device.msg.request,device.msg.response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		atomic.AddInt32(&count, 1)
		if exchange.In.Headers().Get("topic") == "device.msg.response" {
			assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
			return false
		}
		assert.Equal(t, "test message", exchange.In.GetMsg().GetData())
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// 往指定主题发送数据，用于响应
		exchange.Out.Headers().Add(KeyResponseTopic, "device.msg.response")
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()

	// 注册路由
	_, err = ep.AddRouter(router1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ep.AddRouter(router2)
	assert.NotNil(t, err)
	// 启动服务
	err = ep.Start()
	if err != nil {
		t.Skipf("Failed to start Redis endpoint: %v", err)
	}

	// 测试发布和订阅
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisURL,
	})
	err = redisClient.Ping(context.Background()).Err()
	if err != nil {
		t.Skipf("Redis server not available: %v", err)
	}
	// 发布消息到device.msg.request
	redisClient.Publish(context.TODO(), "device.msg.request", "test message")
	// 等待消息处理
	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, int32(2), atomic.LoadInt32(&count))
	atomic.StoreInt32(&count, 0)

	router3 := endpoint.NewRouter().SetId("router3").From("device.msg.request").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		atomic.AddInt32(&count, 1)
		if exchange.In.Headers().Get("topic") == "device.msg.response" {
			assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
			return false
		}
		assert.Equal(t, "test message", exchange.In.GetMsg().GetData())
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// 往指定主题发送数据，用于响应
		exchange.Out.Headers().Add(KeyResponseTopic, "device.msg.response")
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()

	_, err = ep.AddRouter(router3)
	if err != nil {
		t.Fatal(err)
	}
	// 发布消息到device.msg.request
	redisClient.Publish(context.TODO(), "device.msg.request", "test message")
	// 等待消息处理
	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, int32(4), atomic.LoadInt32(&count))
	atomic.StoreInt32(&count, 0)

	_ = ep.RemoveRouter("router3")

	redisClient.Publish(context.TODO(), "device.msg.request", "test message")
	// 等待消息处理
	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, int32(2), atomic.LoadInt32(&count))
	atomic.StoreInt32(&count, 0)

	_ = ep.RemoveRouter("router1")

	redisClient.Publish(context.TODO(), "device.msg.request", "test message")
	// 等待消息处理
	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, int32(0), atomic.LoadInt32(&count))
	atomic.StoreInt32(&count, 0)

	_, _ = ep.AddRouter(router1)

	redisClient.Publish(context.TODO(), "device.msg.request", "test message")
	// 等待消息处理
	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, int32(2), atomic.LoadInt32(&count))
	atomic.StoreInt32(&count, 0)
}

// TestEndpoint is a placeholder to demonstrate basic endpoint functionality
func TestEndpoint(t *testing.T) {
	// clean redis
	client := redis.NewClient(&redis.Options{
		Addr: redisServer,
	})
	err := client.Ping(context.Background()).Err()
	if err != nil {
		t.Skip("redis not available, skipping test")
	}
	_ = client.FlushDB(context.Background()).Err()
	defer client.Close()

	config := types.NewConfig()
	ep := &Endpoint{}
	err = ep.Init(config, types.Configuration{
		"server": redisServer,
	})
	assert.Nil(t, err)
	assert.Equal(t, Type, ep.Type())

	router := endpoint.NewRouter().From("test_topic").End()
	routerId, err := ep.AddRouter(router)
	assert.Nil(t, err)
	assert.NotEqual(t, "", routerId)

	err = ep.RemoveRouter(routerId)
	assert.Nil(t, err)
	ep.Destroy()
}

// TestRedisEndpointLifecycle tests that the redis endpoint can start, receive a message,
// be destroyed, and not receive any more messages.
func TestRedisEndpointLifecycle(t *testing.T) {
	// clean redis
	client := redis.NewClient(&redis.Options{
		Addr: redisServer,
	})
	err := client.Ping(context.Background()).Err()
	if err != nil {
		t.Skip("redis not available, skipping test")
	}
	_ = client.FlushDB(context.Background()).Err()
	defer client.Close()

	config := types.NewConfig()

	ep := &Endpoint{}
	err = ep.Init(config, types.Configuration{
		"server": redisServer,
	})
	assert.Nil(t, err)

	msgChan := make(chan []byte, 1)

	router := endpoint.NewRouter().From("test_topic_lifecycle").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		msgChan <- exchange.In.Body()
		return true
	}).End()

	_, err = ep.AddRouter(router)
	assert.Nil(t, err)

	// Publish first message, should be received
	err = client.Publish(context.Background(), "test_topic_lifecycle", "msg1").Err()
	assert.Nil(t, err)

	select {
	case msg := <-msgChan:
		assert.Equal(t, "msg1", string(msg))
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message before destroy")
	}

	// Destroy the endpoint
	ep.Destroy()
	// Wait a bit for graceful shutdown
	time.Sleep(200 * time.Millisecond)

	// Publish second message, should NOT be received
	err = client.Publish(context.Background(), "test_topic_lifecycle", "msg2").Err()
	assert.Nil(t, err)

	select {
	case <-msgChan:
		t.Fatal("received message after endpoint was destroyed")
	case <-time.After(1 * time.Second):
		// No message received, as expected
	}
}

func TestEndpointConnectionStatus(t *testing.T) {
	endpoint := &Redis{}
	config := types.NewConfig()
	err := endpoint.Init(config, types.Configuration{
		"Server": redisServer,
	})
	assert.Nil(t, err)
	if _, err := endpoint.SharedNode.GetSafely(); err != nil {
		t.Skipf("redis server not available: %v", err)
	}
	assert.Equal(t, types.StatusConnected, endpoint.ConnectionStatus().Status)
	endpoint.Destroy()
	assert.Equal(t, types.StatusDisconnected, endpoint.ConnectionStatus().Status)
}

const electionTestChannel = "orders.created"

func (x *Redis) subscribedForTest() bool {
	x.RLock()
	defer x.RUnlock()
	return x.pubSub != nil
}

// newElectionEndpoint 构造一个参与选主的端点实例，count 累计其消费的消息数。
// 两个实例共享同一个 LocalLocker 模拟两副本共享锁后端。
func newElectionEndpoint(t *testing.T, locker types.Locker, srv *miniredis.Miniredis) (*Redis, *int32) {
	t.Helper()
	x := &Redis{}
	var count int32
	configuration := types.Configuration{
		"server": srv.Addr(),
		types.NodeConfigurationKeyRuleChainDefinition: &types.RuleChain{
			RuleChain: types.RuleChainBaseInfo{ID: "election-chain"},
		},
	}
	assert.Nil(t, x.Init(types.Config{Locker: locker}, configuration))
	// 用相同 scope 重建短租约守卫加速测试；生产走 Init 内建的默认 15s
	x.guard = types.NewActiveGuard(types.Config{Locker: locker},
		types.OnceScope(Type, "", "election-chain", x.instanceKey),
		types.WithActiveTTL(300*time.Millisecond), types.WithActiveInterval(100*time.Millisecond))
	router := endpoint.NewRouter().From(electionTestChannel).Process(func(rt endpointApi.Router, exchange *endpointApi.Exchange) bool {
		atomic.AddInt32(&count, 1)
		return true
	}).End()
	_, err := x.AddRouter(router)
	assert.Nil(t, err)
	assert.Nil(t, x.Start())
	return x, &count
}

// publishUntil 持续发布消息直到任一计数超过初始值。计数经线程池异步递增，
// 发布后必须等入账再重发，否则一条消息被计成多次。
func publishUntil(t *testing.T, srv *miniredis.Miniredis, want ...*int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		base := make([]int32, len(want))
		for i, c := range want {
			base[i] = atomic.LoadInt32(c)
		}
		srv.Publish(electionTestChannel, "payload")
		wait := time.Now().Add(2 * time.Second)
		for time.Now().Before(wait) {
			for i, c := range want {
				if atomic.LoadInt32(c) > base[i] {
					return
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	t.Fatal("no consumer received the message")
}

// TestRedisEndpointElection 两个副本只有主副本订阅消费；主副本停机后
// 待命副本接管，每条消息整个集群只触发一次规则链。
func TestRedisEndpointElection(t *testing.T) {
	srv := miniredis.RunT(t)
	locker := types.NewLocalLocker()

	a, countA := newElectionEndpoint(t, locker, srv)
	defer a.Close()
	b, countB := newElectionEndpoint(t, locker, srv)
	defer b.Close()

	// 选出唯一 leader：恰好一个订阅、一个不订阅
	deadline := time.Now().Add(5 * time.Second)
	for a.guard.IsActive() == b.guard.IsActive() {
		if time.Now().After(deadline) {
			t.Fatalf("no unique leader elected, active a=%v b=%v", a.guard.IsActive(), b.guard.IsActive())
		}
		time.Sleep(20 * time.Millisecond)
	}
	leader, countLeader := a, countA
	standby, countStandby := b, countB
	if b.guard.IsActive() {
		leader, countLeader = b, countB
		standby, countStandby = a, countA
	}

	deadline = time.Now().Add(3 * time.Second)
	for !leader.subscribedForTest() {
		if time.Now().After(deadline) {
			t.Fatal("leader did not subscribe")
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.False(t, standby.subscribedForTest())

	publishUntil(t, srv, countLeader, countStandby)
	assert.Equal(t, int32(1), atomic.LoadInt32(countLeader))
	assert.Equal(t, int32(0), atomic.LoadInt32(countStandby))

	// 主副本停机释放租约，待命副本接管并重新订阅
	leader.Close()
	deadline = time.Now().Add(5 * time.Second)
	for !standby.subscribedForTest() {
		if time.Now().After(deadline) {
			t.Fatal("new leader did not subscribe after leader shutdown")
		}
		time.Sleep(20 * time.Millisecond)
	}

	publishUntil(t, srv, countStandby)
	// 旧主消费第 1 条、新主消费第 2 条，各自恰好一条
	assert.Equal(t, int32(1), atomic.LoadInt32(countLeader))
	assert.Equal(t, int32(1), atomic.LoadInt32(countStandby))
}
