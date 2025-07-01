package kafka

import (
	"testing"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
)

// TestSharedNodeFeatures 测试SharedNode的特性
func TestSharedNodeFeatures(t *testing.T) {
	t.Run("OperationTracking", func(t *testing.T) {
		testOperationTracking(t)
	})

	t.Run("ShutdownStateManagement", func(t *testing.T) {
		testShutdownStateManagement(t)
	})

	t.Run("ResourcePoolVsLocalMode", func(t *testing.T) {
		testResourcePoolVsLocalMode(t)
	})
}

// testOperationTracking 测试操作跟踪功能
func testOperationTracking(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ProducerNode{})

	node, err := test.CreateAndInitNode("x/kafkaProducer", types.Configuration{
		"server": "localhost:9092",
		"topic":  "test.operation.tracking",
		"key":    "tracking-test",
	}, Registry)
	assert.Nil(t, err)

	producerNode := node.(*ProducerNode)

	// 手动测试操作计数
	assert.Equal(t, int64(0), producerNode.SharedNode.GetActiveOpsCount())

	producerNode.SharedNode.BeginOp()
	assert.Equal(t, int64(1), producerNode.SharedNode.GetActiveOpsCount())

	producerNode.SharedNode.BeginOp()
	assert.Equal(t, int64(2), producerNode.SharedNode.GetActiveOpsCount())

	producerNode.SharedNode.EndOp()
	assert.Equal(t, int64(1), producerNode.SharedNode.GetActiveOpsCount())

	producerNode.SharedNode.EndOp()
	assert.Equal(t, int64(0), producerNode.SharedNode.GetActiveOpsCount())

	producerNode.Destroy()
}

// testShutdownStateManagement 测试关闭状态管理
func testShutdownStateManagement(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ProducerNode{})

	node, err := test.CreateAndInitNode("x/kafkaProducer", types.Configuration{
		"server": "localhost:9092",
		"topic":  "test.shutdown.state",
		"key":    "state-test",
	}, Registry)
	assert.Nil(t, err)

	producerNode := node.(*ProducerNode)

	// 初始状态
	assert.False(t, producerNode.SharedNode.IsShuttingDown())

	// 开始关闭
	producerNode.SharedNode.BeginShutdown()
	assert.True(t, producerNode.SharedNode.IsShuttingDown())

	producerNode.Destroy()
}

// testResourcePoolVsLocalMode 测试资源池模式vs本地模式
func testResourcePoolVsLocalMode(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ProducerNode{})

	// 测试本地模式
	localNode, err := test.CreateAndInitNode("x/kafkaProducer", types.Configuration{
		"server": "localhost:9092",
		"topic":  "test.local.mode",
		"key":    "local-test",
	}, Registry)
	assert.Nil(t, err)
	localProducer := localNode.(*ProducerNode)
	assert.False(t, localProducer.SharedNode.IsFromPool())

	// 测试资源池模式（模拟）
	poolNode, err := test.CreateAndInitNode("x/kafkaProducer", types.Configuration{
		"server": "ref://test-pool-instance",
		"topic":  "test.pool.mode",
		"key":    "pool-test",
	}, Registry)
	// 资源池模式可能成功或失败，取决于实现
	if err != nil {
		t.Logf("Pool mode initialization failed as expected: %v", err)
	}
	poolProducer := poolNode.(*ProducerNode)
	assert.True(t, poolProducer.SharedNode.IsFromPool())

	// 清理
	localProducer.Destroy()
	poolProducer.Destroy()
}
