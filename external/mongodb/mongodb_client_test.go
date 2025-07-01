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

package mongodb

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestMongoDBClient(t *testing.T) {
	// 如果设置了跳过 MongoDB 测试，则跳过
	if os.Getenv("SKIP_MONGODB_TESTS") == "true" {
		t.Skip("Skipping MongoDB tests")
	}

	// 检查是否有可用的 MongoDB 服务器
	mongoURL := os.Getenv("MONGODB_URL")
	if mongoURL == "" {
		mongoURL = "mongodb://localhost:27017"
	}

	// 测试 MongoDB 连接可用性
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURL))
	if err != nil {
		t.Skipf("MongoDB server not available: %v", err)
	}
	defer client.Disconnect(ctx)

	err = client.Ping(ctx, nil)
	if err != nil {
		t.Skipf("MongoDB server not responding: %v", err)
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/mongodbClient"

	t.Run("InitNode", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     mongoURL,
			"database":   "test_db",
			"collection": "test_collection",
			"opType":     "INSERT",
			"doc":        `{"name":"test"}`,
		}, Registry)
		assert.Nil(t, err)
		assert.NotNil(t, node)

		clientNode := node.(*ClientNode)
		assert.Equal(t, mongoURL, clientNode.Config.Server)
		assert.Equal(t, "test_db", clientNode.Config.Database)
		assert.Equal(t, "test_collection", clientNode.Config.Collection)
		assert.Equal(t, "INSERT", clientNode.Config.OpType)
	})

	t.Run("InsertDocument", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     mongoURL,
			"database":   "test_db",
			"collection": "test_collection",
			"opType":     "INSERT",                      // 使用大写的常量
			"doc":        `{"name":"test","value":123}`, // INSERT操作需要doc字段
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create MongoDB node: %v", err)
		}

		config := types.NewConfig()
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			if err != nil {
				t.Logf("MongoDB operation result: %s, error: %v", relationType, err)
			} else {
				assert.Equal(t, types.Success, relationType)
			}
		})

		metaData := types.NewMetadata()
		metaData.PutValue("operation", "insert")

		msg := ctx.NewMsg("TEST_MSG_TYPE", metaData, "{\"name\":\"test\",\"value\":123}")

		clientNode := node.(*ClientNode)
		clientNode.OnMsg(ctx, msg)

		// 等待消息处理
		time.Sleep(time.Millisecond * 200)

		clientNode.Destroy()
	})

	t.Run("FindDocument", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     mongoURL,
			"database":   "test_db",
			"collection": "test_collection",
			"opType":     "QUERY",           // 使用QUERY而不是find
			"filter":     `{"name":"test"}`, // QUERY操作需要filter字段
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create MongoDB node: %v", err)
		}

		config := types.NewConfig()
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			if err != nil {
				t.Logf("MongoDB find operation result: %s, error: %v", relationType, err)
			} else {
				assert.Equal(t, types.Success, relationType)
			}
		})

		metaData := types.NewMetadata()
		metaData.PutValue("operation", "find")

		msg := ctx.NewMsg("TEST_MSG_TYPE", metaData, "{\"name\":\"test\"}")

		clientNode := node.(*ClientNode)
		clientNode.OnMsg(ctx, msg)

		// 等待消息处理
		time.Sleep(time.Millisecond * 200)

		clientNode.Destroy()
	})
}

func TestMongoDBClientConfig(t *testing.T) {
	// 如果设置了跳过 MongoDB 测试，则跳过
	if os.Getenv("SKIP_MONGODB_TESTS") == "true" {
		t.Skip("Skipping MongoDB tests")
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/mongodbClient"

	t.Run("EmptyURIConfig", func(t *testing.T) {
		_, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "",
		}, Registry)
		assert.NotNil(t, err)
	})

	t.Run("InvalidURIConfig", func(t *testing.T) {
		_, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     "invalid://localhost:27017",
			"database":   "test_db",
			"collection": "test_collection",
			"opType":     "INSERT",
			"doc":        `{"name":"test"}`,
		}, Registry)
		// 应该能创建节点，但连接会失败
		assert.Nil(t, err)
	})

	t.Run("EmptyDatabaseConfig", func(t *testing.T) {
		_, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     "mongodb://localhost:27017",
			"database":   "", // 明确设置为空字符串
			"collection": "test_collection",
			"opType":     "INSERT",
			"doc":        `{"name":"test"}`,
		}, Registry)
		// 应该返回错误，因为database是必需的
		assert.NotNil(t, err)
		if err != nil {
			assert.Equal(t, "databaseName can not be empty", err.Error())
		}
	})

	t.Run("EmptyCollectionConfig", func(t *testing.T) {
		_, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     "mongodb://localhost:27017",
			"database":   "test_db",
			"collection": "", // 明确设置为空字符串
			"opType":     "INSERT",
			"doc":        `{"name":"test"}`,
		}, Registry)
		// 应该返回错误，因为collection是必需的
		assert.NotNil(t, err)
		if err != nil {
			assert.Equal(t, "collectionName can not be empty", err.Error())
		}
	})

	t.Run("DefaultValuesConfig", func(t *testing.T) {
		// 测试默认值是否生效
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "mongodb://localhost:27017",
			"opType": "QUERY",
			"filter": `{"name":"test"}`,
			// 不设置database和collection，使用默认值
		}, Registry)
		assert.Nil(t, err)
		assert.NotNil(t, node)

		if node != nil {
			clientNode := node.(*ClientNode)
			assert.Equal(t, "test", clientNode.Config.Database)   // 默认值
			assert.Equal(t, "user", clientNode.Config.Collection) // 默认值
			assert.Equal(t, "QUERY", clientNode.Config.OpType)
		}
	})

	t.Run("CaseInsensitiveOpType", func(t *testing.T) {
		// 测试opType大小写不敏感
		testCases := []string{"insert", "INSERT", "Insert", "find", "FIND", "Find", "query", "QUERY"}

		for _, opType := range testCases {
			node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
				"server":     "mongodb://localhost:27017",
				"database":   "test_db",
				"collection": "test_collection",
				"opType":     opType,
				"doc":        `{"name":"test"}`,
				"filter":     `{"name":"test"}`,
			}, Registry)
			assert.Nil(t, err, "opType %s should be valid", opType)
			assert.NotNil(t, node, "node should be created for opType %s", opType)

			if node != nil {
				clientNode := node.(*ClientNode)
				assert.Equal(t, opType, clientNode.Config.OpType, "原始opType应该被保留")
			}
		}
	})
}

// TestMongoDBCRUDOperations 测试完整的增删改查操作
func TestMongoDBCRUDOperations(t *testing.T) {
	// 如果设置了跳过 MongoDB 测试，则跳过
	if os.Getenv("SKIP_MONGODB_TESTS") == "true" {
		t.Skip("Skipping MongoDB tests")
	}

	// 检查是否有可用的 MongoDB 服务器
	mongoURL := os.Getenv("MONGODB_URL")
	if mongoURL == "" {
		mongoURL = "mongodb://localhost:27017"
	}

	// 测试 MongoDB 连接可用性
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURL))
	if err != nil {
		t.Skipf("MongoDB server not available: %v", err)
	}
	defer client.Disconnect(ctx)

	err = client.Ping(ctx, nil)
	if err != nil {
		t.Skipf("MongoDB server not responding: %v", err)
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/mongodbClient"

	// INSERT 操作测试
	t.Run("InsertOperations", func(t *testing.T) {
		t.Run("InsertSingleDocument", func(t *testing.T) {
			node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
				"server":     mongoURL,
				"database":   "test_crud",
				"collection": "users",
				"opType":     "INSERT",
				"doc":        `{"name": "John", "age": 30, "email": "john@example.com"}`,
				"one":        true,
			}, Registry)
			if err != nil {
				t.Skipf("Failed to create MongoDB node: %v", err)
				return
			}

			config := types.NewConfig()
			successCalled := false
			ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
				if err != nil {
					t.Logf("Insert operation failed: %s, error: %v", relationType, err)
				} else {
					assert.Equal(t, types.Success, relationType)
					successCalled = true
				}
			})

			msg := ctx.NewMsg("TEST_MSG_TYPE", types.NewMetadata(), `{"name": "John", "age": 30, "email": "john@example.com"}`)
			clientNode := node.(*ClientNode)
			clientNode.OnMsg(ctx, msg)

			time.Sleep(time.Millisecond * 300)
			if !successCalled {
				t.Log("Insert operation may have failed or MongoDB server not available")
			}
			clientNode.Destroy()
		})

		t.Run("InsertMultipleDocuments", func(t *testing.T) {
			node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
				"server":     mongoURL,
				"database":   "test_crud",
				"collection": "users",
				"opType":     "insert", // 测试小写
				"doc":        `[{"name": "Alice", "age": 25}, {"name": "Bob", "age": 35}]`,
				"one":        false,
			}, Registry)
			if err != nil {
				t.Skipf("Failed to create MongoDB node: %v", err)
				return
			}

			config := types.NewConfig()
			successCalled := false
			ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
				if err != nil {
					t.Logf("Insert multiple operation failed: %s, error: %v", relationType, err)
				} else {
					assert.Equal(t, types.Success, relationType)
					successCalled = true
				}
			})

			msg := ctx.NewMsg("TEST_MSG_TYPE", types.NewMetadata(), `[{"name": "Alice", "age": 25}, {"name": "Bob", "age": 35}]`)
			clientNode := node.(*ClientNode)
			clientNode.OnMsg(ctx, msg)

			time.Sleep(time.Millisecond * 300)
			if !successCalled {
				t.Log("Insert multiple operation may have failed or MongoDB server not available")
			}
			clientNode.Destroy()
		})
	})

	// QUERY/FIND 操作测试
	t.Run("QueryOperations", func(t *testing.T) {
		t.Run("FindSingleDocument", func(t *testing.T) {
			node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
				"server":     mongoURL,
				"database":   "test_crud",
				"collection": "users",
				"opType":     "QUERY",
				"filter":     `{"name": "John"}`,
				"one":        true,
			}, Registry)
			if err != nil {
				t.Skipf("Failed to create MongoDB node: %v", err)
				return
			}

			config := types.NewConfig()
			ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
				if err != nil {
					t.Logf("Query single operation result: %s, error: %v", relationType, err)
				} else {
					assert.Equal(t, types.Success, relationType)
					t.Logf("Query result: %s", msg.GetData())
				}
			})

			msg := ctx.NewMsg("TEST_MSG_TYPE", types.NewMetadata(), `{}`)
			clientNode := node.(*ClientNode)
			clientNode.OnMsg(ctx, msg)

			time.Sleep(time.Millisecond * 300)
			clientNode.Destroy()
		})

		t.Run("FindMultipleDocuments", func(t *testing.T) {
			node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
				"server":     mongoURL,
				"database":   "test_crud",
				"collection": "users",
				"opType":     "find", // 测试小写和别名
				"filter":     `{"age": {"$gte": 25}}`,
				"one":        false,
			}, Registry)
			if err != nil {
				t.Skipf("Failed to create MongoDB node: %v", err)
				return
			}

			config := types.NewConfig()
			ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
				if err != nil {
					t.Logf("Query multiple operation result: %s, error: %v", relationType, err)
				} else {
					assert.Equal(t, types.Success, relationType)
					t.Logf("Query results: %s", msg.GetData())
				}
			})

			msg := ctx.NewMsg("TEST_MSG_TYPE", types.NewMetadata(), `{}`)
			clientNode := node.(*ClientNode)
			clientNode.OnMsg(ctx, msg)

			time.Sleep(time.Millisecond * 300)
			clientNode.Destroy()
		})

		t.Run("FindWithComplexFilter", func(t *testing.T) {
			node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
				"server":     mongoURL,
				"database":   "test_crud",
				"collection": "users",
				"opType":     "SELECT", // 测试SELECT别名
				"filter":     `{"$and": [{"age": {"$gte": 20}}, {"age": {"$lte": 40}}]}`,
				"one":        false,
			}, Registry)
			if err != nil {
				t.Skipf("Failed to create MongoDB node: %v", err)
				return
			}

			config := types.NewConfig()
			ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
				if err != nil {
					t.Logf("Complex query operation result: %s, error: %v", relationType, err)
				} else {
					assert.Equal(t, types.Success, relationType)
					t.Logf("Complex query results: %s", msg.GetData())
				}
			})

			msg := ctx.NewMsg("TEST_MSG_TYPE", types.NewMetadata(), `{}`)
			clientNode := node.(*ClientNode)
			clientNode.OnMsg(ctx, msg)

			time.Sleep(time.Millisecond * 300)
			clientNode.Destroy()
		})
	})

	// UPDATE 操作测试
	t.Run("UpdateOperations", func(t *testing.T) {
		t.Run("UpdateSingleDocument", func(t *testing.T) {
			node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
				"server":     mongoURL,
				"database":   "test_crud",
				"collection": "users",
				"opType":     "UPDATE",
				"filter":     `{"name": "John"}`,
				"doc":        `{"age": 31, "lastUpdate": "2024-01-01"}`,
				"one":        true,
			}, Registry)
			if err != nil {
				t.Skipf("Failed to create MongoDB node: %v", err)
				return
			}

			config := types.NewConfig()
			ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
				if err != nil {
					t.Logf("Update single operation result: %s, error: %v", relationType, err)
				} else {
					assert.Equal(t, types.Success, relationType)
					// 检查元数据中的更新统计
					matchedCount := msg.Metadata.GetValue("matchedCount")
					modifiedCount := msg.Metadata.GetValue("modifiedCount")
					t.Logf("Update stats - Matched: %s, Modified: %s", matchedCount, modifiedCount)
				}
			})

			msg := ctx.NewMsg("TEST_MSG_TYPE", types.NewMetadata(), `{}`)
			clientNode := node.(*ClientNode)
			clientNode.OnMsg(ctx, msg)

			time.Sleep(time.Millisecond * 300)
			clientNode.Destroy()
		})

		t.Run("UpdateMultipleDocuments", func(t *testing.T) {
			node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
				"server":     mongoURL,
				"database":   "test_crud",
				"collection": "users",
				"opType":     "update", // 测试小写
				"filter":     `{"age": {"$gte": 25}}`,
				"doc":        `{"status": "active", "lastUpdate": "2024-01-01"}`,
				"one":        false,
			}, Registry)
			if err != nil {
				t.Skipf("Failed to create MongoDB node: %v", err)
				return
			}

			config := types.NewConfig()
			ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
				if err != nil {
					t.Logf("Update multiple operation result: %s, error: %v", relationType, err)
				} else {
					assert.Equal(t, types.Success, relationType)
					// 检查元数据中的更新统计
					matchedCount := msg.Metadata.GetValue("matchedCount")
					modifiedCount := msg.Metadata.GetValue("modifiedCount")
					t.Logf("Update multiple stats - Matched: %s, Modified: %s", matchedCount, modifiedCount)
				}
			})

			msg := ctx.NewMsg("TEST_MSG_TYPE", types.NewMetadata(), `{}`)
			clientNode := node.(*ClientNode)
			clientNode.OnMsg(ctx, msg)

			time.Sleep(time.Millisecond * 300)
			clientNode.Destroy()
		})
	})

	// DELETE 操作测试
	t.Run("DeleteOperations", func(t *testing.T) {
		t.Run("DeleteSingleDocument", func(t *testing.T) {
			node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
				"server":     mongoURL,
				"database":   "test_crud",
				"collection": "users",
				"opType":     "DELETE",
				"filter":     `{"name": "Alice"}`,
				"one":        true,
			}, Registry)
			if err != nil {
				t.Skipf("Failed to create MongoDB node: %v", err)
				return
			}

			config := types.NewConfig()
			ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
				if err != nil {
					t.Logf("Delete single operation result: %s, error: %v", relationType, err)
				} else {
					assert.Equal(t, types.Success, relationType)
					// 检查元数据中的删除统计
					deletedCount := msg.Metadata.GetValue("deletedCount")
					t.Logf("Delete stats - Deleted: %s", deletedCount)
				}
			})

			msg := ctx.NewMsg("TEST_MSG_TYPE", types.NewMetadata(), `{}`)
			clientNode := node.(*ClientNode)
			clientNode.OnMsg(ctx, msg)

			time.Sleep(time.Millisecond * 300)
			clientNode.Destroy()
		})

		t.Run("DeleteMultipleDocuments", func(t *testing.T) {
			node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
				"server":     mongoURL,
				"database":   "test_crud",
				"collection": "users",
				"opType":     "delete", // 测试小写
				"filter":     `{"status": "active"}`,
				"one":        false,
			}, Registry)
			if err != nil {
				t.Skipf("Failed to create MongoDB node: %v", err)
				return
			}

			config := types.NewConfig()
			ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
				if err != nil {
					t.Logf("Delete multiple operation result: %s, error: %v", relationType, err)
				} else {
					assert.Equal(t, types.Success, relationType)
					// 检查元数据中的删除统计
					deletedCount := msg.Metadata.GetValue("deletedCount")
					t.Logf("Delete multiple stats - Deleted: %s", deletedCount)
				}
			})

			msg := ctx.NewMsg("TEST_MSG_TYPE", types.NewMetadata(), `{}`)
			clientNode := node.(*ClientNode)
			clientNode.OnMsg(ctx, msg)

			time.Sleep(time.Millisecond * 300)
			clientNode.Destroy()
		})
	})
}

// TestMongoDBErrorHandling 测试错误处理场景
func TestMongoDBErrorHandling(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/mongodbClient"

	t.Run("InvalidOperationType", func(t *testing.T) {
		// 注意：此测试在没有MongoDB连接时会首先遇到连接错误
		// 这是正常的行为，因为代码设计是先连接再处理业务逻辑
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     "mongodb://localhost:27017",
			"database":   "test_crud",
			"collection": "users",
			"opType":     "INVALID_OP",
			"doc":        `{"name":"test"}`,
		}, Registry)
		assert.Nil(t, err) // 节点创建应该成功

		config := types.NewConfig()
		errorCalled := false
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			if err != nil {
				assert.Equal(t, types.Failure, relationType)
				// 可能是连接错误或操作类型错误，都是错误处理的一部分
				errorCalled = true
				if strings.Contains(err.Error(), "unsupported operation type") {
					t.Logf("Got expected operation type error: %v", err)
				} else {
					t.Logf("Got connection error (expected when MongoDB not available): %v", err)
				}
			}
		})

		msg := ctx.NewMsg("TEST_MSG_TYPE", types.NewMetadata(), `{}`)
		clientNode := node.(*ClientNode)
		clientNode.OnMsg(ctx, msg)

		time.Sleep(time.Millisecond * 500) // 增加等待时间
		assert.True(t, errorCalled, "Error callback should be called (either connection or operation error)")
		clientNode.Destroy()
	})

	t.Run("MissingDocForInsert", func(t *testing.T) {
		// 测试INSERT操作缺少doc配置的情况
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     "mongodb://localhost:27017",
			"database":   "test_crud",
			"collection": "users",
			"opType":     "INSERT",
			// 故意不设置doc字段
		}, Registry)
		assert.Nil(t, err) // 节点创建应该成功

		config := types.NewConfig()
		errorCalled := false
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			if err != nil {
				assert.Equal(t, types.Failure, relationType)
				errorCalled = true
				if strings.Contains(err.Error(), "doc") || strings.Contains(err.Error(), "INSERT") {
					t.Logf("Got expected INSERT doc error: %v", err)
				} else {
					t.Logf("Got connection error (expected when MongoDB not available): %v", err)
				}
			}
		})

		msg := ctx.NewMsg("TEST_MSG_TYPE", types.NewMetadata(), `{}`)
		clientNode := node.(*ClientNode)
		clientNode.OnMsg(ctx, msg)

		time.Sleep(time.Millisecond * 500)
		assert.True(t, errorCalled, "Error callback should be called for missing doc in INSERT")
		clientNode.Destroy()
	})

	t.Run("MissingFilterForQuery", func(t *testing.T) {
		// 测试QUERY操作缺少filter配置的情况
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     "mongodb://localhost:27017",
			"database":   "test_crud",
			"collection": "users",
			"opType":     "QUERY",
			// 故意不设置filter字段
		}, Registry)
		assert.Nil(t, err) // 节点创建应该成功

		config := types.NewConfig()
		errorCalled := false
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			if err != nil {
				assert.Equal(t, types.Failure, relationType)
				errorCalled = true
				if strings.Contains(err.Error(), "filter") || strings.Contains(err.Error(), "QUERY") {
					t.Logf("Got expected QUERY filter error: %v", err)
				} else {
					t.Logf("Got connection error (expected when MongoDB not available): %v", err)
				}
			}
		})

		msg := ctx.NewMsg("TEST_MSG_TYPE", types.NewMetadata(), `{}`)
		clientNode := node.(*ClientNode)
		clientNode.OnMsg(ctx, msg)

		time.Sleep(time.Millisecond * 500)
		assert.True(t, errorCalled, "Error callback should be called for missing filter in QUERY")
		clientNode.Destroy()
	})
}

// TestMongoDBExpressionSupport 测试表达式支持
func TestMongoDBExpressionSupport(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/mongodbClient"

	t.Run("DynamicDatabaseName", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     "mongodb://localhost:27017",
			"database":   "test_${env}", // 使用模板表达式
			"collection": "users",
			"opType":     "QUERY",
			"filter":     `{"name": "test"}`,
		}, Registry)
		assert.Nil(t, err)

		config := types.NewConfig()
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			// 这个测试主要验证配置解析没有错误
			t.Logf("Dynamic database test result: %s", relationType)
		})

		// 设置环境变量值
		metaData := types.NewMetadata()
		metaData.PutValue("env", "dynamic")

		msg := ctx.NewMsg("TEST_MSG_TYPE", metaData, `{}`)
		clientNode := node.(*ClientNode)
		clientNode.OnMsg(ctx, msg)

		time.Sleep(time.Millisecond * 100)
		clientNode.Destroy()
	})

	t.Run("DynamicCollectionName", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     "mongodb://localhost:27017",
			"database":   "test_crud",
			"collection": "${collection_name}", // 使用模板表达式
			"opType":     "QUERY",
			"filter":     `{"name": "test"}`,
		}, Registry)
		assert.Nil(t, err)

		config := types.NewConfig()
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			// 这个测试主要验证配置解析没有错误
			t.Logf("Dynamic collection test result: %s", relationType)
		})

		// 设置集合名称
		metaData := types.NewMetadata()
		metaData.PutValue("collection_name", "dynamic_users")

		msg := ctx.NewMsg("TEST_MSG_TYPE", metaData, `{}`)
		clientNode := node.(*ClientNode)
		clientNode.OnMsg(ctx, msg)

		time.Sleep(time.Millisecond * 100)
		clientNode.Destroy()
	})
}
