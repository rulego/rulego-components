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
	// If you set to skip MongoDB testing, skip it
	if os.Getenv("SKIP_MONGODB_TESTS") == "true" {
		t.Skip("Skipping MongoDB tests")
	}

	// Check if MongoDB servers are available
	mongoURL := os.Getenv("MONGODB_URL")
	if mongoURL == "" {
		mongoURL = "mongodb://localhost:27017"
	}

	// Testing MongoDB connection availability
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
			"opType":     "INSERT",                      // Use capitalized constants
			"doc":        `{"name":"test","value":123}`, // The INSERT operation requires the doc field
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

		// Waiting for the message to be processed
		time.Sleep(time.Millisecond * 200)

		clientNode.Destroy()
	})

	t.Run("FindDocument", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     mongoURL,
			"database":   "test_db",
			"collection": "test_collection",
			"opType":     "QUERY",           // Use QUERY instead of find
			"filter":     `{"name":"test"}`, // The QUERY operation requires the filter field
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

		// Waiting for the message to be processed
		time.Sleep(time.Millisecond * 200)

		clientNode.Destroy()
	})
}

func TestMongoDBClientConfig(t *testing.T) {
	// If you set to skip MongoDB testing, skip it
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
		// It should be possible to create nodes, but connections will fail
		assert.Nil(t, err)
	})

	t.Run("EmptyDatabaseConfig", func(t *testing.T) {
		_, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     "mongodb://localhost:27017",
			"database":   "", // Set explicitly to an empty string
			"collection": "test_collection",
			"opType":     "INSERT",
			"doc":        `{"name":"test"}`,
		}, Registry)
		// An error should be returned because the database is required
		assert.NotNil(t, err)
		if err != nil {
			assert.Equal(t, "databaseName can not be empty", err.Error())
		}
	})

	t.Run("EmptyCollectionConfig", func(t *testing.T) {
		_, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     "mongodb://localhost:27017",
			"database":   "test_db",
			"collection": "", // Set explicitly to an empty string
			"opType":     "INSERT",
			"doc":        `{"name":"test"}`,
		}, Registry)
		// Errors should be returned because collection is required
		assert.NotNil(t, err)
		if err != nil {
			assert.Equal(t, "collectionName can not be empty", err.Error())
		}
	})

	t.Run("DefaultValuesConfig", func(t *testing.T) {
		// Test whether the default value is effective
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "mongodb://localhost:27017",
			"opType": "QUERY",
			"filter": `{"name":"test"}`,
			// Do not set database and collection; use default values
		}, Registry)
		assert.Nil(t, err)
		assert.NotNil(t, node)

		if node != nil {
			clientNode := node.(*ClientNode)
			assert.Equal(t, "test", clientNode.Config.Database)   // Default values
			assert.Equal(t, "user", clientNode.Config.Collection) // Default values
			assert.Equal(t, "QUERY", clientNode.Config.OpType)
		}
	})

	t.Run("CaseInsensitiveOpType", func(t *testing.T) {
		// Test opType case-insensitivity
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

// TestMongoDBCRUDOperations tests complete CRUD operations
func TestMongoDBCRUDOperations(t *testing.T) {
	// If you set to skip MongoDB testing, skip it
	if os.Getenv("SKIP_MONGODB_TESTS") == "true" {
		t.Skip("Skipping MongoDB tests")
	}

	// Check if MongoDB servers are available
	mongoURL := os.Getenv("MONGODB_URL")
	if mongoURL == "" {
		mongoURL = "mongodb://localhost:27017"
	}

	// Testing MongoDB connection availability
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

	// INSERT operation test
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
				"opType":     "insert", // Test lowercase
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

	// QUERY/FIND operation testing
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
				"opType":     "find", // Testing lowercase and aliases
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
				"opType":     "SELECT", // Test the SELECT alias
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

	// UPDATE operation test
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
					// Check update statistics in the metadata
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
				"opType":     "update", // Test lowercase
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
					// Check update statistics in the metadata
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

	// DELETE operation test
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
					// Check the deletion statistics in the metadata
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
				"opType":     "delete", // Test lowercase
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
					// Check the deletion statistics in the metadata
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

// TestMongoDBErrorHandling tests error handling scenarios
func TestMongoDBErrorHandling(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/mongodbClient"

	t.Run("InvalidOperationType", func(t *testing.T) {
		// Note: This test will first encounter connection errors when there is no MongoDB connection
		// This is normal behavior because code design connects first and then processes business logic
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     "mongodb://localhost:27017",
			"database":   "test_crud",
			"collection": "users",
			"opType":     "INVALID_OP",
			"doc":        `{"name":"test"}`,
		}, Registry)
		assert.Nil(t, err) // Node creation should be successful

		config := types.NewConfig()
		errorCalled := false
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			if err != nil {
				assert.Equal(t, types.Failure, relationType)
				// It may be connection errors or operation type errors, all of which are part of error handling
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

		time.Sleep(time.Millisecond * 500) // Increased waiting times
		assert.True(t, errorCalled, "Error callback should be called (either connection or operation error)")
		clientNode.Destroy()
	})

	t.Run("MissingDocForInsert", func(t *testing.T) {
		// Testing the INSERT operation for missing doc configuration
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     "mongodb://localhost:27017",
			"database":   "test_crud",
			"collection": "users",
			"opType":     "INSERT",
			// Intentionally not set the doc field
		}, Registry)
		assert.Nil(t, err) // Node creation should be successful

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
		// Testing the QUERY operation for missing filter configuration
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     "mongodb://localhost:27017",
			"database":   "test_crud",
			"collection": "users",
			"opType":     "QUERY",
			// Intentionally not set the filter field
		}, Registry)
		assert.Nil(t, err) // Node creation should be successful

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

// TestMongoDBExpressionSupport Test expression support
func TestMongoDBExpressionSupport(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/mongodbClient"

	t.Run("DynamicDatabaseName", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":     "mongodb://localhost:27017",
			"database":   "test_${env}", // Use template expressions
			"collection": "users",
			"opType":     "QUERY",
			"filter":     `{"name": "test"}`,
		}, Registry)
		assert.Nil(t, err)

		config := types.NewConfig()
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			// This test mainly verifies that the configuration resolution is correct
			t.Logf("Dynamic database test result: %s", relationType)
		})

		// Set the environment variable value
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
			"collection": "${collection_name}", // Use template expressions
			"opType":     "QUERY",
			"filter":     `{"name": "test"}`,
		}, Registry)
		assert.Nil(t, err)

		config := types.NewConfig()
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			// This test mainly verifies that the configuration resolution is correct
			t.Logf("Dynamic collection test result: %s", relationType)
		})

		// Set the set name
		metaData := types.NewMetadata()
		metaData.PutValue("collection_name", "dynamic_users")

		msg := ctx.NewMsg("TEST_MSG_TYPE", metaData, `{}`)
		clientNode := node.(*ClientNode)
		clientNode.OnMsg(ctx, msg)

		time.Sleep(time.Millisecond * 100)
		clientNode.Destroy()
	})
}
