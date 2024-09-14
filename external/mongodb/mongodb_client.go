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

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/json"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strconv"
)

const (
	QUERY  = "QUERY"
	INSERT = "INSERT"
	DELETE = "DELETE"
	UPDATE = "UPDATE"
)
const (
	MatchedCount  = "matchedCount"
	ModifiedCount = "modifiedCount"
	DeletedCount  = "deletedCount"
)

// 注册节点
func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

// ClientNodeConfiguration 节点配置
type ClientNodeConfiguration struct {
	Server         string
	DatabaseName   string
	CollectionName string
	GetOne         bool
	OpType         string
}

type ClientNode struct {
	base.SharedNode[*mongo.Client]
	// 节点配置
	Config ClientNodeConfiguration
	client *mongo.Client
}

// Type 返回组件类型
func (x *ClientNode) Type() string {
	return "mongodbClient"
}

func (x *ClientNode) New() types.Node {
	return &ClientNode{Config: ClientNodeConfiguration{
		Server:         "mongodb://localhost:27017",
		DatabaseName:   "",
		CollectionName: "",
		OpType:         INSERT,
	}}
}

// Init 初始化组件
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	if x.Config.Server == "" {
		return errors.New("server can not be empty")
	}
	if x.Config.DatabaseName == "" {
		return errors.New("databaseName can not be empty")
	}
	if x.Config.CollectionName == "" {
		return errors.New("collectionName can not be empty")
	}
	// 初始化客户端
	_ = x.SharedNode.Init(ruleConfig, x.Type(), x.Config.Server, true, func() (*mongo.Client, error) {
		return x.initClient()
	})
	return nil
}

// OnMsg 处理消息
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	if client, err := x.SharedNode.Get(); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		collection := client.Database(x.Config.DatabaseName).Collection(x.Config.CollectionName)
		x.processMessage(ctx, collection, msg, x.Config.OpType)
	}
}

// ProcessMessage 处理消息，执行查询、更新或删除操作
func (x *ClientNode) processMessage(ctx types.RuleContext, collection *mongo.Collection, msg types.RuleMsg, opType string) {
	// 解析 JSON 字符串
	var jsonMap map[string]interface{}
	err := json.Unmarshal([]byte(msg.Data), &jsonMap)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// 将解析后的 map 转换为 bson.M 类型
	doc := bson.M(jsonMap)
	// 根据操作类型执行不同的操作
	switch opType {
	case INSERT:
		// 插入文档
		_, err = collection.InsertOne(ctx.GetContext(), doc)
		ctx.TellSuccess(msg)
	case QUERY:
		if x.Config.GetOne {
			// 查询单个文档
			var result bson.M
			if err := collection.FindOne(ctx.GetContext(), doc).Decode(&result); err != nil {
				ctx.TellFailure(msg, err)
			} else {
				msg.Data = str.ToString(result)
				ctx.TellSuccess(msg)
			}
		} else {
			// 查询文档列表
			cursor, err := collection.Find(ctx.GetContext(), doc)
			if err != nil {
				ctx.TellFailure(msg, err)
				return
			}
			defer cursor.Close(ctx.GetContext())
			var results []bson.M
			if err = cursor.All(ctx.GetContext(), &results); err != nil {
				ctx.TellFailure(msg, err)
			} else {
				msg.Data = str.ToString(results)
				ctx.TellSuccess(msg)
			}
		}

	case UPDATE:
		// 更新文档
		if updateResult, err := collection.UpdateOne(ctx.GetContext(), doc, bson.M{"$set": doc}); err != nil {
			ctx.TellFailure(msg, err)
		} else {
			msg.Metadata.PutValue(MatchedCount, strconv.FormatInt(updateResult.MatchedCount, 10))
			msg.Metadata.PutValue(ModifiedCount, strconv.FormatInt(updateResult.ModifiedCount, 10))
			ctx.TellSuccess(msg)
		}

	case DELETE:
		// 删除文档
		if deleteResult, err := collection.DeleteOne(ctx.GetContext(), doc); err != nil {
			ctx.TellFailure(msg, err)
			return
		} else {
			msg.Metadata.PutValue(DeletedCount, strconv.FormatInt(deleteResult.DeletedCount, 10))
			ctx.TellSuccess(msg)
		}
	default:
		ctx.TellFailure(msg, fmt.Errorf("unsupported operation type: %s", opType))
	}
}

// Destroy 销毁组件
func (x *ClientNode) Destroy() {
	if x.client != nil {
		_ = x.client.Disconnect(context.TODO())
	}
}

// initClient 初始化客户端
func (x *ClientNode) initClient() (*mongo.Client, error) {
	if x.client != nil {
		return x.client, nil
	} else {
		x.Locker.Lock()
		defer x.Locker.Unlock()
		if x.client != nil {
			return x.client, nil
		}
		var err error
		client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(x.Config.Server))
		if err == nil {
			x.client = client
			// 测试连接
			err = x.client.Ping(context.TODO(), nil)
		}
		return x.client, err
	}
}
