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
	"strconv"
	"strings"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	SELECT = "SELECT"
	QUERY  = "QUERY"
	FIND   = "FIND"
	INSERT = "INSERT"
	DELETE = "DELETE"
	UPDATE = "UPDATE"
)
const (
	MatchedCount  = "matchedCount"
	ModifiedCount = "modifiedCount"
	DeletedCount  = "deletedCount"
	KeyId         = "_id"
)

// Register the node
func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

// ClientNodeConfiguration node configuration
// ClientNodeConfiguration MongoDB client node configuration structure
type ClientNodeConfiguration struct {
	// Server MongoDB server connection address
	Server string `json:"server" label:"Server" desc:"MongoDB connection string, e.g. mongodb://user:pass@localhost:27017" required:"true" ref:"primary"`
	// Database name
	Database string `json:"database" label:"Database" desc:"Database name. Supports ${metadata.key} and ${msg.key} substitution" required:"true"`
	// Collection name
	Collection string `json:"collection" label:"Collection" desc:"Collection name. Supports ${metadata.key} and ${msg.key} substitution" required:"true"`
	// OpType operation type
	OpType string `json:"opType" label:"Op Type" desc:"Operation type: INSERT, UPDATE, DELETE, QUERY" required:"true"`
	// Filter: Filter conditions
	Filter string `json:"filter" label:"Filter" desc:"MongoDB filter query. Supports ${metadata.key} and ${msg.key} substitution"`
	// Doc: Updated or inserted document content
	Doc string `json:"doc" label:"Document" desc:"Document for insert/update. Supports ${metadata.key} and ${msg.key} substitution"`
	// One: Whether to operate only single data entries
	One bool `json:"one" label:"One" desc:"true=operate single document, false=operate multiple"`
}

// ClientNode mongodb client component, which can perform add, delete, modify, and query operations on mongodb
type ClientNode struct {
	base.SharedNode[*mongo.Client]
	// Node configuration
	Config ClientNodeConfiguration
	// DatabaseNameTemplate: A database name template used to parse dynamic database names
	// DatabaseNameTemplate template for resolving dynamic database names
	DatabaseNameTemplate el.Template
	// CollectionNameTemplate: A collection name template used to resolve dynamic collection names
	// CollectionNameTemplate template for resolving dynamic collection names
	CollectionNameTemplate el.Template
	// Filter
	FilterTemplate *el.ExprTemplate
	// Documentation
	DocTemplate *el.ExprTemplate
	// hasVar identifies whether the template contains variables
	// hasVar indicates whether the template contains variables
	hasVar bool
}

// Type returns the component type
func (x *ClientNode) Type() string {
	return "x/mongodbClient"
}

func (x *ClientNode) New() types.Node {
	return &ClientNode{Config: ClientNodeConfiguration{
		Server:     "mongodb://localhost:27017",
		Database:   "test",
		Collection: "user",
		OpType:     QUERY,
		Filter:     `{"age": {"$gte": 18}}`,
	}}
}

// Init initializes the component
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	if x.Config.Server == "" {
		return errors.New("server can not be empty")
	}
	if x.Config.Database == "" {
		return errors.New("databaseName can not be empty")
	} else {
		if template, err := el.NewTemplate(strings.TrimSpace(x.Config.Database)); err != nil {
			return err
		} else {
			x.DatabaseNameTemplate = template
			if template.HasVar() {
				x.hasVar = true
			}
		}
	}
	if x.Config.Collection == "" {
		return errors.New("collectionName can not be empty")
	} else {
		if template, err := el.NewTemplate(strings.TrimSpace(x.Config.Collection)); err != nil {
			return err
		} else {
			x.CollectionNameTemplate = template
			if template.HasVar() {
				x.hasVar = true
			}
		}
	}
	if x.Config.OpType == "" {
		return errors.New("opType can not be empty")
	}
	if x.Config.Filter != "" {
		if template, err := el.NewExprTemplate(strings.TrimSpace(x.Config.Filter)); err != nil {
			return err
		} else {
			x.FilterTemplate = template
			if template.HasVar() {
				x.hasVar = true
			}
		}
	}
	if x.Config.Doc != "" {
		if template, err := el.NewExprTemplate(strings.TrimSpace(x.Config.Doc)); err != nil {
			return err
		} else {
			x.DocTemplate = template
			if template.HasVar() {
				x.hasVar = true
			}
		}
	}
	// Initialize the client
	_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*mongo.Client, error) {
		return x.initClient()
	}, func(client *mongo.Client) error {
		// Cleanup callback function
		return client.Disconnect(context.TODO())
	})
	return nil
}

// OnMsg processes a message
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	if client, err := x.SharedNode.GetSafely(); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)

		databaseName := x.DatabaseNameTemplate.ExecuteAsString(evn)
		collectionName := x.CollectionNameTemplate.ExecuteAsString(evn)
		collection := client.Database(databaseName).Collection(collectionName)
		x.processMessage(ctx, evn, collection, msg, x.Config.OpType)
	}
}

// ProcessMessage handles messages and performs queries, updates, or deletion operations
func (x *ClientNode) processMessage(ctx types.RuleContext, evn map[string]interface{}, collection *mongo.Collection, msg types.RuleMsg, opType string) {
	// Convert to uppercase to support case-insensitivity
	opTypeUpper := strings.ToUpper(opType)

	// Different operations are performed depending on the type of operation
	switch opTypeUpper {
	case INSERT:
		x.insert(ctx, evn, collection, msg)
	case QUERY, SELECT, FIND:
		x.query(ctx, evn, collection, msg)
	case UPDATE:
		x.update(ctx, evn, collection, msg)
	case DELETE:
		x.delete(ctx, evn, collection, msg)
	default:
		ctx.TellFailure(msg, fmt.Errorf("unsupported operation type: %s", opType))
	}
}

func (x *ClientNode) toBsonM(evn map[string]interface{}, template *el.ExprTemplate) (interface{}, error) {
	if out, err := template.Execute(evn); err != nil {
		return nil, err
	} else {
		if r, ok := out.(map[string]interface{}); ok {
			x.tryConvertId(r)
			return r, nil
		} else if s, ok := out.(string); ok {
			var r bson.M
			if err := bson.UnmarshalExtJSON([]byte(s), true, &r); err != nil {
				return nil, err
			}
			x.tryConvertId(r)
			return r, nil
		} else {
			return nil, errors.New("expr result is not map[string]interface{} or json string")
		}
	}
}

// tryConvertId attempts to convert _id to ObjectID
func (x *ClientNode) tryConvertId(m map[string]interface{}) {
	if id, ok := m[KeyId]; ok {
		if idStr, ok := id.(string); ok {
			if oid, err := primitive.ObjectIDFromHex(idStr); err == nil {
				m[KeyId] = oid
			}
		} else if idMap, ok := id.(map[string]interface{}); ok {
			// Handling nested maps, for example {"$in": ["id1", "id2"]}
			x.convertNestedId(idMap)
		} else if idMap, ok := id.(bson.M); ok {
			x.convertNestedId(map[string]interface{}(idMap))
		}
	}
}

// convertNestedId recursively handles nested _id query conditions
func (x *ClientNode) convertNestedId(m map[string]interface{}) {
	for k, v := range m {
		// Handles array type values, such as $in, $nin
		if k == "$in" || k == "$nin" {
			if vList, ok := v.([]interface{}); ok {
				for i, item := range vList {
					if itemStr, ok := item.(string); ok {
						if oid, err := primitive.ObjectIDFromHex(itemStr); err == nil {
							vList[i] = oid
						}
					}
				}
			} else if vList, ok := v.(bson.A); ok {
				for i, item := range vList {
					if itemStr, ok := item.(string); ok {
						if oid, err := primitive.ObjectIDFromHex(itemStr); err == nil {
							vList[i] = oid
						}
					}
				}
			}
		} else if k == "$eq" || k == "$ne" || k == "$gt" || k == "$gte" || k == "$lt" || k == "$lte" {
			// Handles single value types, such as $eq, $ne
			if itemStr, ok := v.(string); ok {
				if oid, err := primitive.ObjectIDFromHex(itemStr); err == nil {
					m[k] = oid
				}
			}
		}
	}
}

func (x *ClientNode) toBsonMList(evn map[string]interface{}, template *el.ExprTemplate) ([]interface{}, error) {
	if out, err := template.Execute(evn); err != nil {
		return nil, err
	} else {
		return x.processBsonList(out)
	}
}

func (x *ClientNode) processBsonList(out interface{}) ([]interface{}, error) {
	if r, ok := out.([]interface{}); ok {
		for _, item := range r {
			if m, ok := item.(map[string]interface{}); ok {
				x.tryConvertId(m)
			} else if m, ok := item.(bson.M); ok {
				x.tryConvertId(m)
			}
		}
		return r, nil
	} else if r, ok := out.(map[string]interface{}); ok {
		x.tryConvertId(r)
		return []interface{}{r}, nil
	} else if r, ok := out.(bson.M); ok {
		x.tryConvertId(r)
		return []interface{}{r}, nil
	} else if r, ok := out.([]map[string]interface{}); ok {
		var interfaceList []interface{}
		for _, item := range r {
			x.tryConvertId(item)
			interfaceList = append(interfaceList, item)
		}
		return interfaceList, nil
	} else if s, ok := out.(string); ok {
		// Try parsing it as a JSON array
		var r []bson.M
		if err := bson.UnmarshalExtJSON([]byte(s), true, &r); err == nil {
			var interfaceList []interface{}
			for _, item := range r {
				x.tryConvertId(item)
				interfaceList = append(interfaceList, item)
			}
			return interfaceList, nil
		}
		// Try parsing it as a single JSON object
		var single bson.M
		if err := bson.UnmarshalExtJSON([]byte(s), true, &single); err == nil {
			x.tryConvertId(single)
			return []interface{}{single}, nil
		}
		return nil, errors.New("expr result is not valid json")
	} else {
		return nil, errors.New("expr result is not []map[string]interface{} or []interface{}")
	}
}

func (x *ClientNode) insert(ctx types.RuleContext, evn map[string]interface{}, collection *mongo.Collection, msg types.RuleMsg) {
	// Check whether the DocTemplate is empty
	if x.DocTemplate == nil {
		ctx.TellFailure(msg, errors.New("doc template is required for INSERT operation"))
		return
	}

	if x.Config.One {
		if doc, err := x.toBsonM(evn, x.DocTemplate); err != nil {
			ctx.TellFailure(msg, err)
		} else {
			// Insert document
			_, err = collection.InsertOne(ctx.GetContext(), doc)
			if err != nil {
				ctx.TellFailure(msg, err)
			} else {
				ctx.TellSuccess(msg)
			}
		}
	} else {
		if docs, err := x.toBsonMList(evn, x.DocTemplate); err != nil {
			ctx.TellFailure(msg, err)
		} else {
			// Insert multiple documents
			_, err = collection.InsertMany(ctx.GetContext(), docs)
			if err != nil {
				ctx.TellFailure(msg, err)
			} else {
				ctx.TellSuccess(msg)
			}
		}
	}
}

func (x *ClientNode) query(ctx types.RuleContext, evn map[string]interface{}, collection *mongo.Collection, msg types.RuleMsg) {
	// Check if FilterTemplate is empty
	if x.FilterTemplate == nil {
		ctx.TellFailure(msg, errors.New("filter template is required for QUERY operation"))
		return
	}

	if filter, err := x.toBsonM(evn, x.FilterTemplate); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		if x.Config.One {
			// Query individual documents
			var result bson.M
			if err := collection.FindOne(ctx.GetContext(), filter).Decode(&result); err != nil {
				ctx.TellFailure(msg, err)
			} else {
				msg.SetData(str.ToString(result))
				ctx.TellSuccess(msg)
			}
		} else {
			// Query the list of documents
			cursor, err := collection.Find(ctx.GetContext(), filter)
			if err != nil {
				ctx.TellFailure(msg, err)
				return
			}
			defer cursor.Close(ctx.GetContext())
			var results []bson.M
			if err = cursor.All(ctx.GetContext(), &results); err != nil {
				ctx.TellFailure(msg, err)
			} else {
				msg.SetData(str.ToString(results))
				ctx.TellSuccess(msg)
			}
		}
	}
}
func (x *ClientNode) update(ctx types.RuleContext, evn map[string]interface{}, collection *mongo.Collection, msg types.RuleMsg) {
	// Check whether DocTemplate and FilterTemplate are empty
	if x.DocTemplate == nil {
		ctx.TellFailure(msg, errors.New("doc template is required for UPDATE operation"))
		return
	}
	if x.FilterTemplate == nil {
		ctx.TellFailure(msg, errors.New("filter template is required for UPDATE operation"))
		return
	}

	var err error
	var doc interface{}
	var filter interface{}

	if doc, err = x.toBsonM(evn, x.DocTemplate); err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	if filter, err = x.toBsonM(evn, x.FilterTemplate); err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	if x.Config.One {
		// Update individual documents
		if updateResult, err := collection.UpdateOne(ctx.GetContext(), filter, bson.M{"$set": doc}); err != nil {
			ctx.TellFailure(msg, err)
		} else {
			msg.Metadata.PutValue(MatchedCount, strconv.FormatInt(updateResult.MatchedCount, 10))
			msg.Metadata.PutValue(ModifiedCount, strconv.FormatInt(updateResult.ModifiedCount, 10))
			ctx.TellSuccess(msg)
		}
	} else {
		if updateResult, err := collection.UpdateMany(ctx.GetContext(), filter, bson.M{"$set": doc}); err != nil {
			ctx.TellFailure(msg, err)
		} else {
			msg.Metadata.PutValue(MatchedCount, strconv.FormatInt(updateResult.MatchedCount, 10))
			msg.Metadata.PutValue(ModifiedCount, strconv.FormatInt(updateResult.ModifiedCount, 10))
			ctx.TellSuccess(msg)
		}
	}

}
func (x *ClientNode) delete(ctx types.RuleContext, evn map[string]interface{}, collection *mongo.Collection, msg types.RuleMsg) {
	// Check if FilterTemplate is empty
	if x.FilterTemplate == nil {
		ctx.TellFailure(msg, errors.New("filter template is required for DELETE operation"))
		return
	}

	if filter, err := x.toBsonM(evn, x.FilterTemplate); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		if x.Config.One {
			// Delete the document
			if deleteResult, err := collection.DeleteOne(ctx.GetContext(), filter); err != nil {
				ctx.TellFailure(msg, err)
				return
			} else {
				msg.Metadata.PutValue(DeletedCount, strconv.FormatInt(deleteResult.DeletedCount, 10))
				ctx.TellSuccess(msg)
			}
		} else {
			if deleteResult, err := collection.DeleteMany(ctx.GetContext(), filter); err != nil {
				ctx.TellFailure(msg, err)
				return
			} else {
				msg.Metadata.PutValue(DeletedCount, strconv.FormatInt(deleteResult.DeletedCount, 10))
				ctx.TellSuccess(msg)
			}
		}
	}
}

func (x *ClientNode) Destroy() {
	_ = x.SharedNode.Close()
}

// Desc returns the component description
func (x *ClientNode) Desc() string {
	return "MongoDB client for CRUD operations. OpType: INSERT, UPDATE, DELETE, QUERY. All fields support ${metadata.key} and ${msg.key} substitution. Routes to Success/Failure"
}

// initClient initializes the client
func (x *ClientNode) initClient() (*mongo.Client, error) {
	var err error
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(x.Config.Server))
	if err == nil {
		// Test the connection
		err = client.Ping(context.TODO(), nil)
	}
	return client, err
}
