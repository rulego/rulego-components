package redis

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

// Register the node
func init() {
	_ = rulego.Registry.Register(&PublisherNode{})
}

// KeyResult: Number of subscribers receiving messages
const KeyResult = "result"

// PublisherNodeConfiguration node configuration
type PublisherNodeConfiguration struct {
	// Server redis server address
	Server string `json:"server" label:"Server" desc:"Redis server address, e.g. 127.0.0.1:6379" required:"true" ref:"primary"`
	// Password
	Password string `json:"password" label:"Password" desc:"Redis password" ref:"shared"`
	// PoolSize: Connect the pool size
	PoolSize int `json:"poolSize" label:"Pool Size" desc:"Connection pool size"`
	// db database index
	Db int `json:"db" label:"DB" desc:"Redis database index"`
	// Channel: Release channel
	Channel string `json:"channel" label:"Channel" desc:"Pub/sub channel. Supports ${metadata.key} substitution" required:"true"`
}

// PublisherNode redis is the publisher node
// Success: Switch to the Success chain and use msg.metadata.result to get the number of subscribers who received messages
// Failure: Switch to the Failure chain
type PublisherNode struct {
	base.SharedNode[*redis.Client]
	//Node configuration
	Config          PublisherNodeConfiguration
	channelTemplate el.Template
	// hasVar identifies whether the template contains variables
	hasVar bool
}

// Type returns the component type
func (x *PublisherNode) Type() string {
	return "x/redisPub"
}

func (x *PublisherNode) New() types.Node {
	return &PublisherNode{Config: PublisherNodeConfiguration{
		Server:  "127.0.0.1:6379",
		Channel: "default",
		Db:      0,
	}}
}

// Init initializes the component
func (x *PublisherNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		//Initialize the client
		_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*redis.Client, error) {
			return x.initClient()
		}, func(client *redis.Client) error {
			// Cleanup callback function
			return client.Close()
		})
		x.channelTemplate, err = el.NewTemplate(strings.TrimSpace(x.Config.Channel))
		if err != nil {
			return err
		}
		// Check if the template contains variables
		x.hasVar = x.channelTemplate.HasVar()
	}
	return err
}

// OnMsg processes a message
func (x *PublisherNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	var channel string
	if x.hasVar {
		evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
		channel = x.channelTemplate.ExecuteAsString(evn)
	} else {
		channel = x.Config.Channel
	}
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// Posted the message to Redis
	result, err := client.Publish(ctx.GetContext(), channel, msg.GetData()).Result()
	if err != nil {
		ctx.TellFailure(msg, err)
	} else {
		msg.Metadata.PutValue(KeyResult, str.ToString(result))
		ctx.TellSuccess(msg)
	}
}

func (x *PublisherNode) Destroy() {
	_ = x.SharedNode.Close()
}

// Desc returns the component description
func (x *PublisherNode) Desc() string {
	return "Redis pub/sub publisher. Channel supports ${metadata.key} substitution. Subscriber count stored in metadata.result. Routes to Success/Failure"
}

func (x *PublisherNode) initClient() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     x.Config.Server,
		PoolSize: x.Config.PoolSize,
		DB:       x.Config.Db,
		Password: x.Config.Password,
	})
	err := client.Ping(context.Background()).Err()
	return client, err
}
