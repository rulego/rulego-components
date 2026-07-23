# NSQ Endpoint (RuleGo endpoint)

NSQ endpoints based on [go-nsq](https://github.com/nsqio/go-nsq): consume messages from specified **topic** and drive the rule chain; When needed, **publish processing results** to another **topic via** metadata or response headers. Supports direct **nsqd** connections, **lookupd** address discovery, and **polling on multiple nsqd to publish** and failed attempts.

In your code, please use the **`Type`** constant (`types. EndpointTypePrefix + "nsq"`) exported from this package as the component type for `endpoint. Registry`. Do not handwrite strings to avoid inconsistency with prefixes in version RuleGo.

## Configuration Items

| Field | Note |
|------|------|
| `server` | **Required**. NSQ address, see below in "`server` Writing Method" |
| `channel` | Default channel; `AddRouter` Use |when no parameters are passed
| `authToken` | Authentication Secret consistent with nsqd (if authentication is enabled in the cluster) |
| `certFile` / `certKeyFile` | Reserved TLS certificate configuration (only added when consistent with current go-nsq consumer/producer usage) |

## `server` Writing Method

- **Single nsqd**: `127.0.0.1:4150` (Establish a link to this address within the process; This was the only time of publication)
- **Multiple nsqd** (separated by commas): `10.0.0.1:4150,10.0.0.2:4150`  
  **will create `Producer` for each** reachable address; **Consumption** `ConnectToNSQDs`; **Publish** **round-robin** within the process; if a single `Publish` fails, it will retry other nodes in the same call
- **lookupd** (requires `http://` or `https://`): `http://127.0.0.1:4161` or `http://a:4161,http://b:4161`  
  Request `/nodes` from each lookupd in order. After the first **of** successfully returns a non-null nsqd list, establish a link for the nsqd reachable in that list, with the same publishing behavior as above

## Usage Steps (Concept)

1. Register and **Init** this endpoint in the RuleGo, passing in `Config` (at least `Server`)
2. **AddRouter**: The string of `router.From` is the **NSQ topic** to subscribe to; The second parameter can cover **channel**
3. Rule chain handles inbound messages; To write the result back to NSQ, set **response topic** and body on the outbound side (see below)

## Reference Example 1: Registration and routing in Go

Consistent with the `endpoint/nsq/nsq_test.go` in the warehouse: first register the rule chain `rulego. New`, then `endpoint. Registry. New` create the endpoint (the third parameter is `nsq. Config`, which will internally complete `Init`).

```go
import (
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	endpointapi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	nsqend "github.com/rulego/rulego-components/endpoint/nsq"
)

func example(cfg types.Config) error {
	// The rule chain has been loaded, for example: _, _ = rulego.New("default", chainBytes, rulego.WithConfig(cfg))

	ep, err := endpoint.Registry.New(nsqend.Type, cfg, nsqend.Config{
		Server:  "127.0.0.1:4150", // Or more nsqd / lookupd, see above
		Channel: "default",
		// AuthToken: "your-secret",
	})
	if err != nil {
		return err
	}

	router := endpoint.NewRouter().From("device_msg_request"). // Subscribe to NSQ topic
		Process(func(r endpointapi.Router, ex *endpointapi.Exchange) bool {
			// Process inbound messages: exchange.In.GetMsg() …
			return true
		}).
		To("chain:default"). // Enter the rule chain
		Process(func(r endpointapi.Router, ex *endpointapi.Exchange) bool {
			// Post results to another topic (consistent with KeyResponseTopic)
			ex.Out.Headers().Add(nsqend.KeyResponseTopic, "device_msg_response")
			ex.Out.SetBody([]byte(`this is response`))
			return true
		}).End()

	// The second parameter is channel, overriding Config.Channel; You can omit it to use the channel in the configuration
	_, err = ep.AddRouter(router, "channel1")
	if err != nil {
		return err
	}
	return ep.Start()
}
```

> If injecting a configuration from map/JSON, usually the `types. Configuration` is constructed first, then `maps. Map2Struct` to the `nsq. Config` before registration; Refer to the usage of other endpoint in your project.

## Reference Example 2: Specify response topic from rule message metadata

`SetBody`, the target topic is drawn from the `responseTopic` **of the**metadata or the `responseTopic` **of the**Header, with the constant named `KeyResponseTopic` (`"responseTopic"`):

```go
.Process(func(r endpointapi.Router, ex *endpointapi.Exchange) bool {
	if ex.In.GetMsg() != nil {
		// Metadata can also be written in the preceding node of the rule chain
		ex.In.GetMsg().Metadata.PutValue(nsqend.KeyResponseTopic, "replies")
	}
	ex.Out.SetMsg(ex.In.GetMsg()) // If you need to use the same msg metadata
	ex.Out.SetBody([]byte("pong"))
	return true
})
```

## With RuleGo JSON / Low-Code

When configuring endpoints in visualization or JSON, bind `server` (and optional `channel`, `authToken`) to node type `rulego/endpoint/nsq` (based on your actual RuleGo endpoint type name); The **From** of the route corresponds to NSQ **topic**.

Example (**Structure Diagram**, field names based on the schema of endpoint in your project):

```json
{
  "id": "nsq_in",
  "type": "nsq",
  "config": {
    "server": "http://lookup-1:4161,http://lookup-2:4161",
    "channel": "default"
  }
}
```

## Behavior and Limitations (Simplified)

- Multi-**lookupd** does not merge unions of multiplex returns, only takes **first** non-empty discovery results; Multiple **nsqd** will try to connect all available instances during Init and then poll and publish.
- **When a new nsqd process joins the cluster after Init** it will not automatically enter the pool; generally, you need to restart the endpoint **** or perform a side-control de-plug.
- For more detailed semantics (with NSQ, cross-process balancing, etc.), see the next section, or read `roundRobinProducers` / `buildReachableProducers` / `discoverNsqdProducersFromLookupds` in the source code.

### Historical Issues and Key Points of Current Implementation (Operations and Maintenance Orientation)

- The discovery of multiple lookupd and multiple nsqd and **problems with only the first** have been resolved by sequential attempts and full Ping pool construction.
- During runtime, use polling + simultaneous retries within the **process** to reduce single-point release pressure; **Whether the global uniformity across** processes/multiple instances still depends on the results and number of instances found each time.

## Local Unit Testing

Some NSQ-related use cases in this repository will `Skip` when the **is not nsqd** started. When needed:

```bash
export NSQD_ADDRESS=127.0.0.1:4150
export LOOKUPD_ADDRESS=http://127.0.0.1:4161
go test -count=1 ./endpoint/nsq/...
# Skip tests that require a real NSQ: export SKIP_NSQ_TESTS=true
```

---

## import path with RuleGo module

In business warehouses, you should use the module path of this repository, for example:

```text
github.com/rulego/rulego-components/endpoint/nsq
```

If your fork or intranet coordinates differ, please replace it with the module path in the actual `go.mod`.
