# transform
Transform messages.

## How to customize components
Implement the `types.Node` interface.
Example of a custom component:
```go
// Define the Node component
// UpperNode A plugin that converts the message data to uppercase
type UpperNode struct{}

func (n *UpperNode) Type() string {
    return "test/upper"
}

func (n *UpperNode) New() types.Node {
    return &UpperNode{}
}

func (n *UpperNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
    // Do some initialization work
    return nil
}

// Process the message
func (n *UpperNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
    msg.Data = strings.ToUpper(msg.Data)
    // Send the modified message to the next node
    ctx.TellSuccess(msg)
    return nil
}

func (n *UpperNode) Destroy() {
    // Do some cleanup work
}
```

## Usage

Register the component to the `RuleGo` default registry:
```go
rulego.Registry.Register(&MyNode{})
```

Then use your component in the rule chain DSL file:
```json
{
  "ruleChain": {
    "id": "rue01",
    "name": "Test rule chain
  },
  "metadata": {
    "nodes": [
      {
        "id": "s1",
        "type": "test/upper",
        "name": "Name",
        "debugMode": true,
        "configuration": {
          "field1": "Configuration parameters defined by the component",
          "....": "..."
        }
      }
    ],
    "connections": [
      {
        "fromId": "s1",
        "toId": "Connect to the next component ID",
        "type": "The connection relationship with the component"
      }
    ]
  }
}
```