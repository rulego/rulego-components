# action

执行某些动作。

## 怎样自定义组件
实现`types.Node`接口，例子：
```go
//定义Node组件
//UpperNode A plugin that converts the message data to uppercase
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
//处理消息
func (n *UpperNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) error {
msg.Data = strings.ToUpper(msg.Data)
// Send the modified message to the next node
ctx.TellSuccess(msg)
return nil
}

func (n *UpperNode) Destroy() {
// Do some cleanup work
}
```

## 使用

把组件注册到`RuleGo`默认注册器
```go
rulego.Registry.Register(&MyNode{})
```

然后在规则链DSL文件使用您的组件
```json
{
  "ruleChain": {
    "id": "rue01",
    "name": "测试规则链"
  },
  "metadata": {
    "nodes": [
      {
        "id": "s1",
        "type": "test/upper",
        "name": "名称",
        "debugMode": true,
        "configuration": {
          "field1": "组件定义的配置参数",
          "....": "..."
        }
      }
    ],
    "connections": [
      {
        "fromId": "s1",
        "toId": "连接下一个组件ID",
        "type": "与组件的连接关系"
      }
    ],
    "ruleChainConnections": null
  }
}
```