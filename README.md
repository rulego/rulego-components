# rulego-components

`rulego-components` is a rule engine extension component library for [RuleGo](https://github.com/rulego/rulego).

## Features
The component library is divided into the following submodules:
* **endpoint:** Receiver endpoint, responsible for listening and receiving data, and then handing it over to the `RuleGo` rule engine for processing. For example: MQTT endpoint (subscribe to MQTT Broker data), HTTP endpoint (HTTP server)
* **filter:** Filter messages.
* **transform:** Transform messages.
* **action:** Perform some actions.
* **stats:** Perform statistics and analysis on data.
* **external:** External integration, integrate with third-party systems, such as: calling kafka, database, third-party api, etc.

## Installation

Use the `go get` command to install `RuleGo`:

```bash
go get github.com/rulego/rulego
```

Use the `go get` command to install `rulego-components`:

```bash
go get github.com/rulego/rulego-components/{submodel}
```


## Usage

Register the component to the `RuleGo` default registry
```go
rulego.Registry.Register(&MyNode{})
```

Then use your component in the rule chain DSL file
```json
{
  "ruleChain": {
    "name": "Test rule chain",
    "root": true,
    "debugMode": false
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
    ],
    "ruleChainConnections": null
  }
}
```

## Contributing

`RuleGo` aims to create an open and rich ecosystem, and welcomes contributions of extension components. If the component you submit has no third-party dependencies or is a general-purpose component, please submit it to [github.com/rulego/rulego/components](https://github.com/rulego/rulego) under the built-in `components`,
otherwise submit it to this repository.

We also welcome any form of contribution, including submitting issues, suggestions, documentation, tests or code. Please follow these steps:

* Clone the project repository to your local machine
* Create a new branch and make your changes
* Submit a merge request to the main branch
* Wait for review and feedback

## License

`RuleGo` is licensed under the Apache 2.0 License - see the [LICENSE] file for details.
