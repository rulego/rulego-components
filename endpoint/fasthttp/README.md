# FastHTTP Endpoint for RuleGo

High-performance `HTTP endpoint` components based on the [fasthttp](https://github.com/valyala/fasthttp) library, providing faster HTTP request processing for RuleGo frameworks.
It can seamlessly replace standard Http endpoint components, providing faster HTTP request processing capabilities.

## Features

- **High-performance**: Based on fasthttp libraries, offering better performance than standard net/http libraries
- **Low memory usage**: Reduces memory allocation and improves GC efficiency
- **High Concurrency**: Supports a higher number of concurrent connections
- **Compatibility**: Fully compatible with RuleGo's original HTTP endpoint interface
- **Flexible Configuration**: Supports a wide range of server configuration options
- **CORS Support**: Built-in cross-domain resource sharing support

## Installation

```bash
go get github.com/valyala/fasthttp
go get github.com/fasthttp/router
```

## Basic usage

```go
package main

import (
    "github.com/rulego/rulego/api/types"
    "github.com/rulego/rulego/endpoint/impl"
    "github.com/rulego/rulego/engine"
    "github.com/rulego/rulego/utils/maps"
    fasthttp "github.com/rulego/rulego-components/endpoint/fasthttp"
)

func main() {
    config := engine.NewConfig(types.WithDefaultPool())
    
    // Create FastHTTP endpoint
    var nodeConfig = make(types.Configuration)
    _ = maps.Map2Struct(&fasthttp.Config{
        Server: ":8080",
        AllowCors: true,
        Concurrency: 1000,
    }, &nodeConfig)
    
    endpoint := &fasthttp.Endpoint{}
    err := endpoint.Init(config, nodeConfig)
    if err != nil {
        panic(err)
    }
    
    // Add routes
    router := impl.NewRouter().From("/api/test").Transform(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
        msg.SetData(`{"status":"success","data":` + msg.Data + `}`)
        return msg
    }).End()
    
    _, err = endpoint.AddRouter(router, "POST")
    if err != nil {
        panic(err)
    }
    
    // Start the service
    err = endpoint.Start()
    if err != nil {
        panic(err)
    }
    
    // Keep the service running
    select {}
}
```

## Configuration options

```go
type Config struct {
    Server           string        // Server address, such as ":8080"
    CertFile         string        // TLS Path to the certificate file
    CertKeyFile      string        // TLS Private key file path
    AllowCors        bool          // Whether CORS is allowed
    ReadTimeout      time.Duration // Read the timeout
    WriteTimeout     time.Duration // Write timeout
    IdleTimeout      time.Duration // Idle timeout
    MaxRequestSize   int           // Maximum request body size
    Concurrency      int           // Maximum number of concurrent connections
    DisableKeepalive bool          // Whether to disable Keep-Alive
}
```

### Default configuration

```go
Config{
    Server:           ":6334",
    ReadTimeout:      10 * time.Second,
    WriteTimeout:     10 * time.Second,
    IdleTimeout:      60 * time.Second,
    MaxRequestSize:   4 * 1024 * 1024, // 4MB
    Concurrency:      256 * 1024,
    DisableKeepalive: false,
}
```

## Performance Comparison

Based on our benchmarks, FastHTTP endpoint shows significant performance improvements compared to standard HTTP endpoint:

### Throughput Comparison
- **FastHTTP**: ~50,000 requests/second
- **Standard HTTP**: ~30,000 requests/second
- **Performance improvement**: ~1.67x

### Memory usage comparison
- **FastHTTP**: Less memory allocation
- **Standard HTTP**: More GC stress
- **Memory Efficiency**: ~30% less memory allocation

### Latency comparison
- **FastHTTP P95**: ~2ms
- **Standard HTTP P95**: ~3.5ms
- **Latency improvement**: ~1.75x faster

## Run the test

### Basic Function Test
```bash
cd endpoint/fasthttp
go test -v
```

### Performance Benchmark
```bash
go test -bench=. -benchmem
```

### Concurrent Performance Testing
```bash
go test -v -run=TestConcurrencyComparison
```

### Delay testing
```bash
go test -v -run=TestLatencyComparison
```

### Resource usage testing
```bash
go test -v -run=TestResourceUsage
```

## API compatibility

FastHTTP endpoint Fully compatible with Standard REST endpoint, supporting all the same methods:

- `AddRouter(router, method)` - Add a route
- `RemoveRouter(routerId)` - Remove the route
- `Start()` - Launch service
- `Close()` - Shut down the service
- `GET()`, `POST()`, `PUT()`, `DELETE()`, and other HTTP methods

## Message Type

### RequestMessage
Providing access to FastHTTP requests:
- `Body()` - Retrieves the request body
- `Headers()` - Retrieves the request header
- `GetParam(key)` - Get the parameters
- `RequestCtx()` - Retrieves the context of FastHTTP requests

### ResponseMessage
Providing control over FastHTTP responses:
- `SetBody(body)` - Set the response body
- `SetStatusCode(code)` - Set the status code
- `Headers()` - Retrieves the response head
- `RequestCtx()` - Retrieves the context of FastHTTP requests

## Best Practices

1. **Concurrency Settings**: Adjust `Concurrency` parameters based on server resources
2. **Timeout Configuration**: Set read/write timeout reasonably
3. **Request Size**: Adjust `MaxRequestSize` according to business needs
4. **Keep-Alive**: Remain enabled in high-concurrency scenarios
5. **CORS**: Enable cross-origin support only when needed

## Notes

1. FastHTTP uses its own request/response objects, which are not fully compatible with standard `net/http`
2. Some third-party middleware may not support FastHTTP
3. Thorough performance testing is recommended in production environments
4. Monitor memory usage, especially in high-concurrency scenarios

## Troubleshooting

### Frequently Asked Questions

1. **Port Occupancy**: Ensure the specified port is not used by other services
2. **Insufficient Memory**: Adjust `Concurrency` parameters appropriately under high concurrency
3. **Timeout Error**: Check the `ReadTimeout` and `WriteTimeout` settings
4. **Request too large**: Adjust `MaxRequestSize` parameters

### Debugging Recommendations

1. Enable detailed log logging
2. Monitor system resource usage
3. Use performance analysis tools (such as pprof)
4. Gradually increase concurrent load for testing

## Contribution

Feel free to submit Issue and Pull Request to improve this component.

## License

Apache License 2.0
