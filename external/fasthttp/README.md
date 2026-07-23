# FastHTTP API Call Node

High-performance HTTP client components based on FastHTTP, supporting proxy configuration and streaming responses. Standard `restApiCall` client components can be seamlessly replaced, providing more efficient HTTP request processing.

## Functional Features

- 🚀 **High-performance**: Based on FastHTTP libraries, outperforming standard HTTP libraries
- 🔗 **Proxy Support**: Supports HTTP/HTTPS and SOCKS5 proxies
- 🔐 **Authentication Support**: Supports proxy username and password authentication
- 🌐 **System Proxy**: Supports configuration using system proxies
- 📡 **Stream Response**: Supports Server-Sent Events (SSE) streaming data processing
- 🔒 **TLS Configuration**: Supports TLS certificate authentication configuration
- ⚡ **Connection Pool**: Supports connection pool configuration to improve concurrency performance
- 📝 **Template supports**: URL, Headers, Body supports template variable replacement

## Configure parameters

### Basic Configuration

| Parameter                         | Type                | Default value                                  | Note                         |
|----------------------------|-------------------|--------------------------------------|----------------------------|
| `restEndpointUrlPattern`   | string            | -                                    | HTTP URL address, supports template variable          |
| `requestMethod`            | string            | POST                                 | Request method (GET/POST/PUT/DELETE) |
| `withoutRequestBody`       | bool              | false                                | Whether to avoid sending the request body                   |
| `headers`                  | map[string]string | { "Content-Type": "application/json" } | Request header, supports template variable                 |
| `body`                     | string            | -                                    | Request body, supports template variable                 |
| `readTimeoutMs`            | int               | 2000                                 | Read timeout (milliseconds)                 |
| `insecureSkipVerify`       | bool              | false                                | Whether to skip TLS certificate validation              |
| `maxParallelRequestsCount` | int               | 200                                  | Maximum number of concurrent connections                    |

### Proxy configuration

| Parameter                         | Type     | Default value   | Note                       |
|----------------------------|--------|-------|--------------------------|
| `enableProxy`              | bool   | false | Whether to enable proxy                   |
| `useSystemProxyProperties` | bool   | false | Whether to use a system proxy to configure               |
| `proxyScheme`              | string | -     | Agency Agreement (http/https/socks5) |
| `proxyHost`                | string | -     | Proxy server address                  |
| `proxyPort`                | int    | -     | Proxy server port                  |
| `proxyUser`                | string | -     | Proxy username                    |
| `proxyPassword`            | string | -     | Proxy password                     |

## Usage Examples

### Fundamentals HTTP Request

```json
{
  "id": "fasthttp1",
  "type": "fasthttpApiCall",
  "name": "HTTP请求",
  "configuration": {
    "restEndpointUrlPattern": "https://api.example.com/data",
    "requestMethod": "POST",
    "headers": {
      "Content-Type": "application/json",
      "Authorization": "Bearer ${metadata.token}"
    },
    "body": "{\"name\":\"${msg.name}\",\"value\":\"${msg.value}\"}",
    "readTimeoutMs": 5000
  }
}
```

### HTTP Proxy configuration

```json
{
  "id": "fasthttp2",
  "type": "fasthttpApiCall",
  "name": "HTTP代理请求",
  "configuration": {
    "restEndpointUrlPattern": "https://api.example.com/data",
    "requestMethod": "GET",
    "enableProxy": true,
    "proxyScheme": "http",
    "proxyHost": "proxy.example.com",
    "proxyPort": 8080,
    "proxyUser": "username",
    "proxyPassword": "password"
  }
}
```

### SOCKS5 Proxy configuration

```json
{
  "id": "fasthttp3",
  "type": "fasthttpApiCall",
  "name": "SOCKS5代理请求",
  "configuration": {
    "restEndpointUrlPattern": "https://api.example.com/data",
    "requestMethod": "POST",
    "enableProxy": true,
    "proxyScheme": "socks5",
    "proxyHost": "127.0.0.1",
    "proxyPort": 1080
  }
}
```

### System Proxy Configuration

```json
{
  "id": "fasthttp4",
  "type": "fasthttpApiCall",
  "name": "系统代理请求",
  "configuration": {
    "restEndpointUrlPattern": "https://api.example.com/data",
    "requestMethod": "GET",
    "enableProxy": true,
    "useSystemProxyProperties": true
  }
}
```

### Server-Sent Events (SSE) Stream response

```json
{
  "id": "fasthttp5",
  "type": "fasthttpApiCall",
  "name": "SSE流式请求",
  "configuration": {
    "restEndpointUrlPattern": "https://api.example.com/stream",
    "requestMethod": "GET",
    "headers": {
      "Accept": "text/event-stream",
      "Cache-Control": "no-cache"
    }
  }
}
```

## Template variables

Template variables are supported in the following fields:

- `restEndpointUrlPattern`: URL address
- `headers`: The key and value of the request header
- `body`: Content of the request body

### Variable format

- `${metadata.key}`: Retrieves values from message metadata
- `${msg.key}`: Retrieves values from the message payload

### Example

```json
{
  "restEndpointUrlPattern": "https://api.example.com/users/${metadata.userId}/posts",
  "headers": {
    "Authorization": "Bearer ${metadata.token}",
    "X-Request-ID": "${metadata.requestId}"
  },
  "body": "{\"title\":\"${msg.title}\",\"content\":\"${msg.content}\"}"
}
```

## Response Handling

### Successful response

- HTTP When the status code is 200, the message is sent to chain `Success`
- Set the response content to message data
- Metadata includes:
  - `status`: HTTP State text
  - `statusCode`: HTTP status code

### Failure response

- HTTP When the status code is not 200, the message is sent to chain `Failure`
- Metadata includes:
  - `status`: HTTP State text
  - `statusCode`: HTTP status code
  - `errorBody`: Error response content

### SSE Stream response

For SSE stream responses, each event triggers a message processing:

- The metadata contains `eventType`: event type
- Message data is the event content

## Proxy Configuration Description

### System Proxy

When `useSystemProxyProperties` is enabled, components automatically read the following environment variables:

- `HTTP_PROXY` or `http_proxy`
- `HTTPS_PROXY` or `https_proxy`

### Agency protocol support

- **HTTP/HTTPS**: Use the HTTP CONNECT method to create a tunnel
- **SOCKS5**: Connect using the SOCKS5 protocol

### Proxy Authentication

Supports username and password authentication, suitable for HTTP and SOCKS5 proxies.

### TLS configuration

You can set `insecureSkipVerify: true` to skip certificate verification.

## Error Handling

Components handle the following types of errors:

1. **Network Connection Error**: Unable to connect to the target server
2. **Proxy connection error**: Unable to connect to the proxy server
3. **HTTP Error**: The server returns an error status code
4. **Timeout error**: Request timeout
5. **Template parsing error**: Template variable parsing failed

All errors send messages to the `Failure` chain and record the error information in the metadata.
