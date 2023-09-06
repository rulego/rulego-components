module github.com/rulego/rulego-components/examples/redis

go 1.18

require (
	github.com/rulego/rulego v0.13.0
	github.com/rulego/rulego-components/redis v0.0.0-00010101000000-000000000000
)

require (
	github.com/dlclark/regexp2 v1.7.0 // indirect
	github.com/dop251/goja v0.0.0-20230605162241-28ee0ee714f3 // indirect
	github.com/eclipse/paho.mqtt.golang v1.4.2 // indirect
	github.com/go-redis/redis v6.15.9+incompatible // indirect
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/gofrs/uuid/v5 v5.0.0 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/pprof v0.0.0-20230207041349-798e818bf904 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/nxadm/tail v1.4.8 // indirect
	golang.org/x/net v0.12.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/text v0.11.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/rulego/rulego-components/redis => ./../../external/redis
