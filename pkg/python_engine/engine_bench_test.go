/*
 * Copyright 2026 The RuleGo Authors.
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

package pythonEngine

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
)

// benchPool creates a standard pool for benchmarking.
func benchPool(b *testing.B, script string) *ProcessPool {
	b.Helper()
	checkPythonBench(b)
	return NewStringProcessPool(types.NewConfig(), "Process", script, pythonPath(), 30*time.Second, 20, nil)
}

func checkPythonBench(b *testing.B) {
	b.Helper()
	if _, err := ResolvePythonPath(""); err != nil {
		b.Skipf("python not available, skipping benchmark: %v", err)
	}
}

// BenchmarkExecute_PassThrough measures the simplest script:
// just return msg, metadata, msgType.
// This isolates the process startup + IPC overhead.
func BenchmarkExecute_PassThrough(b *testing.B) {
	pool := benchPool(b, "return msg, metadata, msgType")
	defer pool.Shutdown()

	msg := `{"temperature":35,"humidity":60}`
	metadata := map[string]string{"device": "sensor01"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := pool.Execute(msg, metadata, "TEST", "JSON")
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkExecute_PassThrough_Parallel measures throughput under concurrency.
func BenchmarkExecute_PassThrough_Parallel(b *testing.B) {
	pool := benchPool(b, "return msg, metadata, msgType")
	defer pool.Shutdown()

	msg := `{"temperature":35,"humidity":60}`
	metadata := map[string]string{"device": "sensor01"}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := pool.Execute(msg, metadata, "TEST", "JSON")
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})
}

// BenchmarkExecute_JSONTransform measures a script that parses and modifies JSON.
// This represents a more realistic workload than pass-through.
func BenchmarkExecute_JSONTransform(b *testing.B) {
	script := `import json
data = json.loads(msg) if isinstance(msg, str) else msg
data["processed"] = True
data["timestamp"] = 1234567890
return json.dumps(data), metadata, msgType`
	pool := benchPool(b, script)
	defer pool.Shutdown()

	msg := `{"temperature":35,"humidity":60,"sensor":"th01","location":"building-A/floor-3"}`
	metadata := map[string]string{"device": "sensor01"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := pool.Execute(msg, metadata, "TEST", "JSON")
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkExecute_JSONTransform_Parallel measures JSON transform under concurrency.
func BenchmarkExecute_JSONTransform_Parallel(b *testing.B) {
	script := `import json
data = json.loads(msg) if isinstance(msg, str) else msg
data["processed"] = True
data["timestamp"] = 1234567890
return json.dumps(data), metadata, msgType`
	pool := benchPool(b, script)
	defer pool.Shutdown()

	msg := `{"temperature":35,"humidity":60,"sensor":"th01","location":"building-A/floor-3"}`
	metadata := map[string]string{"device": "sensor01"}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := pool.Execute(msg, metadata, "TEST", "JSON")
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})
}

// BenchmarkExecute_ComputeHeavy measures a script with CPU-bound work.
// This helps understand how much of the total latency is startup vs computation.
func BenchmarkExecute_ComputeHeavy(b *testing.B) {
	script := `import json, math
data = json.loads(msg) if isinstance(msg, str) else msg
result = sum(math.sin(i) * math.cos(i) for i in range(1000))
data["result"] = result
return json.dumps(data), metadata, msgType`
	pool := benchPool(b, script)
	defer pool.Shutdown()

	msg := `{"temperature":35}`
	metadata := map[string]string{"device": "sensor01"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := pool.Execute(msg, metadata, "TEST", "JSON")
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkExecute_LargePayload measures overhead with a large JSON message.
func BenchmarkExecute_LargePayload(b *testing.B) {
	pool := benchPool(b, "return msg, metadata, msgType")
	defer pool.Shutdown()

	// Build a ~10KB JSON payload
	largeMsg := `{"devices":[`
	for i := 0; i < 100; i++ {
		if i > 0 {
			largeMsg += ","
		}
		largeMsg += fmt.Sprintf(`{"id":"sensor-%d","temp":%d,"hum":%d,"status":"active","location":"zone-%d"}`, i, 20+i%10, 50+i%20, i%5)
	}
	largeMsg += `]}`
	metadata := map[string]string{"source": "gateway"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := pool.Execute(largeMsg, metadata, "TEST", "JSON")
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkThroughput_Concurrent measures aggregate throughput: N messages / wall time.
// This is a custom benchmark (not using b.N pattern) that gives ops/sec directly.
func BenchmarkThroughput_Concurrent(b *testing.B) {
	pool := benchPool(b, "return msg, metadata, msgType")
	defer pool.Shutdown()

	msg := `{"temperature":35}`
	metadata := map[string]string{"device": "sensor01"}

	for _, concurrency := range []int{1, 5, 10, 20} {
		b.Run(fmt.Sprintf("workers=%d", concurrency), func(b *testing.B) {
			var count atomic.Int64
			var wg sync.WaitGroup
			start := time.Now()

			b.ResetTimer()
			total := b.N
			ch := make(chan struct{}, total)
			for i := 0; i < total; i++ {
				ch <- struct{}{}
			}
			close(ch)

			for w := 0; w < concurrency; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range ch {
						_, err := pool.Execute(msg, metadata, "TEST", "JSON")
						if err != nil {
							b.Errorf("unexpected error: %v", err)
							return
						}
						count.Add(1)
					}
				}()
			}
			wg.Wait()

			elapsed := time.Since(start)
			b.ReportMetric(float64(count.Load())/elapsed.Seconds(), "ops/sec")
		})
	}
}

// BenchmarkStartupOverhead measures the raw cost of a single python3 subprocess
// invocation with the smallest possible script.
func BenchmarkStartupOverhead(b *testing.B) {
	checkPythonBench(b)
	pp := pythonPath()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool := NewStringProcessPool(types.NewConfig(), "Process", "return msg", pp, 10*time.Second, 1, nil)
		_, err := pool.Execute("x", nil, "T", "TEXT")
		pool.Shutdown()
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}
