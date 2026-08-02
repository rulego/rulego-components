/*
 * Copyright 2025 The RuleGo Authors.
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

package streamsql

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/rulego/streamsql"
)

// TableConfig 描述一张流-表 JOIN 的元数据表（来源/格式/刷新见各字段）。
// file 路径在节点加载时静态解析，无消息上下文。
type TableConfig struct {
	Name    string                   `json:"name"`
	Source  string                   `json:"source"`  // inline | file | http，默认 inline
	Path    string                   `json:"path"`    // file 路径或 http URL
	Format  string                   `json:"format"`  // json | csv，默认 json
	Rows    []map[string]interface{} `json:"rows"`    // source=inline 时的数据
	Refresh string                   `json:"refresh"` // 刷新间隔，如 "30s"；空=file/http 默认 1h、inline 不刷新

	// HTTP 专用选项
	Headers map[string]string `json:"headers"`
	Timeout string            `json:"timeout"` // HTTP 超时，默认 10s
}

const (
	defaultHTTPTimeout     = 10 * time.Second
	defaultRefreshInterval = "1h" // file/http 表未显式配置 Refresh 时的默认刷新间隔
)

// loadTableRows 按 Source 加载并解码元数据行。Init 阶段失败会让节点初始化失败。
func loadTableRows(tbl TableConfig) ([]map[string]interface{}, error) {
	switch tbl.Source {
	case "", "inline":
		return tbl.Rows, nil
	case "file":
		b, err := os.ReadFile(tbl.Path)
		if err != nil {
			return nil, fmt.Errorf("read table file %s: %w", tbl.Path, err)
		}
		return decodeRows(b, tbl.Format)
	case "http":
		b, err := loadHTTP(tbl)
		if err != nil {
			return nil, fmt.Errorf("load table http %s: %w", tbl.Path, err)
		}
		return decodeRows(b, tbl.Format)
	default:
		return nil, fmt.Errorf("unknown table source %q", tbl.Source)
	}
}

// decodeRows 按格式解码字节流为行集合。
func decodeRows(b []byte, format string) ([]map[string]interface{}, error) {
	switch format {
	case "", "json":
		var rows []map[string]interface{}
		if err := json.Unmarshal(b, &rows); err != nil {
			return nil, fmt.Errorf("decode json: %w", err)
		}
		return rows, nil
	case "csv":
		return decodeCSV(b)
	default:
		return nil, fmt.Errorf("unsupported table format %q", format)
	}
}

// decodeCSV 将 CSV 解析为行集合：首行为列名，其余为数据，值均为字符串。
// 整型流侧 JOIN key 与 CSV 字符串类型不一致会匹配失败，整型键建议用 json。
func decodeCSV(b []byte) ([]map[string]interface{}, error) {
	reader := csv.NewReader(bytes.NewReader(b))
	// 容忍行长不一致：实际 CSV 常有缺列/多列，按表头尽可能取值而非报错。
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("decode csv: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("decode csv: empty")
	}
	header := records[0]
	rows := make([]map[string]interface{}, 0, len(records)-1)
	for _, rec := range records[1:] {
		row := make(map[string]interface{}, len(header))
		for i, col := range header {
			if i < len(rec) {
				row[col] = rec[i]
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// loadHTTP 发起 GET 请求拉取表数据，返回响应体字节。
func loadHTTP(tbl TableConfig) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, tbl.Path, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range tbl.Headers {
		req.Header.Set(k, v)
	}
	client := &http.Client{Timeout: httpTimeout(tbl.Timeout)}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// httpTimeout 解析 HTTP 超时配置，非法或为空时回退默认值。
func httpTimeout(s string) time.Duration {
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return d
	}
	return defaultHTTPTimeout
}

// tableManager 管理一个节点全部元数据表的生命周期：加载、注册、可选后台刷新。
// streamTransform 与 streamAggregator 共用，避免重复实现。
type tableManager struct {
	ssql *streamsql.Streamsql
	stop chan struct{}
	wg   sync.WaitGroup
	once sync.Once
}

// newTableManager 创建表管理器，绑定一个 StreamSQL 实例（须已 Execute）。
func newTableManager(ssql *streamsql.Streamsql) *tableManager {
	return &tableManager{ssql: ssql, stop: make(chan struct{})}
}

// register 加载并注册一张表，并在配置了 Refresh 时启动后台刷新。
func (m *tableManager) register(tbl TableConfig) error {
	if tbl.Name == "" {
		return fmt.Errorf("join table config requires name")
	}
	rows, err := loadTableRows(tbl)
	if err != nil {
		return fmt.Errorf("load table %s: %w", tbl.Name, err)
	}
	if _, err := m.ssql.RegisterTable(tbl.Name, rows); err != nil {
		return fmt.Errorf("register table %s: %w", tbl.Name, err)
	}
	return m.maybeStartRefresh(tbl)
}

// maybeStartRefresh 按刷新间隔启动后台 goroutine；刷新失败保留旧快照，下周期重试。
// 间隔由 refreshInterval 决定（file/http 默认 1h，inline 默认不刷新，显式 Refresh 优先）。
func (m *tableManager) maybeStartRefresh(tbl TableConfig) error {
	interval, err := refreshInterval(tbl)
	if err != nil {
		return err
	}
	if interval == 0 {
		return nil
	}
	m.wg.Add(1)
	go m.refreshLoop(tbl, interval)
	return nil
}

// refreshInterval 解析刷新间隔：file/http 默认 1 小时；inline 等无外部源默认不刷新；
// 显式 Refresh 优先。返回 0 表示不刷新。
func refreshInterval(tbl TableConfig) (time.Duration, error) {
	spec := tbl.Refresh
	if spec == "" {
		if tbl.Source == "file" || tbl.Source == "http" {
			spec = defaultRefreshInterval
		} else {
			return 0, nil
		}
	}
	d, err := time.ParseDuration(spec)
	if err != nil {
		return 0, fmt.Errorf("invalid refresh %q: %w", tbl.Refresh, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("refresh must be positive: %q", tbl.Refresh)
	}
	return d, nil
}

// refreshLoop 定时重新加载并通过 RegisterTable 替换内存表（按表名覆盖，读侧不撕裂）。
func (m *tableManager) refreshLoop(tbl TableConfig, interval time.Duration) {
	defer m.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
			rows, err := loadTableRows(tbl)
			if err != nil {
				continue
			}
			if _, err := m.ssql.RegisterTable(tbl.Name, rows); err != nil {
				continue
			}
		}
	}
}

// Close 停止所有刷新 goroutine。幂等：部分初始化失败或 Destroy 重复调用都安全。
func (m *tableManager) Close() {
	m.once.Do(func() {
		close(m.stop)
		m.wg.Wait()
	})
}
