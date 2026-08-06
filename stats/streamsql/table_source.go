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

// TableConfig describes a metadata table for stream-table JOIN (see each field for source/format/refresh).
// File paths are resolved statically at node load time, without a message context.
type TableConfig struct {
	Name    string                   `json:"name"`
	Source  string                   `json:"source"`  // inline | file | http, defaults to inline
	Path    string                   `json:"path"`    // file path or http URL
	Format  string                   `json:"format"`  // json | csv, defaults to json
	Rows    []map[string]interface{} `json:"rows"`    // data when source=inline
	Refresh string                   `json:"refresh"` // refresh interval, e.g. "30s"; empty = defaults to 1h for file/http, no refresh for inline

	// HTTP-only options
	Headers map[string]string `json:"headers"`
	Timeout string            `json:"timeout"` // HTTP timeout, defaults to 10s
}

const (
	defaultHTTPTimeout     = 10 * time.Second
	defaultRefreshInterval = "1h" // default refresh interval when a file/http table has no explicit Refresh
)

// loadTableRows loads and decodes metadata rows by Source. A failure during Init fails node initialization.
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

// decodeRows decodes the byte stream into a row set according to the format.
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

// decodeCSV parses CSV into a row set: the first row holds column names, the rest are data, all values are strings.
// An integer JOIN key on the stream side does not match the CSV string type; prefer json for integer keys.
func decodeCSV(b []byte) ([]map[string]interface{}, error) {
	reader := csv.NewReader(bytes.NewReader(b))
	// Tolerate inconsistent record lengths: real CSVs often have missing/extra columns, so take values by header as much as possible instead of erroring.
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

// loadHTTP issues a GET request to fetch the table data and returns the response body bytes.
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

// httpTimeout parses the HTTP timeout configuration, falling back to the default when invalid or empty.
func httpTimeout(s string) time.Duration {
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return d
	}
	return defaultHTTPTimeout
}

// tableManager manages the lifecycle of all metadata tables of a node: loading, registration, optional background refresh.
// Shared by streamTransform and streamAggregator to avoid duplicate implementations.
type tableManager struct {
	ssql *streamsql.Streamsql
	stop chan struct{}
	wg   sync.WaitGroup
	once sync.Once
}

// newTableManager creates a table manager bound to a StreamSQL instance (which must already be Executed).
func newTableManager(ssql *streamsql.Streamsql) *tableManager {
	return &tableManager{ssql: ssql, stop: make(chan struct{})}
}

// register loads and registers a table, and starts background refresh when Refresh is configured.
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

// maybeStartRefresh starts a background goroutine according to the refresh interval; on refresh failure the old snapshot is kept and retried in the next cycle.
// The interval is decided by refreshInterval (defaults to 1h for file/http, no refresh for inline, explicit Refresh takes precedence).
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

// refreshInterval parses the refresh interval: defaults to 1 hour for file/http; sources without an external origin (e.g. inline) default to no refresh;
// explicit Refresh takes precedence. Returning 0 means no refresh.
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

// refreshLoop periodically reloads and replaces the in-memory table via RegisterTable (overwriting by table name, so readers do not observe torn state).
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

// Close stops all refresh goroutines. Idempotent: safe on partial initialization failure or repeated Destroy calls.
func (m *tableManager) Close() {
	m.once.Do(func() {
		close(m.stop)
		m.wg.Wait()
	})
}
