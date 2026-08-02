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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rulego/rulego/test/assert"
	"github.com/rulego/streamsql"
)

// writeTempFile 在临时目录写入内容并返回路径。
func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return p
}

func TestLoadTableRows_Inline(t *testing.T) {
	rows := []map[string]interface{}{
		{"deviceId": "d1", "location": "plantA"},
		{"deviceId": "d2", "location": "plantB"},
	}
	// Source 留空与显式 inline 行为一致。
	for _, src := range []string{"", "inline"} {
		got, err := loadTableRows(TableConfig{Source: src, Rows: rows})
		assert.Nil(t, err, "inline 加载不应失败")
		assert.Equal(t, 2, len(got), "应返回 2 行")
		assert.Equal(t, "plantA", got[0]["location"], "首行 location 正确")
	}
}

func TestLoadTableRows_FileJSON(t *testing.T) {
	path := writeTempFile(t, "meta.json",
		`[{"deviceId":"d1","location":"plantA"},{"deviceId":"d2","location":"plantB"}]`)
	got, err := loadTableRows(TableConfig{Source: "file", Path: path, Format: "json"})
	assert.Nil(t, err, "file json 加载不应失败")
	assert.Equal(t, 2, len(got), "应解析出 2 行")
	assert.Equal(t, "d2", got[1]["deviceId"], "第二行 deviceId 正确")
}

func TestLoadTableRows_FileCSV(t *testing.T) {
	path := writeTempFile(t, "meta.csv", "deviceId,location,type\nd1,plantA,temp\nd2,plantB,humid\n")
	got, err := loadTableRows(TableConfig{Source: "file", Path: path, Format: "csv"})
	assert.Nil(t, err, "file csv 加载不应失败")
	assert.Equal(t, 2, len(got), "应解析出 2 行（去掉表头）")
	assert.Equal(t, "plantA", got[0]["location"], "首行按表头取值")
	assert.Equal(t, "humid", got[1]["type"], "第二行 type 正确")
}

func TestLoadTableRows_FileCSVShortRow(t *testing.T) {
	// 行字段少于表头时，缺失列不写入，不越界。
	path := writeTempFile(t, "meta.csv", "deviceId,location,type\nd1,plantA\n")
	got, err := loadTableRows(TableConfig{Source: "file", Path: path, Format: "csv"})
	assert.Nil(t, err, "短行 csv 应容忍")
	assert.Equal(t, 1, len(got), "应解析出 1 行")
	assert.Equal(t, "plantA", got[0]["location"], "存在的列正常取值")
	_, hasType := got[0]["type"]
	assert.False(t, hasType, "缺失列不应出现")
}

func TestLoadTableRows_HTTP(t *testing.T) {
	body := `[{"deviceId":"d1","location":"plantA"}]`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Token") != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Write([]byte(body))
	}))
	defer srv.Close()

	t.Run("带 header 成功", func(t *testing.T) {
		got, err := loadTableRows(TableConfig{
			Source: "http", Path: srv.URL, Format: "json",
			Headers: map[string]string{"X-Token": "secret"},
		})
		assert.Nil(t, err, "带正确 header 应成功")
		assert.Equal(t, 1, len(got), "应解析出 1 行")
		assert.Equal(t, "plantA", got[0]["location"], "location 正确")
	})

	t.Run("缺 header 报错", func(t *testing.T) {
		_, err := loadTableRows(TableConfig{Source: "http", Path: srv.URL, Format: "json"})
		assert.NotNil(t, err, "缺 header 服务端返回 401，应报错")
	})
}

func TestLoadTableRows_HTTPErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	_, err := loadTableRows(TableConfig{Source: "http", Path: srv.URL, Format: "json"})
	assert.NotNil(t, err, "5xx 应报错")
}

func TestLoadTableRows_UnknownSource(t *testing.T) {
	_, err := loadTableRows(TableConfig{Source: "ftp", Path: "x"})
	assert.NotNil(t, err, "未知 source 应报错")
}

func TestLoadTableRows_FileMissing(t *testing.T) {
	_, err := loadTableRows(TableConfig{Source: "file", Path: filepath.Join(t.TempDir(), "nope.json")})
	assert.NotNil(t, err, "文件不存在应报错")
}

func TestLoadTableRows_BadJSON(t *testing.T) {
	path := writeTempFile(t, "bad.json", "{not json")
	_, err := loadTableRows(TableConfig{Source: "file", Path: path, Format: "json"})
	assert.NotNil(t, err, "非法 json 应报错")
}

func TestLoadTableRows_UnsupportedFormat(t *testing.T) {
	path := writeTempFile(t, "meta.xml", "<a/>")
	_, err := loadTableRows(TableConfig{Source: "file", Path: path, Format: "xml"})
	assert.NotNil(t, err, "不支持的格式应报错")
}

func TestLoadTableRows_CSVErrors(t *testing.T) {
	t.Run("空文件", func(t *testing.T) {
		path := writeTempFile(t, "empty.csv", "")
		_, err := loadTableRows(TableConfig{Source: "file", Path: path, Format: "csv"})
		assert.NotNil(t, err, "空 csv 应报错")
	})
}

func TestHTTPTimeout(t *testing.T) {
	assert.Equal(t, defaultHTTPTimeout, httpTimeout(""), "空字符串回退默认")
	assert.Equal(t, defaultHTTPTimeout, httpTimeout("abc"), "非法值回退默认")
	assert.Equal(t, 5*time.Second, httpTimeout("5s"), "合法值按解析")
}

// TestLoadTableRows_RoundtripJSON 校验加载结果可被 JSON 回环，确保返回的是
// []map[string]interface{} 标准结构，供 RegisterTable 直接消费。
func TestLoadTableRows_RoundtripJSON(t *testing.T) {
	path := writeTempFile(t, "meta.json", `[{"deviceId":"d1","v":1}]`)
	got, err := loadTableRows(TableConfig{Source: "file", Path: path})
	assert.Nil(t, err, "加载不应失败")
	b, err := json.Marshal(got)
	assert.Nil(t, err, "应可序列化")
	assert.True(t, len(b) > 0, "序列化结果非空")
}

// tableManager 的生命周期与错误路径单测。这些路径此前只被集成测试间接覆盖。

// newJoinStreamsql 构造一个已 Execute JOIN 查询的实例，供 tableManager 注册表。
func newJoinStreamsql(t *testing.T) *streamsql.Streamsql {
	t.Helper()
	ssql := streamsql.New()
	if err := ssql.Execute("SELECT m.x FROM stream JOIN meta m ON id = m.id"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return ssql
}

// TestTableManager_CloseIdempotent 验证 Close 幂等（sync.Once），重复调用不 panic。
func TestTableManager_CloseIdempotent(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	m := newTableManager(ssql)
	m.Close()
	m.Close()
}

// TestTableManager_RegisterRequiresName 验证空表名注册直接报错。
func TestTableManager_RegisterRequiresName(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	m := newTableManager(ssql)
	err := m.register(TableConfig{Source: "inline"})
	assert.NotNil(t, err, "空表名应报错")
}

// TestTableManager_BadRefreshDuration 验证非法刷新间隔报错。
func TestTableManager_BadRefreshDuration(t *testing.T) {
	ssql := newJoinStreamsql(t)
	defer ssql.Stop()
	m := newTableManager(ssql)
	err := m.register(TableConfig{
		Name: "meta", Source: "inline",
		Rows: []map[string]interface{}{{"id": 1, "x": "a"}}, Refresh: "not-a-duration",
	})
	assert.NotNil(t, err, "非法刷新间隔应报错")
	m.Close()
}

// TestTableManager_PartialInitCleanup 验证部分注册失败时 Close 能停掉已启动的刷新
// goroutine（不泄漏、不挂起）。
func TestTableManager_PartialInitCleanup(t *testing.T) {
	ssql := newJoinStreamsql(t)
	defer ssql.Stop()
	m := newTableManager(ssql)

	// 第一张表带刷新：注册成功并启动刷新 goroutine。
	assert.Nil(t, m.register(TableConfig{
		Name: "meta", Source: "inline",
		Rows: []map[string]interface{}{{"id": 1, "x": "a"}}, Refresh: "100ms",
	}))
	// 第二张表加载失败 → 返回错误。
	err := m.register(TableConfig{Name: "nope", Source: "file", Path: "/nonexistent/x.json"})
	assert.NotNil(t, err, "不存在的文件应报错")

	// Close 应 promptly 返回（刷新 goroutine 已停）。
	done := make(chan struct{})
	go func() { m.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close 超时：刷新 goroutine 未停止")
	}
}

// TestRefreshInterval 验证刷新间隔默认值与解析。
func TestRefreshInterval(t *testing.T) {
	// file/http 未配置 Refresh 默认 1 小时。
	d, err := refreshInterval(TableConfig{Source: "file"})
	assert.Nil(t, err)
	assert.Equal(t, time.Hour, d)
	d, err = refreshInterval(TableConfig{Source: "http"})
	assert.Nil(t, err)
	assert.Equal(t, time.Hour, d)

	// inline 等无外部源默认不刷新。
	d, err = refreshInterval(TableConfig{Source: "inline"})
	assert.Nil(t, err)
	assert.Equal(t, time.Duration(0), d)

	// 显式 Refresh 优先于默认。
	d, err = refreshInterval(TableConfig{Source: "file", Refresh: "30s"})
	assert.Nil(t, err)
	assert.Equal(t, 30*time.Second, d)

	// 非法/非正间隔报错。
	_, err = refreshInterval(TableConfig{Source: "file", Refresh: "bad"})
	assert.NotNil(t, err)
	_, err = refreshInterval(TableConfig{Source: "file", Refresh: "-1s"})
	assert.NotNil(t, err)
}
