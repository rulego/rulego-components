package opengemini

import (
	"fmt"
	"github.com/openGemini/opengemini-client-go/opengemini"
	"strconv"
	"strings"
	"time"
)

// parseLineProtocol 解析单行 Line Protocol 字符串并返回 Point 结构体
func parseLineProtocol(line string) (*opengemini.Point, error) {
	parts := strings.Split(line, " ")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid line format")
	}

	var p opengemini.Point
	p.Tags = make(map[string]string)
	p.Fields = make(map[string]interface{})
	p.Precision = opengemini.PrecisionNanosecond

	// 解析 measurement 和 tags
	measurementAndTags := strings.Split(parts[0], ",")
	//measurementEnd := strings.Index(measurementAndTags, " ")
	if len(parts) < 2 {
		return nil, fmt.Errorf("measurement name is missing")
	}
	p.Measurement = measurementAndTags[0]

	tags := measurementAndTags[1:]
	for _, tag := range tags {
		if tag == "" {
			continue
		}
		kv := strings.SplitN(tag, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid tag format: %s", tag)
		}
		p.Tags[kv[0]] = kv[1]
	}

	// 解析 fields
	fieldsStr := parts[1]
	for _, field := range strings.Split(fieldsStr, ",") {
		if field == "" {
			continue
		}
		kv := strings.SplitN(field, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid field format: %s", field)
		}
		value := kv[1]
		var fval interface{}
		parsedInt, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			fval = parsedInt
		} else {
			parsedFloat, err := strconv.ParseFloat(value, 64)
			if err == nil {
				fval = parsedFloat
			} else {
				// 如果既不是整数也不是浮点数，则保留原始字符串
				fval = value
			}
		}
		p.Fields[kv[0]] = fval
	}

	// 解析时间戳
	if len(parts) < 3 {
		p.Timestamp = time.Now().UnixNano()
	} else {
		timestamp, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp format: %s", parts[len(parts)-1])
		}
		p.Timestamp = timestamp
	}

	return &p, nil
}

// parseMultiLineProtocol 接受一个包含多行 Line Protocol 数据的字符串，并返回解析后的 Point 列表
func parseMultiLineProtocol(data string) ([]*opengemini.Point, error) {
	// 使用换行符分割字符串
	lines := strings.Split(data, "\n")
	var points []*opengemini.Point
	for _, line := range lines {
		line = strings.TrimSpace(line) // 移除行首尾的空白字符
		if line == "" {
			continue // 跳过空行
		}
		point, err := parseLineProtocol(line)
		if err != nil {
			return nil, fmt.Errorf("error parsing line: %s, error: %v", line, err)
		}
		points = append(points, point)
	}
	return points, nil
}
