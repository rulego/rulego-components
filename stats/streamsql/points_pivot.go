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
	"errors"
	"fmt"
)

// InputFormat config values.
const (
	// InputFormatAuto feeds each array element into the stream as one row (long format), the default behavior.
	InputFormatAuto = "auto"
	// InputFormatColumns pivots an IoT point array into a single wide row before feeding the stream.
	InputFormatColumns = "columns"
)

// Field keys of the IoT point array contract ([{name, value, timestamp, error}])
// and of the streamsql window metadata. rulego-components does not depend on the
// iot library, so the contract is recognized by duck typing on these keys.
const (
	pointNameKey      = "name"
	pointValueKey     = "value"
	pointErrorKey     = "error"
	pointTimestampKey = "timestamp"
	windowIDKey       = "window_id"
)

// ErrInvalidInputFormat reports that inputFormat accepts only empty, auto or columns.
var ErrInvalidInputFormat = errors.New("invalid inputFormat, expects empty/auto or columns")

// validateInputFormat validates the inputFormat config value.
func validateInputFormat(format string) error {
	switch format {
	case "", InputFormatAuto, InputFormatColumns:
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrInvalidInputFormat, format)
	}
}

// pivotPointArray pivots an IoT point array ([{name,value,timestamp,error}]) into a
// single wide row {pointName: value}.
// The iot_points.Data contract is recognized by duck typing (rulego-components does
// not depend on the iot library): it returns (row, true) when every element is a map
// carrying a non-empty string "name" key and a "value" key; elements with a non-empty
// "error" or an empty "name" are skipped (when all are skipped, row is an empty map);
// a non point-array returns (nil, false) so the caller falls back to per-element processing.
// The pivoted row carries a "timestamp" key (the max Timestamp of the valid points, ns)
// for event-time windowing WITH(TIMESTAMP='timestamp', TIMEUNIT='ns'); x/tsdbWrite treats
// it as a reserved key, extracting it as the point timestamp instead of a field. The key
// is omitted when every point Timestamp is 0/missing.
func pivotPointArray(data interface{}) (map[string]interface{}, bool) {
	var items []interface{}
	switch v := data.(type) {
	case []interface{}:
		items = v
	case []map[string]interface{}:
		items = make([]interface{}, len(v))
		for i, item := range v {
			items[i] = item
		}
	default:
		return nil, false
	}
	if len(items) == 0 {
		return nil, false
	}
	row := make(map[string]interface{}, len(items))
	var maxTs int64
	for _, item := range items {
		m, ok := item.(map[string]interface{})
		if !ok {
			return nil, false
		}
		nameVal, ok := m[pointNameKey]
		if !ok {
			return nil, false
		}
		name, ok := nameVal.(string)
		if !ok || name == "" {
			return nil, false
		}
		value, ok := m[pointValueKey]
		if !ok {
			return nil, false
		}
		if errVal, exists := m[pointErrorKey]; exists {
			if errStr, ok := errVal.(string); ok && errStr != "" {
				// Skip bad points.
				continue
			}
		}
		row[name] = value
		if ts := toInt64Ns(m[pointTimestampKey]); ts > maxTs {
			maxTs = ts
		}
	}
	if maxTs > 0 {
		row[pointTimestampKey] = maxTs
	}
	return row, true
}

// toInt64Ns extracts a nanosecond timestamp from a map value (JSON numbers arrive as float64).
// Returns 0 for non-numeric or missing values.
func toInt64Ns(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	}
	return 0
}
