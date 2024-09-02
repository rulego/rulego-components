package opengemini

import (
	"github.com/rulego/rulego/test/assert"
	"testing"
)

func TestParseLineProtocol(t *testing.T) {
	line := "cpu_usage,host=server01,region=us-west value=23.5 1434055562000000000"

	point, err := parseLineProtocol(line)
	if err != nil {
		t.Fatal(t)
	}
	assert.Equal(t, "server01", point.Tags["host"])
	assert.Equal(t, "us-west", point.Tags["region"])
	assert.Equal(t, 23.5, point.Fields["value"])
}

func TestParseMultiLineProtocol(t *testing.T) {
	lines := `
cpu_usage,host=server01,region=us-west value=23.5 1434055562000000000
cpu_usage,host=server02,region=us-east value=45.6 1434055562000000000
 `
	points, err := parseMultiLineProtocol(lines)
	if err != nil {
		t.Fatal(t)
	}
	assert.Equal(t, 2, len(points))
	assert.Equal(t, "cpu_usage", points[0].Measurement)
	assert.Equal(t, "server01", points[0].Tags["host"])
	assert.Equal(t, "us-west", points[0].Tags["region"])
	assert.Equal(t, 23.5, points[0].Fields["value"])

	assert.Equal(t, "cpu_usage", points[1].Measurement)
	assert.Equal(t, "server02", points[1].Tags["host"])
	assert.Equal(t, "us-east", points[1].Tags["region"])
	assert.Equal(t, 45.6, points[1].Fields["value"])

}
