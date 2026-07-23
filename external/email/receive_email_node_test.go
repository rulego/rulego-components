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

package email

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
)

// ============================================================
// Node lifecycle testing
// ============================================================

func TestReceiveEmailNodeNew(t *testing.T) {
	var node ReceiveEmailNode
	newNode := node.New()
	assert.NotNil(t, newNode)
	assert.Equal(t, "x/receiveEmail", newNode.Type())
}

func TestReceiveEmailNodeDefaultConfig(t *testing.T) {
	var node ReceiveEmailNode
	newNode := node.New()
	err := newNode.Init(types.NewConfig(), types.Configuration{})
	assert.Nil(t, err)

	n := newNode.(*ReceiveEmailNode)
	assert.Equal(t, 993, n.Config.Port)
	assert.Equal(t, true, n.Config.EnableTLS)
	assert.Equal(t, 10, n.Config.ConnectTimeout)
	assert.Equal(t, "INBOX", n.Config.Search.Folder)
	assert.Equal(t, "full", n.Config.Fetch.ContentType)
	assert.Equal(t, "none", n.Config.PostAction.Action)
}

func TestReceiveEmailNodeInit(t *testing.T) {
	var node ReceiveEmailNode
	configuration := types.Configuration{
		"server":         "imap.example.com",
		"port":           993,
		"username":       "user@example.com",
		"password":       "pass123",
		"enableTls":      true,
		"connectTimeout": 5,
		"search": types.Configuration{
			"folder":   "INBOX",
			"lastDays": 7,
			"unread":   false,
			"limit":    10,
		},
		"fetch": types.Configuration{
			"contentType":         "full",
			"includeAttachments":  true,
			"maxAttachmentSizeMb": 5,
		},
		"postAction": types.Configuration{
			"action": "none",
		},
	}
	err := node.Init(types.NewConfig(), configuration)
	assert.Nil(t, err)
	assert.Equal(t, "imap.example.com", node.Config.Server)
	assert.Equal(t, 993, node.Config.Port)
	assert.Equal(t, "INBOX", node.Config.Search.Folder)
	assert.Equal(t, 7, node.Config.Search.LastDays)
	assert.Equal(t, 10, node.Config.Search.Limit)
}

func TestReceiveEmailNodeInitDefaults(t *testing.T) {
	var node ReceiveEmailNode
	// Only required fields are provided; others use default values
	err := node.Init(types.NewConfig(), types.Configuration{
		"server":   "imap.example.com",
		"username": "user@example.com",
		"password": "pass123",
	})
	assert.Nil(t, err)
	assert.Equal(t, 993, node.Config.Port)
	assert.Equal(t, false, node.Config.EnableTLS)
	assert.Equal(t, 10, node.Config.ConnectTimeout)
	assert.Equal(t, "INBOX", node.Config.Search.Folder)
	assert.Equal(t, "full", node.Config.Fetch.ContentType)
	assert.Equal(t, "none", node.Config.PostAction.Action)
}

func TestReceiveEmailNodeInitSearchConfig(t *testing.T) {
	var node ReceiveEmailNode
	err := node.Init(types.NewConfig(), types.Configuration{
		"server":   "imap.example.com",
		"username": "u",
		"password": "p",
		"search": types.Configuration{
			"folder":  "Sent",
			"since":   "2024-01-01",
			"before":  "2024-12-31",
			"from":    "sender@example.com",
			"to":      "receiver@example.com",
			"subject": "Important",
			"unread":  true,
			"limit":   20,
		},
	})
	assert.Nil(t, err)
	assert.Equal(t, "Sent", node.Config.Search.Folder)
	assert.Equal(t, "2024-01-01", node.Config.Search.Since)
	assert.Equal(t, "2024-12-31", node.Config.Search.Before)
	assert.Equal(t, "sender@example.com", node.Config.Search.From)
	assert.Equal(t, "receiver@example.com", node.Config.Search.To)
	assert.Equal(t, "Important", node.Config.Search.Subject)
	assert.Equal(t, true, node.Config.Search.Unread)
	assert.Equal(t, 20, node.Config.Search.Limit)
}

func TestReceiveEmailNodeInitFetchConfig(t *testing.T) {
	var node ReceiveEmailNode
	err := node.Init(types.NewConfig(), types.Configuration{
		"server":   "imap.example.com",
		"username": "u",
		"password": "p",
		"fetch": types.Configuration{
			"contentType":         "headers",
			"includeAttachments":  false,
			"maxAttachmentSizeMb": 2,
			"attachmentSavePath":  "/tmp/attachments",
		},
	})
	assert.Nil(t, err)
	assert.Equal(t, "headers", node.Config.Fetch.ContentType)
	assert.Equal(t, false, node.Config.Fetch.IncludeAttachments)
	assert.Equal(t, 2, node.Config.Fetch.MaxAttachmentSizeMB)
	assert.Equal(t, "/tmp/attachments", node.Config.Fetch.AttachmentSavePath)
}

func TestReceiveEmailNodeInitPostActionConfig(t *testing.T) {
	var node ReceiveEmailNode
	err := node.Init(types.NewConfig(), types.Configuration{
		"server":   "imap.example.com",
		"username": "u",
		"password": "p",
		"postAction": types.Configuration{
			"action":       "markRead",
			"targetFolder": "",
		},
	})
	assert.Nil(t, err)
	assert.Equal(t, "markRead", node.Config.PostAction.Action)

	// move
	var node2 ReceiveEmailNode
	err = node2.Init(types.NewConfig(), types.Configuration{
		"server":   "imap.example.com",
		"username": "u",
		"password": "p",
		"postAction": types.Configuration{
			"action":       "move",
			"targetFolder": "Archive",
		},
	})
	assert.Nil(t, err)
	assert.Equal(t, "move", node2.Config.PostAction.Action)
	assert.Equal(t, "Archive", node2.Config.PostAction.TargetFolder)
}

func TestReceiveEmailNodeInitTemplate(t *testing.T) {
	var node ReceiveEmailNode
	err := node.Init(types.NewConfig(), types.Configuration{
		"server":   "${metadata.host}",
		"username": "${metadata.user}",
		"password": "${metadata.pass}",
		"search": types.Configuration{
			"since":   "${metadata.sinceDate}",
			"from":    "${metadata.from}",
			"subject": "${msg.subject}",
		},
	})
	assert.Nil(t, err)
	assert.True(t, node.hasVar)
}

// ============================================================
// OnMsg Validation Test (Not Connecting to Real Servers)
// ============================================================

func TestOnMsgMissingServer(t *testing.T) {
	var node ReceiveEmailNode
	_ = node.Init(types.NewConfig(), types.Configuration{
		"username": "u",
		"password": "p",
	})

	ctx := test.NewRuleContext(types.NewConfig(), func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Failure, relationType)
		assert.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), "server"))
	})
	msg := ctx.NewMsg("TEST", types.NewMetadata(), "{}")
	node.OnMsg(ctx, msg)
	time.Sleep(50 * time.Millisecond)
}

func TestOnMsgMissingUsername(t *testing.T) {
	var node ReceiveEmailNode
	_ = node.Init(types.NewConfig(), types.Configuration{
		"server":   "imap.example.com",
		"password": "p",
	})

	ctx := test.NewRuleContext(types.NewConfig(), func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Failure, relationType)
		assert.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), "username"))
	})
	msg := ctx.NewMsg("TEST", types.NewMetadata(), "{}")
	node.OnMsg(ctx, msg)
	time.Sleep(50 * time.Millisecond)
}

func TestOnMsgMissingPassword(t *testing.T) {
	var node ReceiveEmailNode
	_ = node.Init(types.NewConfig(), types.Configuration{
		"server":   "imap.example.com",
		"username": "u",
	})

	ctx := test.NewRuleContext(types.NewConfig(), func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Failure, relationType)
		assert.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), "password"))
	})
	msg := ctx.NewMsg("TEST", types.NewMetadata(), "{}")
	node.OnMsg(ctx, msg)
	time.Sleep(50 * time.Millisecond)
}

func TestOnMsgConnectFailed(t *testing.T) {
	// Connecting to a non-existent server should return Failure
	var node ReceiveEmailNode
	_ = node.Init(types.NewConfig(), types.Configuration{
		"server":         "127.0.0.1",
		"port":           19999,
		"username":       "u",
		"password":       "p",
		"connectTimeout": 1,
	})

	ctx := test.NewRuleContext(types.NewConfig(), func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Failure, relationType)
		assert.NotNil(t, err)
		t.Logf("Expected connection error: %v", err)
	})
	msg := ctx.NewMsg("TEST", types.NewMetadata(), "{}")
	node.OnMsg(ctx, msg)

	time.Sleep(2 * time.Second)
}

// ============================================================
// buildSearchCriteria test
// ============================================================

func TestBuildSearchCriteriaByDate(t *testing.T) {
	var node ReceiveEmailNode
	_ = node.Init(types.NewConfig(), types.Configuration{
		"server":   "imap.example.com",
		"username": "u",
		"password": "p",
	})

	criteria := node.buildSearchCriteria("2024-01-01", "2024-12-31", "", "", "")
	assert.True(t, !criteria.Since.IsZero())
	assert.True(t, !criteria.Before.IsZero())
}

func TestBuildSearchCriteriaLastDays(t *testing.T) {
	var node ReceiveEmailNode
	_ = node.Init(types.NewConfig(), types.Configuration{
		"server":   "imap.example.com",
		"username": "u",
		"password": "p",
		"search": types.Configuration{
			"lastDays": 7,
		},
	})

	criteria := node.buildSearchCriteria("2024-01-01", "", "", "", "")
	// LastDays takes precedence over Since
	expectedSince := time.Now().AddDate(0, 0, -7)
	assert.True(t, criteria.Since.Sub(expectedSince) < time.Second)
}

func TestBuildSearchCriteriaBySenderRecipientSubject(t *testing.T) {
	var node ReceiveEmailNode
	_ = node.Init(types.NewConfig(), types.Configuration{
		"server":   "imap.example.com",
		"username": "u",
		"password": "p",
	})

	criteria := node.buildSearchCriteria("", "", "alice@example.com", "bob@example.com", "Hello")
	assert.Equal(t, "alice@example.com", criteria.Header.Get("From"))
	assert.Equal(t, "bob@example.com", criteria.Header.Get("To"))
	assert.Equal(t, "Hello", criteria.Header.Get("Subject"))
}

func TestBuildSearchCriteriaUnread(t *testing.T) {
	var node ReceiveEmailNode
	_ = node.Init(types.NewConfig(), types.Configuration{
		"server":   "imap.example.com",
		"username": "u",
		"password": "p",
		"search": types.Configuration{
			"unread": true,
		},
	})

	criteria := node.buildSearchCriteria("", "", "", "", "")
	assert.Equal(t, 1, len(criteria.WithoutFlags))
	assert.Equal(t, "\\Seen", criteria.WithoutFlags[0])
}

func TestBuildSearchCriteriaEmpty(t *testing.T) {
	var node ReceiveEmailNode
	_ = node.Init(types.NewConfig(), types.Configuration{
		"server":   "imap.example.com",
		"username": "u",
		"password": "p",
	})

	criteria := node.buildSearchCriteria("", "", "", "", "")
	assert.True(t, criteria.Since.IsZero())
	assert.True(t, criteria.Before.IsZero())
	assert.Equal(t, 0, len(criteria.Header))
	assert.Equal(t, 0, len(criteria.WithoutFlags))
}

// ============================================================
// buildFetchItems test
// ============================================================

func TestBuildFetchItemsHeaders(t *testing.T) {
	var node ReceiveEmailNode
	_ = node.Init(types.NewConfig(), types.Configuration{
		"server":   "imap.example.com",
		"username": "u",
		"password": "p",
		"fetch": types.Configuration{
			"contentType": "headers",
		},
	})

	items := node.buildFetchItems()
	found := false
	for _, item := range items {
		if fmt.Sprintf("%v", item) == "ENVELOPE" {
			found = true
		}
	}
	assert.True(t, found)
}

func TestBuildFetchItemsBody(t *testing.T) {
	var node ReceiveEmailNode
	_ = node.Init(types.NewConfig(), types.Configuration{
		"server":   "imap.example.com",
		"username": "u",
		"password": "p",
		"fetch": types.Configuration{
			"contentType": "body",
		},
	})

	items := node.buildFetchItems()
	assert.True(t, len(items) > 0)
}

func TestBuildFetchItemsFull(t *testing.T) {
	var node ReceiveEmailNode
	_ = node.Init(types.NewConfig(), types.Configuration{
		"server":   "imap.example.com",
		"username": "u",
		"password": "p",
		"fetch": types.Configuration{
			"contentType": "full",
		},
	})

	items := node.buildFetchItems()
	// Full should include ENVELOPE and BODY
	assert.True(t, len(items) >= 5) // UID + Flags + Date + Envelope + Body
}

// ============================================================
// Decode function testing
// ============================================================

func TestDecodeMimeHeader(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Simple Text", "Simple Text"},
		{"=?UTF-8?B?5rWL6K+V5Lit?=", "测试中"},
		{"=?UTF-8?Q?Test_=E6=B5=8B=E8=AF=95?=", "Test 测试"},
		{"", ""},
	}

	for _, tt := range tests {
		result := decodeMimeHeader(tt.input)
		assert.Equal(t, tt.expected, result)
		if tt.input != "" {
			t.Logf("decodeMimeHeader(%q) = %q", tt.input, result)
		}
	}
}

func TestDecodeCharset(t *testing.T) {
	// UTF-8 does not convert
	assert.Equal(t, "hello", decodeCharset([]byte("hello"), "utf-8"))
	assert.Equal(t, "hello", decodeCharset([]byte("hello"), ""))
	assert.Equal(t, "hello", decodeCharset([]byte("hello"), "utf8"))
	assert.Equal(t, "hello", decodeCharset([]byte("hello"), "us-ascii"))

	// GBK to UTF-8
	gbkData := []byte{0xc4, 0xe3, 0xba, 0xc3} // "Hello" in GBK
	assert.Equal(t, "你好", decodeCharset(gbkData, "gbk"))
	assert.Equal(t, "你好", decodeCharset(gbkData, "GBK"))

	// ISO-8859-1 Special Characters
	latin1Data := []byte{0xe9} // é in ISO-8859-1
	result := decodeCharset(latin1Data, "iso-8859-1")
	assert.Equal(t, "é", result)

	// Unsupported charsets are reverted to the original string
	assert.Equal(t, "test", decodeCharset([]byte("test"), "unknown-charset"))
}

func TestDecodeBody(t *testing.T) {
	// No code
	assert.Equal(t, "plain text", string(decodeBody([]byte("plain text"), "")))
	assert.Equal(t, "plain text", string(decodeBody([]byte("plain text"), "7bit")))

	// base64 No line breaks
	b64 := base64.StdEncoding.EncodeToString([]byte("Hello World"))
	result := decodeBody([]byte(b64), "base64")
	assert.Equal(t, "Hello World", string(result))

	// base64 with line break (common format in emails)
	b64WithLineBreaks := "SGVsbG8g\r\nV29ybGQ="
	result = decodeBody([]byte(b64WithLineBreaks), "base64")
	assert.Equal(t, "Hello World", string(result))

	// base64 is case-insensitive
	result = decodeBody([]byte(b64WithLineBreaks), "Base64")
	assert.Equal(t, "Hello World", string(result))

	// base64 Reverts invalid data
	result = decodeBody([]byte("not-valid-base64!!!"), "base64")
	assert.Equal(t, "not-valid-base64!!!", string(result))
}

func TestDecodeBodyQuotedPrintable(t *testing.T) {
	// Simple QP encoding
	result := decodeBody([]byte("Hello=20World"), "quoted-printable")
	assert.Equal(t, "Hello World", string(result))

	// QP Chinese encoding
	result = decodeBody([]byte("=E4=BD=A0=E5=A5=BD"), "quoted-printable")
	assert.Equal(t, "你好", string(result))

	// QP soft line break
	result = decodeBody([]byte("Line1=\r\nLine2"), "quoted-printable")
	assert.Equal(t, "Line1Line2", string(result))

	result = decodeBody([]byte("Line1=\nLine2"), "quoted-printable")
	assert.Equal(t, "Line1Line2", string(result))
}

func TestStripBase64Whitespace(t *testing.T) {
	input := []byte("SGVs\r\nbG8=\tV29y\nbGQ=")
	result := stripBase64Whitespace(input)
	assert.Equal(t, "SGVsbG8=V29ybGQ=", string(result))
}

func TestFormatFlags(t *testing.T) {
	flags := []string{"\\Seen", "\\Flagged", "\\Answered"}
	result := formatFlags(flags)
	assert.Equal(t, 3, len(result))
	assert.Equal(t, "Seen", result[0])
	assert.Equal(t, "Flagged", result[1])
	assert.Equal(t, "Answered", result[2])

	// Flags without backslashes
	result = formatFlags([]string{"custom"})
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "custom", result[0])

	// Empty flags
	result = formatFlags([]string{})
	assert.Equal(t, 0, len(result))
}

// ============================================================
// Data structure serialization testing
// ============================================================

func TestReceiveEmailResultJSON(t *testing.T) {
	result := &ReceiveEmailResult{
		Emails: []EmailMessage{
			{
				UID:       1,
				MessageID: "<msg1@example.com>",
				Subject:   "Test",
				From:      EmailAddress{Name: "Alice", Address: "alice@example.com"},
				To:        []EmailAddress{{Address: "bob@example.com"}},
				Date:      time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
				Body:      "Hello World",
				Flags:     []string{"Seen"},
			},
		},
		Total: 1,
	}

	data, err := json.Marshal(result)
	assert.Nil(t, err)
	assert.True(t, len(data) > 0)
	t.Logf("JSON: %s", data)

	// Deserialization verification
	var parsed ReceiveEmailResult
	err = json.Unmarshal(data, &parsed)
	assert.Nil(t, err)
	assert.Equal(t, 1, parsed.Total)
	assert.Equal(t, "Test", parsed.Emails[0].Subject)
	assert.Equal(t, "alice@example.com", parsed.Emails[0].From.Address)
}

func TestEmailAttachmentJSON(t *testing.T) {
	// Embedding Base64
	att := EmailAttachment{
		Filename:      "test.txt",
		ContentType:   "text/plain",
		Size:          12,
		ContentBase64: base64.StdEncoding.EncodeToString([]byte("Hello World!")),
	}
	data, err := json.Marshal(att)
	assert.Nil(t, err)
	assert.True(t, strings.Contains(string(data), "test.txt"))

	// File path
	att2 := EmailAttachment{
		Filename:    "doc.pdf",
		ContentType: "application/pdf",
		Size:        1024,
		Path:        "/tmp/doc.pdf",
	}
	data2, err := json.Marshal(att2)
	assert.Nil(t, err)
	assert.True(t, strings.Contains(string(data2), "/tmp/doc.pdf"))
}

// ============================================================
// Template variable testing (not connected to the internet, only verifying template parsing)
// ============================================================

func TestTemplateResolution(t *testing.T) {
	var node ReceiveEmailNode
	err := node.Init(types.NewConfig(), types.Configuration{
		"server":   "${metadata.host}",
		"username": "${metadata.user}",
		"password": "${metadata.pass}",
		"search": types.Configuration{
			"since":   "${metadata.sinceDate}",
			"from":    "${metadata.from}",
			"subject": "${msg.subject}",
		},
	})
	assert.Nil(t, err)
	assert.True(t, node.hasVar)

	// Verify that the template can be executed correctly
	evn := map[string]interface{}{
		"metadata": map[string]string{
			"host":      "imap.test.com",
			"user":      "testuser",
			"pass":      "testpass",
			"sinceDate": "2024-01-01",
			"from":      "sender@test.com",
		},
		"msg": map[string]string{
			"subject": "Important Email",
		},
	}

	assert.Equal(t, "imap.test.com", node.serverTemplate.ExecuteAsString(evn))
	assert.Equal(t, "testuser", node.usernameTemplate.ExecuteAsString(evn))
	assert.Equal(t, "testpass", node.passwordTemplate.ExecuteAsString(evn))
	assert.Equal(t, "2024-01-01", node.sinceTemplate.ExecuteAsString(evn))
	assert.Equal(t, "sender@test.com", node.fromTemplate.ExecuteAsString(evn))
	assert.Equal(t, "Important Email", node.subjectTemplate.ExecuteAsString(evn))
}

// ============================================================
// Real IMAP Server Integration Test (requires environment variables)
// ============================================================

func TestReceiveEmailNodeWithRealServer(t *testing.T) {
	if os.Getenv("TEST_IMAP_USERNAME") == "" {
		t.Skip("Skipping: TEST_IMAP_USERNAME not set")
	}

	imapHost := os.Getenv("TEST_IMAP_HOST")
	if imapHost == "" {
		imapHost = "imap.gmail.com"
	}
	username := os.Getenv("TEST_IMAP_USERNAME")
	password := os.Getenv("TEST_IMAP_PASSWORD")

	var node ReceiveEmailNode
	configuration := types.Configuration{
		"server":         imapHost,
		"port":           993,
		"username":       username,
		"password":       password,
		"enableTls":      true,
		"connectTimeout": 10,
		"search": types.Configuration{
			"folder":   "INBOX",
			"lastDays": 7,
			"limit":    5,
		},
		"fetch": types.Configuration{
			"contentType":         "full",
			"includeAttachments":  true,
			"maxAttachmentSizeMb": 5,
		},
		"postAction": types.Configuration{
			"action": "none",
		},
	}
	err := node.Init(types.NewConfig(), configuration)
	assert.Nil(t, err)

	config := types.NewConfig()
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
		if err != nil {
			t.Logf("Error: %v", err)
		}
		assert.Equal(t, types.Success, relationType)
		assert.Nil(t, err)

		data := msg.GetData()
		assert.True(t, len(data) > 0)
		t.Logf("Received data: %s", data)

		// Verify the JSON structure
		var result ReceiveEmailResult
		err = json.Unmarshal([]byte(data), &result)
		assert.Nil(t, err)
		t.Logf("Total: %d, Fetched: %d", result.Total, len(result.Emails))
	})
	metaData := types.NewMetadata()
	msg := ctx.NewMsg("TEST_MSG", metaData, "{}")
	node.OnMsg(ctx, msg)

	time.Sleep(3 * time.Second)
}

func TestListMailboxes(t *testing.T) {
	if os.Getenv("TEST_IMAP_USERNAME") == "" {
		t.Skip("Skipping: TEST_IMAP_USERNAME not set")
	}

	imapHost := os.Getenv("TEST_IMAP_HOST")
	if imapHost == "" {
		imapHost = "imap.gmail.com"
	}
	username := os.Getenv("TEST_IMAP_USERNAME")
	password := os.Getenv("TEST_IMAP_PASSWORD")

	result, err := ListMailboxes(imapHost, 993, username, password, true)
	if err != nil {
		t.Logf("Error: %v", err)
		if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "timeout") {
			t.Skip("Skipping: network error")
		}
		t.Fatalf("ListMailboxes failed: %v", err)
	}

	assert.NotNil(t, result)
	t.Logf("Found %d mailboxes", len(result.Mailboxes))
	for _, m := range result.Mailboxes {
		t.Logf("  - %s (attributes: %v)", m.Name, m.Attributes)
	}

	hasInbox := false
	for _, m := range result.Mailboxes {
		if m.Name == "INBOX" {
			hasInbox = true
			break
		}
	}
	assert.True(t, hasInbox)
}

func TestListMailboxesWithConfig(t *testing.T) {
	if os.Getenv("TEST_IMAP_USERNAME") == "" {
		t.Skip("Skipping: TEST_IMAP_USERNAME not set")
	}

	imapHost := os.Getenv("TEST_IMAP_HOST")
	if imapHost == "" {
		imapHost = "imap.gmail.com"
	}
	username := os.Getenv("TEST_IMAP_USERNAME")
	password := os.Getenv("TEST_IMAP_PASSWORD")

	config := ReceiveEmailConfiguration{
		Server:    imapHost,
		Port:      993,
		Username:  username,
		Password:  password,
		EnableTLS: true,
	}

	result, err := ListMailboxesWithConfig(config)
	if err != nil {
		t.Logf("Error: %v", err)
		if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "timeout") {
			t.Skip("Skipping: network error")
		}
		t.Fatalf("ListMailboxesWithConfig failed: %v", err)
	}

	assert.NotNil(t, result)
	t.Logf("Found %d mailboxes", len(result.Mailboxes))
}
