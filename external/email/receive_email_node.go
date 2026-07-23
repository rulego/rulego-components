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
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/text/encoding/htmlindex"
	"golang.org/x/text/transform"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

const (
	defaultIMAPPort         = 993
	defaultConnectTimeout   = 10
	defaultMailFolder       = "INBOX"
	defaultFetchContentType = "full"
	defaultPostAction       = "none"
	dateLayout              = "2006-01-02"
	nodeType                = "x/receiveEmail"

	// Post action types
	actionMarkRead = "markRead"
	actionDelete   = "delete"
	actionMove     = "move"

	// Content type values
	contentTypeHeaders = "headers"
	contentTypeBody    = "body"

	// Default attachment filename
	defaultAttachmentFilename = "attachment"
)

func init() {
	_ = rulego.Registry.Register(&ReceiveEmailNode{})
}

// SearchConfig Email search configuration
type SearchConfig struct {
	Folder   string `json:"folder" label:"Folder" desc:"Mailbox folder name, e.g. INBOX, Sent"`
	Since    string `json:"since" label:"Since" desc:"Search start date, format: 2006-01-02"`
	Before   string `json:"before" label:"Before" desc:"Search end date, format: 2006-01-02"`
	LastDays int    `json:"lastDays" label:"Last Days" desc:"Search emails from the last N days"`
	From     string `json:"from" label:"From" desc:"Filter by sender address"`
	To       string `json:"to" label:"To" desc:"Filter by recipient address"`
	Subject  string `json:"subject" label:"Subject" desc:"Filter by email subject"`
	Unread   bool   `json:"unread" label:"Unread Only" desc:"Search only unread emails"`
	Limit    int    `json:"limit" label:"Limit" desc:"Maximum number of emails to return"`
}

// FetchConfig email acquisition option
type FetchConfig struct {
	ContentType         string `json:"contentType" label:"Content Type" desc:"Email content type: text, html, default text"`
	IncludeAttachments  bool   `json:"includeAttachments" label:"Include Attachments" desc:"Download email attachments"`
	MaxAttachmentSizeMB int    `json:"maxAttachmentSizeMb" label:"Max Attachment Size (MB)" desc:"Max attachment size in MB, skip if exceeded"`
	AttachmentSavePath  string `json:"attachmentSavePath" label:"Attachment Save Path" desc:"Directory path to save attachments"`
}

// PostActionConfig Operation configuration after execution
type PostActionConfig struct {
	Action       string `json:"action" label:"Action" desc:"Post-fetch action: markRead, move, delete"`
	TargetFolder string `json:"targetFolder" label:"Target Folder" desc:"Target folder for move action"`
}

// ReceiveEmailConfiguration
type ReceiveEmailConfiguration struct {
	Server         string           `json:"server" label:"Server" desc:"IMAP server address" required:"true" ref:"primary"`
	Port           int              `json:"port" label:"Port" desc:"IMAP server port, default 993"`
	Username       string           `json:"username" label:"Username" desc:"Email login username" required:"true" ref:"shared"`
	Password       string           `json:"password" label:"Password" desc:"Email login password" required:"true" ref:"shared"`
	EnableTLS      bool             `json:"enableTls" label:"Enable TLS" desc:"Enable TLS encryption"`
	ConnectTimeout int              `json:"connectTimeout" label:"Connect Timeout (s)" desc:"Connection timeout in seconds"`
	Search         SearchConfig     `json:"search" label:"Search" desc:"Email search criteria configuration"`
	Fetch          FetchConfig      `json:"fetch" label:"Fetch" desc:"Email content fetch configuration"`
	PostAction     PostActionConfig `json:"postAction" label:"Post Action" desc:"Post-fetch action configuration"`
}

// EmailAddress
type EmailAddress struct {
	Name    string `json:"name,omitempty"`
	Address string `json:"address"`
}

// EmailAttachment
type EmailAttachment struct {
	Filename string `json:"filename"`
	// ContentType MIME
	ContentType string `json:"contentType,omitempty"`
	// Size (bytes)
	Size int64 `json:"size"`
	// ContentBase64 Base64 encoded content (when not saved to file)
	ContentBase64 string `json:"contentBase64,omitempty"`
	// Path (when saving to a file)
	Path string `json:"path,omitempty"`
}

// EmailMessage
type EmailMessage struct {
	// UID email uniqueness
	UID uint32 `json:"uid"`
	// MessageID
	MessageID string `json:"messageId,omitempty"`
	// Subject
	Subject string `json:"subject"`
	// From the sender
	From EmailAddress `json:"from"`
	// To the recipient list
	To []EmailAddress `json:"to,omitempty"`
	// CC CC list
	Cc []EmailAddress `json:"cc,omitempty"`
	// Date
	Date time.Time `json:"date"`
	// Body main text content
	Body string `json:"body"`
	// HtmlBody HTML body
	HtmlBody string `json:"htmlBody,omitempty"`
	// Headers heading fields
	Headers map[string]string `json:"headers,omitempty"`
	// Attachments list
	Attachments []EmailAttachment `json:"attachments,omitempty"`
	// Flags mark
	Flags []string `json:"flags,omitempty"`
}

// ReceiveEmailResult: Receive the result of the email
type ReceiveEmailResult struct {
	Emails []EmailMessage `json:"emails"`
	Total  int            `json:"total"`
}

// MailboxInfo folder information
type MailboxInfo struct {
	// Name: Mailing folder name
	Name string `json:"name"`
	// Delim hierarchical separator
	Delim string `json:"delim,omitempty"`
	// Attributes
	Attributes []string `json:"attributes,omitempty"`
}

// ListMailboxesResult to get the results of the mailbox list
type ListMailboxesResult struct {
	Mailboxes []MailboxInfo `json:"mailboxes"`
}

// ReceiveEmailNode receives emails via the IMAP protocol
// If the request is `Success`ful, send the message to the 'Success' chain; otherwise, send it to the `Failure` chain
type ReceiveEmailNode struct {
	Config                 ReceiveEmailConfiguration
	ConnectTimeoutDuration time.Duration
	ruleConfig             types.Config

	// Template field
	serverTemplate     el.Template
	usernameTemplate   el.Template
	passwordTemplate   el.Template
	sinceTemplate      el.Template
	beforeTemplate     el.Template
	fromTemplate       el.Template
	toTemplate         el.Template
	subjectTemplate    el.Template
	attachmentPathTemp el.Template

	// Whether variables are included
	hasVar bool
}

// Type returns the component type
func (x *ReceiveEmailNode) Type() string {
	return nodeType
}

// New creates an instance
func (x *ReceiveEmailNode) New() types.Node {
	return &ReceiveEmailNode{
		Config: ReceiveEmailConfiguration{
			Port:           defaultIMAPPort,
			EnableTLS:      true,
			ConnectTimeout: defaultConnectTimeout,
			Search: SearchConfig{
				Folder: defaultMailFolder,
			},
			Fetch: FetchConfig{
				ContentType: defaultFetchContentType,
			},
			PostAction: PostActionConfig{
				Action: defaultPostAction,
			},
		},
	}
}

// Init initializes the component
func (x *ReceiveEmailNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	x.ruleConfig = ruleConfig
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}

	// Set the default value
	if x.Config.Port == 0 {
		x.Config.Port = defaultIMAPPort
	}
	if x.Config.ConnectTimeout <= 0 {
		x.Config.ConnectTimeout = defaultConnectTimeout
	}
	if x.Config.Search.Folder == "" {
		x.Config.Search.Folder = defaultMailFolder
	}
	if x.Config.Fetch.ContentType == "" {
		x.Config.Fetch.ContentType = defaultFetchContentType
	}
	if x.Config.PostAction.Action == "" {
		x.Config.PostAction.Action = defaultPostAction
	}

	x.ConnectTimeoutDuration = time.Duration(x.Config.ConnectTimeout) * time.Second

	// Initialize the template
	if x.serverTemplate, err = el.NewTemplate(x.Config.Server); err != nil {
		return err
	}
	if x.usernameTemplate, err = el.NewTemplate(x.Config.Username); err != nil {
		return err
	}
	if x.passwordTemplate, err = el.NewTemplate(x.Config.Password); err != nil {
		return err
	}
	if x.sinceTemplate, err = el.NewTemplate(x.Config.Search.Since); err != nil {
		return err
	}
	if x.beforeTemplate, err = el.NewTemplate(x.Config.Search.Before); err != nil {
		return err
	}
	if x.fromTemplate, err = el.NewTemplate(x.Config.Search.From); err != nil {
		return err
	}
	if x.toTemplate, err = el.NewTemplate(x.Config.Search.To); err != nil {
		return err
	}
	if x.subjectTemplate, err = el.NewTemplate(x.Config.Search.Subject); err != nil {
		return err
	}
	if x.attachmentPathTemp, err = el.NewTemplate(x.Config.Fetch.AttachmentSavePath); err != nil {
		return err
	}

	// Check if variables are included
	x.hasVar = x.serverTemplate.HasVar() || x.usernameTemplate.HasVar() || x.passwordTemplate.HasVar() ||
		x.sinceTemplate.HasVar() || x.beforeTemplate.HasVar() || x.fromTemplate.HasVar() ||
		x.toTemplate.HasVar() || x.subjectTemplate.HasVar() || x.attachmentPathTemp.HasVar()

	return nil
}

// OnMsg processes a message
func (x *ReceiveEmailNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	// Get environment variables
	var evn map[string]interface{}
	if x.hasVar {
		evn = base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	}

	// Execute the template
	server := x.serverTemplate.ExecuteAsString(evn)
	username := x.usernameTemplate.ExecuteAsString(evn)
	password := x.passwordTemplate.ExecuteAsString(evn)
	since := x.sinceTemplate.ExecuteAsString(evn)
	before := x.beforeTemplate.ExecuteAsString(evn)
	from := x.fromTemplate.ExecuteAsString(evn)
	to := x.toTemplate.ExecuteAsString(evn)
	subject := x.subjectTemplate.ExecuteAsString(evn)
	attachmentPath := x.attachmentPathTemp.ExecuteAsString(evn)

	// Validate required fields
	if server == "" {
		ctx.TellFailure(msg, errors.New("server is required"))
		return
	}
	if username == "" {
		ctx.TellFailure(msg, errors.New("username is required"))
		return
	}
	if password == "" {
		ctx.TellFailure(msg, errors.New("password is required"))
		return
	}

	// Perform receiving emails
	result, err := x.receiveEmails(server, username, password, since, before, from, to, subject, attachmentPath)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// Update the news
	data := str.ToString(result)
	msg.SetData(data)
	ctx.TellSuccess(msg)
}

// Destroy releases resources
func (x *ReceiveEmailNode) Destroy() {
}

// Desc returns the component description
func (x *ReceiveEmailNode) Desc() string {
	return "Receive emails via IMAP protocol. Supports SSL/TLS. Routes to Success/Failure"
}

// receiveEmails Receive emails
func (x *ReceiveEmailNode) receiveEmails(server, username, password, since, before, from, to, subject, attachmentPath string) (*ReceiveEmailResult, error) {
	addr := fmt.Sprintf("%s:%d", server, x.Config.Port)

	// Connect to the IMAP server
	var c *client.Client
	var err error

	dialer := &net.Dialer{Timeout: x.ConnectTimeoutDuration}

	if x.Config.EnableTLS {
		c, err = client.DialWithDialerTLS(dialer, addr, nil)
	} else {
		c, err = client.DialWithDialer(dialer, addr)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to IMAP server: %v", err)
	}
	defer c.Logout()

	// Log in
	if err := c.Login(username, password); err != nil {
		return nil, fmt.Errorf("login failed: %v", err)
	}

	// Select the mailbox
	mbox, err := c.Select(x.Config.Search.Folder, false)
	if err != nil {
		return nil, fmt.Errorf("failed to select folder: %v", err)
	}

	// If the mailbox is empty
	if mbox.Messages == 0 {
		return &ReceiveEmailResult{Emails: []EmailMessage{}, Total: 0}, nil
	}

	// Construct search criteria
	criteria := x.buildSearchCriteria(since, before, from, to, subject)

	// Search emails
	uids, err := c.Search(criteria)
	if err != nil {
		return nil, fmt.Errorf("search failed: %v", err)
	}

	if len(uids) == 0 {
		return &ReceiveEmailResult{Emails: []EmailMessage{}, Total: 0}, nil
	}

	// Apply limit
	total := len(uids)
	limit := x.Config.Search.Limit
	if limit > 0 && len(uids) > limit {
		// Retrieve the latest N emails (from back to front)
		uids = uids[len(uids)-limit:]
	}

	// Get the email
	seqset := new(imap.SeqSet)
	seqset.AddNum(uids...)

	// Retrieve the email content
	messages := make(chan *imap.Message, len(uids))
	fetchItems := x.buildFetchItems()

	err = c.Fetch(seqset, fetchItems, messages)
	if err != nil {
		return nil, fmt.Errorf("fetch failed: %v", err)
	}

	// Parsing emails
	var emails []EmailMessage
	for msg := range messages {
		if msg == nil {
			continue
		}
		email, err := x.parseEmail(msg, attachmentPath)
		if err != nil {
			x.ruleConfig.Logger.Warnf("parseEmail uid=%d failed: %v", msg.Uid, err)
			continue
		}
		emails = append(emails, *email)
	}

	// Perform post-processing
	if x.Config.PostAction.Action != defaultPostAction && len(emails) > 0 {
		x.executePostAction(c, uids)
	}

	return &ReceiveEmailResult{Emails: emails, Total: total}, nil
}

// buildSearchCriteria Constructs the search criteria
func (x *ReceiveEmailNode) buildSearchCriteria(since, before, from, to, subject string) *imap.SearchCriteria {
	criteria := &imap.SearchCriteria{
		Header: make(textproto.MIMEHeader),
	}

	// Time conditions
	if x.Config.Search.LastDays > 0 {
		criteria.Since = time.Now().AddDate(0, 0, -x.Config.Search.LastDays)
	} else {
		if since != "" {
			if t, err := time.Parse(dateLayout, since); err == nil {
				criteria.Since = t
			}
		}
		if before != "" {
			if t, err := time.Parse(dateLayout, before); err == nil {
				criteria.Before = t
			}
		}
	}

	// Sender
	if from != "" {
		criteria.Header.Add("From", from)
	}

	// Recipient
	if to != "" {
		criteria.Header.Add("To", to)
	}

	// Theme
	if subject != "" {
		criteria.Header.Add("Subject", subject)
	}

	// Unread mark
	if x.Config.Search.Unread {
		criteria.WithoutFlags = []string{imap.SeenFlag}
	}

	return criteria
}

// buildFetchItems to construct the getter item
func (x *ReceiveEmailNode) buildFetchItems() []imap.FetchItem {
	items := []imap.FetchItem{imap.FetchUid, imap.FetchFlags, imap.FetchInternalDate}

	switch x.Config.Fetch.ContentType {
	case contentTypeHeaders:
		items = append(items, imap.FetchEnvelope)
	case contentTypeBody:
		items = append(items, imap.FetchBody)
	default:
		// full or other values by default get the full content
		items = append(items, imap.FetchEnvelope, imap.FetchBody)
	}

	return items
}

// parseEmail parses emails
func (x *ReceiveEmailNode) parseEmail(msg *imap.Message, attachmentPath string) (*EmailMessage, error) {
	email := &EmailMessage{
		UID:     msg.Uid,
		Date:    msg.InternalDate,
		Flags:   formatFlags(msg.Flags),
		Headers: make(map[string]string),
	}

	// Analyze the envelope
	if msg.Envelope != nil {
		email.MessageID = msg.Envelope.MessageId
		email.Subject = decodeMimeHeader(msg.Envelope.Subject)

		// Sender
		if len(msg.Envelope.From) > 0 {
			email.From = EmailAddress{
				Name:    decodeMimeHeader(msg.Envelope.From[0].PersonalName),
				Address: msg.Envelope.From[0].Address(),
			}
		}

		// Recipient
		for _, addr := range msg.Envelope.To {
			email.To = append(email.To, EmailAddress{
				Name:    decodeMimeHeader(addr.PersonalName),
				Address: addr.Address(),
			})
		}

		// Copying people
		for _, addr := range msg.Envelope.Cc {
			email.Cc = append(email.Cc, EmailAddress{
				Name:    decodeMimeHeader(addr.PersonalName),
				Address: addr.Address(),
			})
		}
	}

	// Analyze the main text and attachments
	if x.Config.Fetch.ContentType != contentTypeHeaders {
		if err := x.parseBody(msg, email, attachmentPath); err != nil {
			x.ruleConfig.Logger.Warnf("parseBody uid=%d failed: %v", msg.Uid, err)
		}
	}

	return email, nil
}

// parseBody parses the main text and attachments
func (x *ReceiveEmailNode) parseBody(msg *imap.Message, email *EmailMessage, attachmentPath string) error {
	// Get the main text
	section := &imap.BodySectionName{}
	reader := msg.GetBody(section)
	if reader == nil {
		return nil
	}

	// Read the full content
	bodyBytes, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	contentType := "text/plain"
	contentTransferEncoding := ""

	// Separate the head from the original content from the main text
	var sepLen int
	headerEnd := bytes.Index(bodyBytes, []byte("\r\n\r\n"))
	if headerEnd != -1 {
		sepLen = 4
	} else {
		headerEnd = bytes.Index(bodyBytes, []byte("\n\n"))
		if headerEnd != -1 {
			sepLen = 2
		}
	}

	var headerBytes []byte
	var bodyContent []byte
	if headerEnd != -1 {
		headerBytes = bodyBytes[:headerEnd]
		bodyContent = bodyBytes[headerEnd+sepLen:]
	} else {
		bodyContent = bodyBytes
	}

	// Analyze the head
	if len(headerBytes) > 0 {
		headerReader := textproto.NewReader(bufio.NewReader(bytes.NewReader(headerBytes)))
		header, err := headerReader.ReadMIMEHeader()
		if err == nil {
			contentType = header.Get("Content-Type")
			contentTransferEncoding = header.Get("Content-Transfer-Encoding")
		}
	}

	// Parse Content-Type
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		email.Body = string(bodyContent)
		return nil
	}

	// Handles multipart messages
	if strings.HasPrefix(mediaType, "multipart/") {
		boundary := params["boundary"]
		if boundary == "" {
			return nil
		}

		mr := multipart.NewReader(bytes.NewReader(bodyContent), boundary)
		for {
			part, err := mr.NextPart()
			if err != nil {
				break
			}

			partMediaType, partParams, err := mime.ParseMediaType(part.Header.Get("Content-Type"))
			if err != nil {
				continue
			}

			// Handling attachments
			filename := part.FileName()
			if filename != "" || part.Header.Get("Content-Disposition") != "" {
				if x.Config.Fetch.IncludeAttachments {
					x.processAttachment(part, filename, partMediaType, email, attachmentPath)
				}
				continue
			}

			// Handle the main text
			partBytes, err := io.ReadAll(part)
			if err != nil {
				continue
			}

			partEncoding := part.Header.Get("Content-Transfer-Encoding")
			decoded := decodeBody(partBytes, partEncoding)
			bodyStr := decodeCharset(decoded, partParams["charset"])

			if strings.HasPrefix(partMediaType, "text/html") {
				email.HtmlBody = bodyStr
			} else if strings.HasPrefix(partMediaType, "text/plain") {
				email.Body = bodyStr
			}
		}
	} else {
		// Single part of the news
		decoded := decodeBody(bodyContent, contentTransferEncoding)
		bodyStr := decodeCharset(decoded, params["charset"])

		if strings.HasPrefix(mediaType, "text/html") {
			email.HtmlBody = bodyStr
		} else {
			email.Body = bodyStr
		}
	}

	return nil
}

// processAttachment handles attachments
func (x *ReceiveEmailNode) processAttachment(part *multipart.Part, filename, contentType string, email *EmailMessage, savePath string) {
	// Decode the file name
	filename = decodeMimeHeader(filename)
	if filename == "" {
		filename = defaultAttachmentFilename
	}

	// Get the attachment content
	content, err := io.ReadAll(part)
	if err != nil {
		x.ruleConfig.Logger.Warnf("read attachment %s failed: %v", filename, err)
		return
	}

	// Check the size limit
	maxSize := int64(x.Config.Fetch.MaxAttachmentSizeMB) * 1024 * 1024
	if maxSize > 0 && int64(len(content)) > maxSize {
		x.ruleConfig.Logger.Infof("attachment %s size %d exceeds limit %d, skipped", filename, len(content), maxSize)
		return
	}

	attachment := EmailAttachment{
		Filename:    filename,
		ContentType: contentType,
		Size:        int64(len(content)),
	}

	// Save to a file or embed it in a message
	if savePath != "" {
		fullPath := filepath.Join(savePath, filename)
		if err := os.MkdirAll(savePath, 0755); err != nil {
			x.ruleConfig.Logger.Warnf("create dir %s failed: %v", savePath, err)
		} else if err := os.WriteFile(fullPath, content, 0644); err != nil {
			x.ruleConfig.Logger.Warnf("write attachment %s failed: %v", fullPath, err)
		} else {
			attachment.Path = fullPath
		}
	} else {
		// No save path specified, embedded in the message
		attachment.ContentBase64 = base64.StdEncoding.EncodeToString(content)
	}

	email.Attachments = append(email.Attachments, attachment)
}

// executePostAction executes the post-action action
func (x *ReceiveEmailNode) executePostAction(c *client.Client, uids []uint32) {
	seqset := new(imap.SeqSet)
	seqset.AddNum(uids...)

	switch x.Config.PostAction.Action {
	case actionMarkRead:
		flags := []interface{}{imap.SeenFlag}
		if err := c.Store(seqset, imap.AddFlags, flags, nil); err != nil {
			x.ruleConfig.Logger.Warnf("markRead failed: %v", err)
		}
	case actionDelete:
		flags := []interface{}{imap.DeletedFlag}
		if err := c.Store(seqset, imap.AddFlags, flags, nil); err != nil {
			x.ruleConfig.Logger.Warnf("delete mark failed: %v", err)
		}
		if err := c.Expunge(nil); err != nil {
			x.ruleConfig.Logger.Warnf("expunge failed: %v", err)
		}
	case actionMove:
		if x.Config.PostAction.TargetFolder != "" {
			if err := c.Move(seqset, x.Config.PostAction.TargetFolder); err != nil {
				x.ruleConfig.Logger.Warnf("move to %s failed: %v", x.Config.PostAction.TargetFolder, err)
			}
		}
	}
}

// decodeMimeHeader Decodes the MIME header
func decodeMimeHeader(header string) string {
	if header == "" {
		return ""
	}
	dec := new(mime.WordDecoder)
	decoded, err := dec.DecodeHeader(header)
	if err != nil {
		return header
	}
	return decoded
}

// decodeBody decodes the body transfer encoding (base64/quoted-printable), returning the original byte after decoding
func decodeBody(body []byte, encoding string) []byte {
	encoding = strings.ToLower(strings.TrimSpace(encoding))
	switch encoding {
	case "base64":
		// Email base64 content should be folded into lines every 76 characters, with spaces removed
		cleaned := stripBase64Whitespace(body)
		decoded, err := base64.StdEncoding.DecodeString(string(cleaned))
		if err != nil {
			return body
		}
		return decoded
	case "quoted-printable":
		return decodeQuotedPrintable(body)
	default:
		return body
	}
}

// decodeCharset converts bytes from a specified character set to UTF-8
func decodeCharset(data []byte, charset string) string {
	charset = strings.ToLower(strings.TrimSpace(charset))
	if charset == "" || charset == "utf-8" || charset == "utf8" || charset == "us-ascii" {
		return string(data)
	}
	enc, err := htmlindex.Get(charset)
	if err != nil {
		return string(data)
	}
	decoded, err := io.ReadAll(transform.NewReader(bytes.NewReader(data), enc.NewDecoder()))
	if err != nil {
		return string(data)
	}
	return string(decoded)
}

// stripBase64Whitespace removes the whitespace characters from base64 content
func stripBase64Whitespace(data []byte) []byte {
	return bytes.Map(func(r rune) rune {
		if r == '\r' || r == '\n' || r == ' ' || r == '\t' {
			return -1
		}
		return r
	}, data)
}

// decodeQuotedPrintable Decodes quoted-printable encoding
func decodeQuotedPrintable(data []byte) []byte {
	var buf bytes.Buffer
	i := 0
	for i < len(data) {
		if data[i] == '=' {
			if i+2 < len(data) && isHexChar(data[i+1]) && isHexChar(data[i+2]) {
				buf.WriteByte(hexVal(data[i+1])<<4 | hexVal(data[i+2]))
				i += 3
			} else if bytes.HasPrefix(data[i:], []byte("=\r\n")) {
				i += 3 // soft line break
			} else if bytes.HasPrefix(data[i:], []byte("=\n")) {
				i += 2 // soft line break
			} else {
				buf.WriteByte(data[i])
				i++
			}
		} else {
			buf.WriteByte(data[i])
			i++
		}
	}
	return buf.Bytes()
}

func isHexChar(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f')
}

func hexVal(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	default:
		return c - 'a' + 10
	}
}

// formatFlags
func formatFlags(flags []string) []string {
	var result []string
	for _, flag := range flags {
		// Remove the backslash before the mark
		flag = strings.TrimPrefix(flag, "\\")
		result = append(result, flag)
	}
	return result
}

// ============================================================
// Utility function - Get the mailbox list
// ============================================================

// ListMailboxes retrieves a list of all mailboxes on the IMAP server
// server: IMAP server address (e.g., imap.gmail.com)
// port: IMAP port (e.g., 993)
// username: username
// password: password
// enableTLS: Whether TLS is enabled
// Return to mailbox list
func ListMailboxes(server string, port int, username, password string, enableTLS bool) (*ListMailboxesResult, error) {
	addr := fmt.Sprintf("%s:%d", server, port)

	// Connect to the IMAP server
	var c *client.Client
	var err error

	dialer := &net.Dialer{Timeout: 10 * time.Second}

	if enableTLS {
		c, err = client.DialWithDialerTLS(dialer, addr, nil)
	} else {
		c, err = client.DialWithDialer(dialer, addr)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to IMAP server: %v", err)
	}
	defer c.Logout()

	// Log in
	if err := c.Login(username, password); err != nil {
		return nil, fmt.Errorf("login failed: %v", err)
	}

	// Get the mailbox list
	mailboxes := make(chan *imap.MailboxInfo, 10)
	done := make(chan error, 1)

	go func() {
		done <- c.List("", "*", mailboxes)
	}()

	var result []MailboxInfo
	for m := range mailboxes {
		result = append(result, MailboxInfo{
			Name:       m.Name,
			Delim:      m.Delimiter,
			Attributes: m.Attributes,
		})
	}

	if err := <-done; err != nil {
		return nil, fmt.Errorf("failed to list mailboxes: %v", err)
	}

	return &ListMailboxesResult{Mailboxes: result}, nil
}

// ListMailboxesWithConfig uses a configuration object to get the mailbox list
func ListMailboxesWithConfig(config ReceiveEmailConfiguration) (*ListMailboxesResult, error) {
	return ListMailboxes(config.Server, config.Port, config.Username, config.Password, config.EnableTLS)
}
