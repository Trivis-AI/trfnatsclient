package natsclient

import (
	"fmt"
	"log"
	"time"

	nats "github.com/nats-io/nats.go"
)

const (
	StreamEmbedding     = "TRF_EMBEDDING"
	SubjectEmbedRequest = "embed.request"

	StreamAudit       = "TRF_AUDIT"
	SubjectAuditEvent = "audit.event"

	StreamErrors      = "TRF_ERRORS"
	SubjectErrorEvent = "error.event"
)

// ErrorStreamMaxAge bounds how long undelivered error events are kept in
// JetStream. Errors are operational telemetry, not compliance data.
const ErrorStreamMaxAge = 30 * 24 * time.Hour

// EmbedRequest is published by domain services when embeddable content changes.
type EmbedRequest struct {
	OrgID       string `json:"org_id"`    // UUID as string
	Domain      string `json:"domain"`    // "journal_entry" | "invoice" | "contact"
	RecordID    string `json:"record_id"` // UUID as string
	Text        string `json:"text"`
	ContentHash string `json:"content_hash"` // SHA256(text) — skip if unchanged
}

// AuditEvent is published by domain services on every data mutation.
type AuditEvent struct {
	EventID    string `json:"event_id"` // uuid — idempotency key
	OrgID      string `json:"org_id"`
	Service    string `json:"service"`     // "ledger"|"invoices"|"crm"|"payments"
	EntityType string `json:"entity_type"` // "journal_entry"|"invoice"|"contact"|"payment"|etc.
	EntityID   string `json:"entity_id"`   // uuid
	Action     string `json:"action"`      // "create"|"update"|"delete"|"post"|"void"|"confirm"|etc.
	ActorID    string `json:"actor_id"`    // account uuid from JWT
	ActorName  string `json:"actor_name"`  // username from JWT
	OccurredAt string `json:"occurred_at"` // RFC3339
	Summary    string `json:"summary"`     // e.g. "Invoice INV-001 confirmed"
}

// ErrorEvent is published by any backend when a user-visible failure occurs:
// LLM timeouts, tool calls with bad params, file upload/parse failures,
// detected user frustration, HTTP error responses. Consumed centrally by
// trfservices into its error_events table. Publishing must always be
// best-effort fire-and-forget — never let telemetry break the main flow.
type ErrorEvent struct {
	EventID    string `json:"event_id"`   // uuid — idempotency key
	OrgID      string `json:"org_id"`     // uuid
	UserID     string `json:"user_id"`    // account uuid, "" when not user-scoped
	Service    string `json:"service"`    // "ai"|"files"|"invoices"|etc.
	Category   string `json:"category"`   // llm_timeout|llm_error|tool_bad_params|tool_error|file_upload_fail|file_parse_fail|user_frustration|http_error
	Severity   string `json:"severity"`   // info|warning|error
	SessionID  string `json:"session_id"` // AI session uuid, "" when not applicable
	TraceID    string `json:"trace_id"`   // correlates with structured stdout logs
	Tool       string `json:"tool"`       // tool name, "" when not applicable
	Message    string `json:"message"`
	Details    string `json:"details"`     // JSON blob: redacted params, status codes, latency, attempt counts
	OccurredAt string `json:"occurred_at"` // RFC3339
}

// EnsureStreams creates required JetStream streams if they do not exist.
// Safe to call on every service startup.
func EnsureStreams(js nats.JetStreamContext) error {
	if err := ensureStream(js, StreamEmbedding, []string{SubjectEmbedRequest}, 0); err != nil {
		return err
	}
	if err := ensureStream(js, StreamAudit, []string{SubjectAuditEvent}, 0); err != nil {
		return err
	}
	return ensureStream(js, StreamErrors, []string{SubjectErrorEvent}, ErrorStreamMaxAge)
}

func ensureStream(js nats.JetStreamContext, name string, subjects []string, maxAge time.Duration) error {
	_, err := js.StreamInfo(name)
	if err == nil {
		log.Printf(`{"op":"nats.stream.ensure.exists","stream":"%s"}`, name)
		return nil
	}
	if err != nats.ErrStreamNotFound {
		return fmt.Errorf("stream info %s: %w", name, err)
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     name,
		Subjects: subjects,
		Storage:  nats.FileStorage,
		MaxAge:   maxAge, // 0 = no expiry
	})
	if err != nil {
		return fmt.Errorf("add stream %s: %w", name, err)
	}
	log.Printf(`{"op":"nats.stream.ensure.created","stream":"%s"}`, name)
	return nil
}
