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

	StreamSupport          = "TRF_SUPPORT"
	SubjectAssistRequested = "support.assist.requested"
)

// ErrorStreamMaxAge bounds how long undelivered error events are kept in
// JetStream. Errors are operational telemetry, not compliance data.
const ErrorStreamMaxAge = 30 * 24 * time.Hour

// SupportStreamMaxAge bounds how long undelivered support events are kept.
// Short on purpose: an assist request is a person waiting right now, and
// replaying a week-old one would page an operator to a conversation that ended
// days ago. If it was not delivered within a day it is scrap — the poller
// rebuilds the queue from state regardless, so nothing is actually lost.
const SupportStreamMaxAge = 24 * time.Hour

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
	ActorID    string `json:"actor_id"`    // executor: account, agent or service principal uuid; never empty
	ActorName  string `json:"actor_name"`  // executor's name
	OccurredAt string `json:"occurred_at"` // RFC3339
	Summary    string `json:"summary"`     // e.g. "Invoice INV-001 confirmed"

	// Audit v2 (documentation/AGENT-PRINCIPALS-PHASE0.md, "Audit event v2").
	// Build events with trflib/audit.New, which fills these from the verified
	// token; never set them by hand. An event without ActorKind is v1.
	ActorKind       string `json:"actor_kind,omitempty"`        // person|agent|service
	OnBehalfOfID    string `json:"on_behalf_of_id,omitempty"`   // person a non-person acted for (token "d")
	OnBehalfOfName  string `json:"on_behalf_of_name,omitempty"` //
	AccountableID   string `json:"accountable_id,omitempty"`    // person who answers for it; empty only for a service on its own account
	Via             string `json:"via,omitempty"`               // channel: "web", "mcp:<client>", "api", "nats:<subject>"
	RunID           string `json:"run_id,omitempty"`            // Phase 1: agent run
	ActionID        string `json:"action_id,omitempty"`         // Phase 1: one tool call / write
	AgentVersion    string `json:"agent_version,omitempty"`     // Phase 1
	ExternalRef     string `json:"external_ref,omitempty"`      // caller's own reference (batch id, idempotency key)
	CorrectsEventID string `json:"corrects_event_id,omitempty"` // this write reverses or amends that event
	SuggestedByID   string `json:"suggested_by_id,omitempty"`   // Phase 1: agent whose result this applied
	SuggestedByRun  string `json:"suggested_by_run,omitempty"`  //
	SuggestedByVer  string `json:"suggested_by_version,omitempty"`
	SuggestionEdit  bool   `json:"suggestion_edited,omitempty"` // the person changed the suggestion before applying it
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

// AssistRequested is published by trfbackai the moment a customer presses
// "talk to a human", and consumed by trfbacksupport to put the chat in the
// support inbox and notify whoever is on duty.
//
// This exists because the alternative is polling, and polling is the wrong
// shape for this one event. trfbacksupport sweeps every org every couple of
// minutes to find broken chats, which is right for a conclusion drawn from
// telemetry — but a person pressing a button is not a conclusion, it is an
// interruption, and making them wait out a sweep interval to be NOTICED defeats
// the point of the button. The sweep still picks these up; it is the backstop,
// not the path.
//
// Publishing is best-effort fire-and-forget, like every other event here. The
// customer's request is already committed to the tenant database before this is
// sent, so a dropped event costs latency, never the request.
type AssistRequested struct {
	EventID   string `json:"event_id"` // uuid — idempotency key
	OrgID     string `json:"org_id"`
	OrgName   string `json:"org_name"`
	OrgSlug   string `json:"org_slug"`
	UserID    string `json:"user_id"`    // account uuid of whoever asked
	SessionID string `json:"session_id"` // the AI session they asked from
	// Goal is the conversation's opening message, truncated. Enough for an
	// operator to tell a password reset from a year-end close before opening
	// anything — deliberately not the transcript, which is customer content and
	// belongs behind the console's access log.
	Goal string `json:"goal"`
	// Language is the conversation's language, so whoever is notified knows
	// which of them can actually answer.
	Language    string `json:"language"`
	RequestedAt string `json:"requested_at"` // RFC3339
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
	if err := ensureStream(js, StreamErrors, []string{SubjectErrorEvent}, ErrorStreamMaxAge); err != nil {
		return err
	}
	return ensureStream(js, StreamSupport, []string{SubjectAssistRequested}, SupportStreamMaxAge)
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
