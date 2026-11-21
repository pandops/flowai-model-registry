// Package broker is model-registry's boundary to NATS: core request/reply
// for the operations it serves and calls, JetStream publishing for its outbox
// and one durable consumer on ALL_EVENT. The NATS implementation is in
// nats.go; unit tests use the in-process fake in internal/mocks.
package broker

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNoResponder means no service answers the subject.
	ErrNoResponder = errors.New("broker: no responder")
	// ErrTimeout means the request got no reply within its timeout.
	ErrTimeout = errors.New("broker: request timeout")
	// ErrDisconnected means the service's own broker connection is down.
	ErrDisconnected = errors.New("broker: disconnected")
)

// Message is one request or stream message.
type Message struct {
	Subject string
	Header  map[string]string
	Data    []byte
}

// Reply is the answer to a request: the status travels in the NATS header
// "status" (agents.md rule 9).
type Reply struct {
	Status int
	Header map[string]string
	Body   []byte
}

// Handler answers one request.
type Handler func(ctx context.Context, m Message) Reply

// Delivery is one stream message delivered to a durable consumer.
type Delivery interface {
	Message() Message
	Ack() error
	Nak() error
	Term() error
}

// Broker is the subset of NATS model-registry uses.
type Broker interface {
	// Serve answers requests on a ".sub" subject (wildcards allowed).
	Serve(subject string, h Handler) error
	// Request sends a request on a ".pub" subject and waits for the reply.
	Request(ctx context.Context, m Message, timeout time.Duration) (Reply, error)
	// Publish publishes a stream message and waits for the JetStream ack.
	Publish(ctx context.Context, m Message) error
	// Consume binds to an existing durable consumer and delivers messages.
	Consume(ctx context.Context, stream, durable string, h func(context.Context, Delivery)) error
	Connected() bool
	Close()
}
