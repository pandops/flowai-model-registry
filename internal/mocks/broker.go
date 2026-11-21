package mocks

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pandops/flowai-model-registry/internal/broker"
)

// Broker is an in-process fake of NATS. A request on a ".pub" subject is
// routed to the responder of the same subject ending in ".sub", as the
// deployment's account subject mappings do. Stream publishes are recorded;
// stream deliveries are pushed by the test with Deliver.
type Broker struct {
	mu         sync.Mutex
	responders map[string]broker.Handler
	silent     map[string]bool // subjects that never answer (timeout)
	published  []broker.Message
	requests   []broker.Message
	consumers  map[string]func(context.Context, broker.Delivery)
}

// NewBroker returns a connected fake broker.
func NewBroker() *Broker {
	return &Broker{responders: map[string]broker.Handler{}, silent: map[string]bool{}, consumers: map[string]func(context.Context, broker.Delivery){}}
}

// Respond registers a fake responder for a ".sub" subject pattern.
func (b *Broker) Respond(subject string, h broker.Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.responders[subject] = h
}

// Unrespond removes a responder, so requests get no responder.
func (b *Broker) Unrespond(subject string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.responders, subject)
}

// Silence makes a ".sub" subject swallow requests until the caller's
// timeout (the owner is stuck); silent=false restores it.
func (b *Broker) Silence(subject string, silent bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.silent[subject] = silent
}

// Published returns the acknowledged stream messages in publish order.
func (b *Broker) Published() []broker.Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.published)
}

// PublishedOn returns acknowledged stream messages whose subject starts with prefix.
func (b *Broker) PublishedOn(prefix string) []broker.Message {
	var out []broker.Message
	for _, m := range b.Published() {
		if strings.HasPrefix(m.Subject, prefix) {
			out = append(out, m)
		}
	}
	return out
}

// Requests returns every request the service sent, in order.
func (b *Broker) Requests() []broker.Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.requests)
}

// RequestsTo returns the requests sent on subjects starting with prefix.
func (b *Broker) RequestsTo(prefix string) []broker.Message {
	var out []broker.Message
	for _, m := range b.Requests() {
		if strings.HasPrefix(m.Subject, prefix) {
			out = append(out, m)
		}
	}
	return out
}

// ResetRecords clears recorded publishes and requests.
func (b *Broker) ResetRecords() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.published = nil
	b.requests = nil
}

// Serve registers the service's own responder.
func (b *Broker) Serve(subject string, h broker.Handler) error {
	if !strings.HasSuffix(subject, ".sub") {
		return errors.New("fake broker: services serve .sub subjects only")
	}
	b.Respond(subject, h)
	return nil
}

func toSub(subject string) string {
	if strings.HasSuffix(subject, ".pub") {
		return strings.TrimSuffix(subject, ".pub") + ".sub"
	}
	return ""
}

// Match reports whether a NATS subject matches a pattern with * and >.
func Match(pattern, subject string) bool {
	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")
	for i, tok := range p {
		if tok == ">" {
			return len(s) > i
		}
		if i >= len(s) || (tok != "*" && tok != s[i]) {
			return false
		}
	}
	return len(p) == len(s)
}

func cloneMsg(m broker.Message) broker.Message {
	return broker.Message{Subject: m.Subject, Header: maps.Clone(m.Header), Data: slices.Clone(m.Data)}
}

// Request routes a ".pub" request to the matching ".sub" responder.
func (b *Broker) Request(ctx context.Context, m broker.Message, timeout time.Duration) (broker.Reply, error) {
	b.mu.Lock()
	b.requests = append(b.requests, cloneMsg(m))
	sub := toSub(m.Subject)
	if sub == "" {
		b.mu.Unlock()
		return broker.Reply{}, errors.New("fake broker: requests are published on .pub subjects")
	}
	var handler broker.Handler
	silent := false
	for pattern, h := range b.responders {
		if Match(pattern, sub) {
			handler = h
			silent = b.silent[pattern]
			break
		}
	}
	b.mu.Unlock()
	if handler == nil {
		return broker.Reply{}, broker.ErrNoResponder
	}
	if silent {
		if timeout > 50*time.Millisecond {
			timeout = 50 * time.Millisecond
		}
		select {
		case <-ctx.Done():
		case <-time.After(timeout):
		}
		return broker.Reply{}, broker.ErrTimeout
	}
	in := cloneMsg(m)
	in.Subject = sub
	reply := handler(ctx, in)
	return reply, nil
}

// Publish records a stream message as acknowledged by JetStream.
func (b *Broker) Publish(_ context.Context, m broker.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !strings.HasSuffix(m.Subject, ".pub") {
		return errors.New("fake broker: publishers publish .pub subjects only")
	}
	// JetStream duplicate window: the same Nats-Msg-Id is stored once.
	if id := m.Header["Nats-Msg-Id"]; id != "" {
		for _, p := range b.published {
			if p.Header["Nats-Msg-Id"] == id {
				return nil
			}
		}
	}
	b.published = append(b.published, cloneMsg(m))
	return nil
}

// Consume registers the durable consumer's handler.
func (b *Broker) Consume(_ context.Context, stream, durable string, h func(context.Context, broker.Delivery)) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consumers[stream+"/"+durable] = h
	return nil
}

// HasConsumer reports whether the service bound the durable consumer.
func (b *Broker) HasConsumer(stream, durable string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.consumers[stream+"/"+durable]
	return ok
}

// Connected reports the fake connection state: always connected.
func (b *Broker) Connected() bool { return true }

// Close is a no-op.
func (b *Broker) Close() {}

// Delivery is one fake stream delivery and the decision the consumer made.
type Delivery struct {
	msg     broker.Message
	mu      sync.Mutex
	outcome string // "", "ack", "nak", "term"
}

// Message returns the delivered message.
func (d *Delivery) Message() broker.Message { return d.msg }

// Ack acknowledges the message.
func (d *Delivery) Ack() error { return d.set("ack") }

// Nak asks for redelivery.
func (d *Delivery) Nak() error { return d.set("nak") }

// Term terminates the message.
func (d *Delivery) Term() error { return d.set("term") }

func (d *Delivery) set(v string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.outcome == "" {
		d.outcome = v
	}
	return nil
}

// Outcome is "ack", "nak", "term" or "" when the consumer decided nothing.
func (d *Delivery) Outcome() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.outcome
}

// Deliver pushes a message on a ".sub" subject to the durable consumer and
// returns the delivery after the handler finished.
func (b *Broker) Deliver(ctx context.Context, stream, durable string, m broker.Message) (*Delivery, error) {
	b.mu.Lock()
	h, ok := b.consumers[stream+"/"+durable]
	b.mu.Unlock()
	if !ok {
		return nil, errors.New("fake broker: no consumer " + stream + "/" + durable)
	}
	d := &Delivery{msg: cloneMsg(m)}
	h(ctx, d)
	return d, nil
}
