package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/pandops/flowai-model-registry/internal/config"
)

// QueueGroup is the queue group every model-registry instance joins.
const QueueGroup = "model-registry"

// NATS is the production broker over a NATS connection with JetStream.
type NATS struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	log    *slog.Logger
	mu     sync.Mutex
	subs   []*nats.Subscription
	cons   []jetstream.ConsumeContext
	reqMax time.Duration
}

// Connect opens the TLS connection with the service's user JWT and seed. It
// keeps reconnecting for the life of the process.
func Connect(cfg config.NATS, identity string, logger *slog.Logger) (*NATS, error) {
	pem, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("nats CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("nats CA holds no certificate")
	}
	nc, err := nats.Connect(cfg.URL,
		nats.Name(identity),
		nats.Secure(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}),
		nats.UserCredentials(cfg.CredentialsFile),
		nats.CustomInboxPrefix(cfg.InboxPrefix),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { logger.Warn("broker disconnected", "error", err) }),
		nats.ReconnectHandler(func(*nats.Conn) { logger.Info("broker reconnected") }),
	)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &NATS{nc: nc, js: js, log: logger, reqMax: cfg.RequestTimeout}, nil
}

func headerMap(h nats.Header) map[string]string {
	out := map[string]string{}
	for k := range h {
		out[k] = h.Get(k)
	}
	return out
}

// Serve answers requests on subject in the service's queue group.
func (n *NATS) Serve(subject string, h Handler) error {
	sub, err := n.nc.QueueSubscribe(subject, QueueGroup, func(m *nats.Msg) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		r := h(ctx, Message{Subject: m.Subject, Header: headerMap(m.Header), Data: m.Data})
		out := nats.NewMsg(m.Reply)
		for k, v := range r.Header {
			out.Header.Set(k, v)
		}
		out.Header.Set("status", strconv.Itoa(r.Status))
		out.Data = r.Body
		if err := m.RespondMsg(out); err != nil {
			n.log.Warn("reply not sent", "subject", m.Subject, "error", err)
		}
	})
	if err != nil {
		return err
	}
	n.mu.Lock()
	n.subs = append(n.subs, sub)
	n.mu.Unlock()
	return nil
}

// Request sends a request and waits for the reply.
func (n *NATS) Request(ctx context.Context, m Message, timeout time.Duration) (Reply, error) {
	if !n.nc.IsConnected() {
		return Reply{}, ErrDisconnected
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	msg := nats.NewMsg(m.Subject)
	for k, v := range m.Header {
		msg.Header.Set(k, v)
	}
	msg.Data = m.Data
	resp, err := n.nc.RequestMsgWithContext(ctx, msg)
	switch {
	case errors.Is(err, nats.ErrNoResponders):
		return Reply{}, ErrNoResponder
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout):
		return Reply{}, ErrTimeout
	case err != nil:
		return Reply{}, ErrDisconnected
	}
	status, err := strconv.Atoi(resp.Header.Get("status"))
	if err != nil {
		return Reply{}, fmt.Errorf("reply on %s has no status header", m.Subject)
	}
	return Reply{Status: status, Header: headerMap(resp.Header), Body: resp.Data}, nil
}

// Publish publishes a stream message and waits for the JetStream ack.
func (n *NATS) Publish(ctx context.Context, m Message) error {
	if !n.nc.IsConnected() {
		return ErrDisconnected
	}
	msg := nats.NewMsg(m.Subject)
	for k, v := range m.Header {
		msg.Header.Set(k, v)
	}
	msg.Data = m.Data
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := n.js.PublishMsg(ctx, msg)
	return err
}

type delivery struct{ m jetstream.Msg }

func (d delivery) Message() Message {
	return Message{Subject: d.m.Subject(), Header: headerMap(d.m.Headers()), Data: d.m.Data()}
}
func (d delivery) Ack() error  { return d.m.Ack() }
func (d delivery) Nak() error  { return d.m.NakWithDelay(time.Second) }
func (d delivery) Term() error { return d.m.Term() }

// Consume binds to the deployment-created durable consumer and delivers its
// messages to h until ctx ends.
func (n *NATS) Consume(ctx context.Context, stream, durable string, h func(context.Context, Delivery)) error {
	if !n.nc.IsConnected() {
		return ErrDisconnected
	}
	c, err := n.js.Consumer(ctx, stream, durable)
	if err != nil {
		return err
	}
	cc, err := c.Consume(func(m jetstream.Msg) { h(ctx, delivery{m}) })
	if err != nil {
		return err
	}
	n.mu.Lock()
	n.cons = append(n.cons, cc)
	n.mu.Unlock()
	go func() {
		<-ctx.Done()
		cc.Stop()
	}()
	return nil
}

// Connected reports whether the connection is up.
func (n *NATS) Connected() bool { return n.nc.IsConnected() }

// Close drains subscriptions and closes the connection.
func (n *NATS) Close() {
	n.mu.Lock()
	for _, c := range n.cons {
		c.Stop()
	}
	n.mu.Unlock()
	_ = n.nc.Drain()
}
