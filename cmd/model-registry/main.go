// Command model-registry runs the model-registry service.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pandops/flowai-model-registry/internal/app"
	"github.com/pandops/flowai-model-registry/internal/broker"
	"github.com/pandops/flowai-model-registry/internal/config"
	"github.com/pandops/flowai-model-registry/internal/store/postgres"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "model-registry")
	if err := run(logger); err != nil {
		logger.Error("model-registry stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	path := flag.String("config", "/etc/flowai/config.yaml", "configuration file")
	flag.Parse()
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := postgres.Open(ctx, cfg.DSN(), cfg.DB.Schema)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return err
	}
	nc, err := broker.Connect(cfg.NATS, "model-registry", logger)
	if err != nil {
		return err
	}
	defer nc.Close()

	a, err := app.New(app.Deps{Config: cfg, Store: st, Broker: nc, Logger: logger})
	if err != nil {
		return err
	}
	if err := a.Start(ctx); err != nil {
		return err
	}
	srv := &http.Server{Addr: cfg.HTTPListen, Handler: a.Handler(), ReadHeaderTimeout: 10 * time.Second}
	errs := make(chan error, 2)
	go func() { errs <- srv.ListenAndServe() }()
	go func() { errs <- a.Run(ctx) }()
	logger.Info("model-registry started", "listen", cfg.HTTPListen)

	select {
	case <-ctx.Done():
	case err = <-errs:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	if err == nil || errors.Is(err, http.ErrServerClosed) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
