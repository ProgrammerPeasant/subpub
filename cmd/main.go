package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"subpub/internal/config"
	grpcserver "subpub/internal/server"
	"subpub/internal/service/subpub"
)

func main() {
	cfg := config.Load()

	log := setupLogger(cfg.Log.Level)
	log.Info("starting pubsub service", slog.String("env", cfg.Env))

	sp := subpub.NewSubPub()
	log.Debug("subpub instance created")

	// инверсия зависимостей
	grpcSrv := grpcserver.NewServer(
		sp,
		log,
		cfg.GRPCServer.MaxConnectionIdle,
		cfg.GRPCServer.Timeout,
	)
	log.Info("gRPC server configured", slog.String("port", cfg.GRPCServer.Port))

	lis, err := net.Listen("tcp", cfg.GRPCServer.Port)
	if err != nil {
		log.Error("failed to listen", slog.Any("error", err))
		os.Exit(1)
	}

	go func() {
		log.Info("starting gRPC server")
		if err := grpcSrv.GRPCServer().Serve(lis); err != nil {
			log.Error("gRPC server failed", slog.Any("error", err))
		}
	}()

	// graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop // жду сигнала останвки

	log.Info("shutdown signal received, initiating graceful shutdown")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.GRPCServer.TimeoutGraceful)
	defer cancel()

	grpcSrv.GRPCServer().GracefulStop()
	log.Info("gRPC server stopped gracefully")

	// закрываю suppub после остановки сервера
	log.Info("closing subpub instance")
	if err := sp.Close(shutdownCtx); err != nil {
		log.Error("subpub close error", slog.Any("error", err))
	} else {
		log.Info("subpub closed gracefully")
	}

	log.Info("service shut down completed")
}

func setupLogger(level string) *slog.Logger {
	var logLevel slog.Level
	switch level {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: logLevel,
	}

	handler := slog.NewJSONHandler(os.Stdout, opts)
	logger := slog.New(handler)
	return logger
}
