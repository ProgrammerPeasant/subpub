package grpcserver

import (
	"context"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "subpub/api/proto"
	"subpub/internal/service/subpub"
)

type Server struct {
	pb.UnimplementedPubSubServer
	subpub     subpub.SubPub
	log        *slog.Logger
	grpcServer *grpc.Server
}

func NewServer(sp subpub.SubPub, log *slog.Logger, maxConnectionIdle time.Duration, timeout time.Duration) *Server {
	kp := keepalive.ServerParameters{
		MaxConnectionIdle: maxConnectionIdle,
		Timeout:           timeout,
		Time:              timeout / 2,
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(kp),
	)

	s := &Server{
		subpub:     sp,
		log:        log,
		grpcServer: grpcServer,
	}

	pb.RegisterPubSubServer(grpcServer, s)
	return s
}

func (s *Server) GRPCServer() *grpc.Server {
	return s.grpcServer
}

func (s *Server) Publish(_ context.Context, req *pb.PublishRequest) (*emptypb.Empty, error) {
	const op = "grpcserver.Publish"
	log := s.log.With(slog.String("op", op), slog.String("key", req.Key))

	if req.Key == "" {
		log.Warn("invalid argument: empty key")
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}
	if req.Data == "" {
		log.Debug("publishing empty data")
	}

	log.Info("publishing message")

	err := s.subpub.Publish(req.Key, req.Data)
	if err != nil {
		if err == subpub.ErrClosed {
			log.Warn("publish failed: service is closed")
			return nil, status.Error(codes.Unavailable, "service is shutting down")
		}
		log.Error("failed to publish message", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to publish message")
	}

	log.Info("message published successfully")
	return &emptypb.Empty{}, nil
}

func (s *Server) Subscribe(req *pb.SubscribeRequest, stream pb.PubSub_SubscribeServer) error {
	const op = "grpcserver.Subscribe"
	log := s.log.With(slog.String("op", op), slog.String("key", req.Key))

	if req.Key == "" {
		log.Warn("invalid argument: empty key")
		return status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	ctx := stream.Context()
	msgChan := make(chan interface{}, 16)

	handler := func(msg interface{}) {
		data, ok := msg.(string)
		if !ok {
			log.Error("received non-string message from subpub", slog.Any("type", msg))
			return
		}

		select {
		case msgChan <- data:
			log.Debug("queued event for sending")
		case <-ctx.Done():
			log.Info("context cancelled while q event, subscriber disconnected (perhaps)")
			return
		}
	}

	log.Info("subscribing client")

	sub, err := s.subpub.Subscribe(req.Key, handler)
	if err != nil {
		if err == subpub.ErrClosed {
			log.Warn("subscribe failed: service is closed")
			return status.Error(codes.Unavailable, "service is shutting down")
		}
		log.Error("failed to subscribe", slog.Any("error", err))
		return status.Error(codes.Internal, "failed to subscribe")
	}
	defer sub.Unsubscribe()
	log.Info("client subscribed successfully")

	for {
		select {
		case <-ctx.Done():
			log.Info("client disconnected", slog.Any("error", ctx.Err()))
			if ctx.Err() == context.Canceled {
				return status.Error(codes.Canceled, "client cancelled the subscription")
			}
			return status.Errorf(codes.Unknown, "stream context ended: %v", ctx.Err())

		case data := <-msgChan:
			strData, ok := data.(string)
			if !ok {
				// воообще такое не должно происходить, так как я проверяю в обработчике, но в целом хорошая практика
				log.Error("dequeued non-string message, skipping", slog.Any("type", data))
				continue
			}

			event := &pb.Event{Data: strData}
			log.Debug("sending event to client")
			if err := stream.Send(event); err != nil {
				log.Error("failed to send event to client", slog.Any("error", err))
				if err == io.EOF || status.Code(err) == codes.Unavailable || status.Code(err) == codes.Canceled {
					log.Info("stream closed by client or network error during send")
					return status.Error(codes.Unavailable, "stream closed during send")
				}
				return status.Errorf(codes.Internal, "failed to send event: %v", err)
			}
			log.Debug("event sent successfully", slog.String("data", strData))
		}
	}
}
