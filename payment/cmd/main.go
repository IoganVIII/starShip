package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	paymentV1 "github.com/IoganVIII/starShip/shared/pkg/proto/payment/v1"
)

// grpcPort порт на котором запускается сервис.
const grpcPort = 50052

// PaymentService сервия для оплаты заказов.
type PaymentService struct {
	paymentV1.UnimplementedPaymentServiceServer
}

// PayOrder метод оплаты заказа.
func (s *PaymentService) PayOrder(ctx context.Context, req *paymentV1.PayOrderRequest) (*paymentV1.PayOrderResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is empty")
	}
	_, err := uuid.Parse(req.OrderUuid)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	_, err = uuid.Parse(req.UserUuid)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	orderID := uuid.New()
	log.Printf("pay order complete, transaction_uuid: %s", orderID)

	return &paymentV1.PayOrderResponse{
		TransactionUuid: orderID.String(),
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Printf("failed to listen: %v\n", err)
		return
	}
	defer func() {
		if cerr := lis.Close(); cerr != nil {
			log.Printf("failed to close listener: %v\n", cerr)
		}
	}()

	s := grpc.NewServer()
	service := &PaymentService{}
	paymentV1.RegisterPaymentServiceServer(s, service)
	reflection.Register(s)

	go func() {
		log.Printf("🚀 gRPC PaymentService listening on %d\n", grpcPort)
		err = s.Serve(lis)
		if err != nil {
			log.Printf("failed to PaymentService: %v\n", err)
			return
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("🛑 Shutting down gRPC PaymentService...")
	s.GracefulStop()
	log.Println("✅ PaymentService stopped")
}
