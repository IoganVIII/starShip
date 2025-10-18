package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	orderV1 "github.com/IoganVIII/starShip/shared/pkg/openapi/order/v1"
	inventoryV1 "github.com/IoganVIII/starShip/shared/pkg/proto/inventory/v1"
	paymentV1 "github.com/IoganVIII/starShip/shared/pkg/proto/payment/v1"
)

const (
	httpPort = "8080"
	// Таймауты для HTTP-сервера
	readHeaderTimeout = 5 * time.Second
	shutdownTimeout   = 10 * time.Second

	InventoryServiceAddress = "localhost:50051"
	PaymentServiceAddress   = "localhost:50052"
)

// OrderStorage хранилище для заказов.
type OrderStorage struct {
	mu     sync.RWMutex
	orders map[string]*orderV1.OrderDto
}

// NewOrderStorage создает новое хранилище заказов.
func NewOrderStorage() *OrderStorage {
	return &OrderStorage{
		orders: make(map[string]*orderV1.OrderDto),
	}
}

// Set добавить элемент в стор.
func (store *OrderStorage) Set(key string, value *orderV1.OrderDto) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.orders[key] = value
}

// Get получить элемент из стора.
func (store *OrderStorage) Get(key string) (*orderV1.OrderDto, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	value, ok := store.orders[key]
	return value, ok
}

// OrderService реализует интерфейс ordersV1.Handler для обработки запросов заказов.
type OrderService struct {
	storage *OrderStorage

	inventoryServiceClient inventoryV1.InventoryServiceClient
	paymentServiceClient   paymentV1.PaymentServiceClient
}

// NewOrderService создает новый обработчик заказов.
func NewOrderService(
	storage *OrderStorage,
	inventoryServiceClient *inventoryV1.InventoryServiceClient,
	paymentServiceClient *paymentV1.PaymentServiceClient,
) *OrderService {
	return &OrderService{
		storage:                storage,
		inventoryServiceClient: *inventoryServiceClient,
		paymentServiceClient:   *paymentServiceClient,
	}
}

// CreateOrder создать заказ.
func (s *OrderService) CreateOrder(
	ctx context.Context, req *orderV1.CreateOrderRequest,
) (orderV1.CreateOrderRes, error) {
	if req == nil {
		return &orderV1.BadRequestError{
			Code:    http.StatusBadRequest,
			Message: "request is empty",
		}, nil
	}

	partsUuidString := make([]string, 0, len(req.PartUuids))
	for _, partID := range req.PartUuids {
		partsUuidString = append(partsUuidString, partID.String())
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res, err := s.inventoryServiceClient.ListParts(ctx, &inventoryV1.ListPartsRequest{
		Filter: &inventoryV1.PartsFilter{
			Uuids: partsUuidString,
		},
	})
	if err != nil {
		return &orderV1.InternalServerError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		}, nil
	}
	if res == nil {
		return &orderV1.InternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "invalid argument: empty result from ListParts",
		}, nil
	}
	if len(res.Parts) != len(req.PartUuids) {
		return &orderV1.BadRequestError{
			Code:    http.StatusBadRequest,
			Message: "count part response unequal count part request",
		}, nil
	}
	var totalPrice float64 = 0
	for _, item := range res.Parts {
		totalPrice += item.Price
	}
	partResponseIDs := make([]uuid.UUID, 0, len(res.Parts))
	for _, item := range res.Parts {
		partResponseIDs = append(partResponseIDs, uuid.MustParse(item.Uuid))
	}
	orderID := uuid.New()
	s.storage.Set(orderID.String(), &orderV1.OrderDto{
		OrderUUID:  orderID,
		UserUUID:   req.UserUUID,
		PartUuids:  partResponseIDs,
		TotalPrice: float32(totalPrice),
		Status:     orderV1.OrderStatusPENDINGPAYMENT,
	})
	return &orderV1.CreateOrderResponse{
		OrderUUID:  orderID,
		TotalPrice: float32(totalPrice),
	}, nil
}

// GetOrderInfo получить информацию о заказе.
func (s *OrderService) GetOrderInfo(
	ctx context.Context, req orderV1.GetOrderInfoParams,
) (orderV1.GetOrderInfoRes, error) {
	order, ok := s.storage.Get(req.OrderUUID.String())
	if !ok {
		return &orderV1.NotFoundError{
			Code:    http.StatusNotFound,
			Message: fmt.Sprintf("order is not found by: %s", req.OrderUUID),
		}, nil
	}
	return &orderV1.OrderDto{
		OrderUUID:       order.OrderUUID,
		UserUUID:        order.UserUUID,
		PartUuids:       order.PartUuids,
		TotalPrice:      order.TotalPrice,
		TransactionUUID: order.TransactionUUID,
		PaymentMethod:   order.PaymentMethod,
		Status:          order.Status,
	}, nil
}

// OrderCancel отменить заказ.
func (s *OrderService) OrderCancel(
	ctx context.Context, req orderV1.OrderCancelParams,
) (orderV1.OrderCancelRes, error) {
	order, ok := s.storage.Get(req.OrderUUID.String())
	if !ok {
		return &orderV1.NotFoundError{
			Code:    http.StatusNotFound,
			Message: fmt.Sprintf("order is not found by: %s", req.OrderUUID.String()),
		}, nil
	}
	if order.Status == orderV1.OrderStatusPAID {
		return &orderV1.ConflictError{
			Code:    http.StatusConflict,
			Message: "order is paid",
		}, nil
	}
	if order.Status == orderV1.OrderStatusPENDINGPAYMENT {
		order.Status = orderV1.OrderStatusCANCELLED
		s.storage.Set(order.OrderUUID.String(), order)
	}

	return nil, nil
}

// OrderPay оплатить заказ.
func (s *OrderService) OrderPay(
	ctx context.Context, req *orderV1.PayOrderRequest, params orderV1.OrderPayParams,
) (orderV1.OrderPayRes, error) {
	order, ok := s.storage.Get(params.OrderUUID.String())
	if !ok {
		return &orderV1.NotFoundError{
			Code:    http.StatusNotFound,
			Message: fmt.Sprintf("order is not found by: %s", params.OrderUUID.String()),
		}, nil
	}
	if order.Status == orderV1.OrderStatusPAID || order.Status == orderV1.OrderStatusCANCELLED {
		return &orderV1.BadRequestError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("cannot be paid becouse order status: %s", params.OrderUUID.String()),
		}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	payOrderRequestPaymentMehod := orderV1PaymentMethodToPaymentV1PaymentMethod(req.PaymentMethod)
	payOrderRes, err := s.paymentServiceClient.PayOrder(ctx, &paymentV1.PayOrderRequest{
		UserUuid:      order.UserUUID.String(),
		OrderUuid:     order.OrderUUID.String(),
		PaymentMethod: payOrderRequestPaymentMehod,
	})
	if err != nil {
		return &orderV1.InternalServerError{
			Code:    http.StatusInternalServerError,
			Message: fmt.Sprintf("error invoke grpc payOrder: %s", err.Error()),
		}, nil
	}
	if payOrderRes == nil {
		return &orderV1.InternalServerError{
			Code:    http.StatusInternalServerError,
			Message: "response form grpc payOrder is emty",
		}, nil
	}

	order.Status = orderV1.OrderStatusPAID
	order.TransactionUUID.Value = uuid.MustParse(payOrderRes.TransactionUuid)
	s.storage.Set(order.OrderUUID.String(), order)
	return &orderV1.PayOrderResponse{
		OrderUUID: orderV1.NewOptUUID(order.TransactionUUID.Value),
	}, nil
}

func (s *OrderService) NewError(
	ctx context.Context, err error,
) *orderV1.GenericErrorStatusCode {
	return &orderV1.GenericErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response: orderV1.GenericError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		},
	}
}

func NewInventoryServiceClient() (*inventoryV1.InventoryServiceClient, error) {
	InventoryServiceConnection, err := grpc.NewClient(
		InventoryServiceAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect InventoryService: %w", err)
	}
	inventoryServiceClient := inventoryV1.NewInventoryServiceClient(InventoryServiceConnection)
	return &inventoryServiceClient, nil
}

func NewPaymentServiceClient() (*paymentV1.PaymentServiceClient, error) {
	PaymentServiceConnection, err := grpc.NewClient(
		PaymentServiceAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect PaymentService: %w", err)
	}
	paymentServiceClient := paymentV1.NewPaymentServiceClient(PaymentServiceConnection)
	return &paymentServiceClient, nil
}

// orderV1PaymentMethodToPaymentV1PaymentMethod конвертор поля PaymentMethod из интерфейса orderV1 в paymentV1
func orderV1PaymentMethodToPaymentV1PaymentMethod(paymentMethod orderV1.PaymentMethod) paymentV1.PaymentMethod {
	var payOrderRequestPaymentMehod paymentV1.PaymentMethod
	switch paymentMethod {
	case orderV1.PaymentMethodCARD:
		payOrderRequestPaymentMehod = paymentV1.PaymentMethod_CARD
	case orderV1.PaymentMethodCREDITCARD:
		payOrderRequestPaymentMehod = paymentV1.PaymentMethod_CREDIT_CARD
	case orderV1.PaymentMethodINVESTORMONEY:
		payOrderRequestPaymentMehod = paymentV1.PaymentMethod_INVESTOR_MONEY
	case orderV1.PaymentMethodSBP:
		payOrderRequestPaymentMehod = paymentV1.PaymentMethod_SBP
	default:
		payOrderRequestPaymentMehod = paymentV1.PaymentMethod_UNKNOWN
	}

	return payOrderRequestPaymentMehod
}

func main() {
	inventoryServiceClient, err := NewInventoryServiceClient()
	if err != nil {
		log.Fatalf("Error create inventoryServiceClient: %v", err)
		return
	}
	paymentServiceClient, err := NewPaymentServiceClient()
	if err != nil {
		log.Fatalf("Error create paymentServiceClient: %v", err)
		return
	}
	orderService := NewOrderService(
		NewOrderStorage(),
		inventoryServiceClient,
		paymentServiceClient,
	)
	ordersServer, err := orderV1.NewServer(orderService)
	if err != nil {
		log.Fatalf("Error create ordersServer: %v", err)
		return
	}

	r := chi.NewRouter()

	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(10 * time.Second))

	r.Mount("/", ordersServer)

	server := &http.Server{
		Addr:              net.JoinHostPort("localhost", httpPort),
		Handler:           r,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	go func() {
		log.Printf("🚀 HTTP-сервер запущен на порту %s\n", httpPort)
		err = server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("❌ Ошибка запуска сервера: %v\n", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("🛑 Завершение работы сервера...")

	// Создаем контекст с таймаутом для остановки сервера
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer func() {
		cancel()
	}()

	err = server.Shutdown(ctx)
	if err != nil {
		log.Printf("❌ Ошибка при остановке сервера: %v\n", err)
	}

	log.Println("✅ Сервер остановлен")
}
