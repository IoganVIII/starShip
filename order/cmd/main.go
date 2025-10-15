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

// OrdersStorage Потокобезопасное хранилище для заказов.
type OrdersStorage struct {
	mu sync.RWMutex
	// TODO - Не нравится как называется модель, можно ли как-то переделать?
	// Почему так важно возрат объекта по ссылке?
	orders map[string]*orderV1.GetOrderResponse
}

// NewOrderStorage создает новое хранилище заказов.
func NewOrderStorage() *OrdersStorage {
	return &OrdersStorage{
		orders: make(map[string]*orderV1.GetOrderResponse),
	}
}

// OrdersHandler реализует интерфейс ordersV1.Handler для обработки запросов заказов.
type OrdersHandler struct {
	storage *OrdersStorage

	inventoryServiceClient inventoryV1.InventoryServiceClient
	paymentServiceClient   paymentV1.PaymentServiceClient
}

// NewOrdersHandler создает новый обработчик заказов.
func NewOrdersHandler(storage *OrdersStorage) *OrdersHandler {
	return &OrdersHandler{
		storage: storage,
	}
}

func (h *OrdersHandler) CreateOrder(
	ctx context.Context, req *orderV1.CreateOrderRequest,
) (orderV1.CreateOrderRes, error) {
	partsUuidString := make([]string, 0, len(req.PartUuids))
	for _, partID := range req.PartUuids {
		partsUuidString = append(partsUuidString, partID.String())
	}
	res, err := h.inventoryServiceClient.ListParts(ctx, &inventoryV1.ListPartsRequest{
		Filter: &inventoryV1.PartsFilter{
			Uuids: partsUuidString,
		},
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, errors.New("invalid argument: empty result from ListParts")
	}
	if len(res.Parts) != len(req.PartUuids) {
		return nil, errors.New("count part response unequal count part request")
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
	h.storage.mu.Lock()
	h.storage.orders[orderID.String()] = &orderV1.GetOrderResponse{
		OrderUUID:       orderID,
		UserUUID:        req.UserUUID,
		PartUuids:       partResponseIDs,
		TotalPrice:      float32(totalPrice),
		TransactionUUID: uuid.Nil,
		PaymentMethod:   orderV1.PaymentMethodUNKNOWN,
		Status:          orderV1.OrderStatusPENDINGPAYMENT,
	}
	h.storage.mu.Unlock()
	return &orderV1.CreateOrderResponse{
		OrderUUID:  orderID,
		TotalPrice: float32(totalPrice),
	}, nil
}

func (h *OrdersHandler) GetOrderInfo(
	ctx context.Context, req orderV1.GetOrderInfoParams,
) (orderV1.GetOrderInfoRes, error) {
	h.storage.mu.Lock()
	order, ok := h.storage.orders[req.OrderUUID.String()]
	if !ok {
		return nil, errors.New("order is not found by: " + req.OrderUUID.String())
	}
	return &orderV1.GetOrderResponse{
		OrderUUID:       order.OrderUUID,
		UserUUID:        order.UserUUID,
		PartUuids:       order.PartUuids,
		TotalPrice:      order.TotalPrice,
		TransactionUUID: order.TransactionUUID,
		PaymentMethod:   orderV1.PaymentMethodCARD,
		Status:          orderV1.OrderStatusPAID,
	}, nil
}

func (h *OrdersHandler) OrderCancel(
	ctx context.Context, req orderV1.OrderCancelParams,
) (orderV1.OrderCancelRes, error) {
	h.storage.mu.Lock()
	defer h.storage.mu.Unlock()

	order, ok := h.storage.orders[req.OrderUUID.String()]
	if !ok {
		return &orderV1.NotFoundError{
			Code:    404,
			Message: fmt.Sprintf("order is not found by: %s", req.OrderUUID.String()),
		}, nil
	}
	if order.Status == orderV1.OrderStatusPAID {
		return &orderV1.ConflictError{
			Code:    409,
			Message: fmt.Sprint("order is paid"),
		}, nil
	}
	if order.Status == orderV1.OrderStatusPENDINGPAYMENT {
		order.Status = orderV1.OrderStatusCANCELLED
		h.storage.orders[order.OrderUUID.String()] = order
	}

	return nil, nil
}

func (h *OrdersHandler) OrderPay(
	ctx context.Context, req *orderV1.PayOrderRequest, params orderV1.OrderPayParams,
) (orderV1.OrderPayRes, error) {
	h.storage.mu.Lock()
	defer h.storage.mu.Unlock()

	order, ok := h.storage.orders[params.OrderUUID.String()]
	if !ok {
		return &orderV1.NotFoundError{
			Code:    404,
			Message: fmt.Sprintf("order is not found by: %s", params.OrderUUID.String()),
		}, nil
	}
	var payOrderRequestPaymentMehod paymentV1.PaymentMethod
	switch req.PaymentMethod {
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
	payOrderRes, err := h.paymentServiceClient.PayOrder(ctx, &paymentV1.PayOrderRequest{
		UserUuid:      order.UserUUID.String(),
		OrderUuid:     order.OrderUUID.String(),
		PaymentMethod: payOrderRequestPaymentMehod,
	})
	if err != nil {
		return nil, errors.New("123")
	}
	if payOrderRes == nil {
		return nil, errors.New("1234")
	}

	order.Status = orderV1.OrderStatusPAID
	order.TransactionUUID = uuid.MustParse(payOrderRes.TransactionUuid)
	h.storage.orders[order.OrderUUID.String()] = order
	return nil, nil
}

func (h *OrdersHandler) NewError(
	ctx context.Context, err error,
) *orderV1.GenericErrorStatusCode {
	return nil
}

func main() {
	storage := NewOrderStorage()
	ordersHandler := NewOrdersHandler(storage)
	ordersServer, err := orderV1.NewServer(ordersHandler)
	if err != nil {
		log.Fatalf("Ошибка создания сервера order: %v", err)
	}

	InventoryServiceConnection, err := grpc.NewClient(
		InventoryServiceAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Printf("failed to connect InventoryService: %v\n", err)
		return
	}
	ordersHandler.inventoryServiceClient = inventoryV1.NewInventoryServiceClient(InventoryServiceConnection)

	PaymentServiceConnection, err := grpc.NewClient(
		PaymentServiceAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Printf("failed to connect InventoryService: %v\n", err)
		return
	}
	ordersHandler.paymentServiceClient = paymentV1.NewPaymentServiceClient(PaymentServiceConnection)

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
		if cerr := InventoryServiceConnection.Close(); cerr != nil {
			log.Printf("failed to close connect InventoryService: %v", cerr)
		}

		cancel()
	}()

	err = server.Shutdown(ctx)
	if err != nil {
		log.Printf("❌ Ошибка при остановке сервера: %v\n", err)
	}

	log.Println("✅ Сервер остановлен")
}
