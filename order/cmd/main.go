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

	orderv1 "github.com/IoganVIII/starShip/shared/pkg/openapi/order/v1"
)

const (
	httpPort = "8080"
	// Таймауты для HTTP-сервера
	readHeaderTimeout = 5 * time.Second
	shutdownTimeout   = 10 * time.Second
)

// OrdersStorage Потокобезопасное хранилище для заказов.
type OrdersStorage struct {
	mu sync.RWMutex
	// TODO - Не нравится как называется модель, можно ли как-то переделать?
	// Почему так важно возрат объекта по ссылке?
	orders map[string]*orderv1.GetOrderResponse
}

// NewOrderStorage создает новое хранилище заказов.
func NewOrderStorage() *OrdersStorage {
	return &OrdersStorage{
		orders: make(map[string]*orderv1.GetOrderResponse),
	}
}

// OrdersHandler реализует интерфейс ordersV1.Handler для обработки запросов заказов.
type OrdersHandler struct {
	storage *OrdersStorage
}

// NewOrdersHandler создает новый обработчик заказов.
func NewOrdersHandler(storage *OrdersStorage) *OrdersHandler {
	return &OrdersHandler{
		storage: storage,
	}
}

func (h *OrdersHandler) CreateOrder(
	ctx context.Context, req *orderv1.CreateOrderRequest,
) (orderv1.CreateOrderRes, error) {
	fmt.Println("Создание детали")
	return nil, nil
}

func (h *OrdersHandler) GetOrderInfo(
	ctx context.Context, params orderv1.GetOrderInfoParams,
) (orderv1.GetOrderInfoRes, error) {
	fmt.Println("Получение информации")
	return nil, nil
}

func (h *OrdersHandler) OrderCancel(
	ctx context.Context, params orderv1.OrderCancelParams,
) (orderv1.OrderCancelRes, error) {
	fmt.Println("Отмена заказа")
	return nil, nil
}

func (h *OrdersHandler) OrderPay(
	ctx context.Context, req *orderv1.PayOrderRequest, params orderv1.OrderPayParams,
) (orderv1.OrderPayRes, error) {
	fmt.Println("Заказ оплачен")
	return nil, nil
}

func (h *OrdersHandler) NewError(
	ctx context.Context, err error,
) *orderv1.GenericErrorStatusCode {
	return nil
}

func main() {
	storage := NewOrderStorage()

	ordersHandler := NewOrdersHandler(storage)

	ordersServer, err := orderv1.NewServer(ordersHandler)
	if err != nil {
		log.Fatalf("Ошибка создания сервера order: %v", err)
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
	defer cancel()

	err = server.Shutdown(ctx)
	if err != nil {
		log.Printf("❌ Ошибка при остановке сервера: %v\n", err)
	}

	log.Println("✅ Сервер остановлен")
}
