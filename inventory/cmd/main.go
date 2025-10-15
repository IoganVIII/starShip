package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/brianvoe/gofakeit"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	inventoryV1 "github.com/IoganVIII/starShip/shared/pkg/proto/inventory/v1"
)

// grpcPort порт на котором запускается сервис.
const grpcPort = 50051

// InventoryService сервис для работы с запчастями кораблей.
type InventoryService struct {
	inventoryV1.UnimplementedInventoryServiceServer

	mu      sync.RWMutex
	storage *Storage
}

// NewInventoryService конструктор сервиса InventoryService.
func NewInventoryService() *InventoryService {
	inventoryService := &InventoryService{
		storage: NewStorage(),
	}
	inventoryService.initParts()

	return inventoryService
}

// Storage потокобезопасный стор для хранения запчастей.
type Storage struct {
	mu    sync.RWMutex
	parts map[string]*inventoryV1.Part
}

// NewStorage конструктор потокобезопасного стора.
func NewStorage() *Storage {
	return &Storage{
		parts: make(map[string]*inventoryV1.Part),
	}
}

// Set добавить элемент в стор.
func (store *Storage) Set(key string, value *inventoryV1.Part) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.parts[key] = value
}

// Get получить элемент из стора.
func (store *Storage) Get(key string) (*inventoryV1.Part, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value, ok := store.parts[key]
	return value, ok
}

// GetPart получить запчасть под идентификатору.
func (s *InventoryService) GetPart(ctx context.Context, req *inventoryV1.GetPartRequest) (*inventoryV1.GetPartResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is empty")
	}
	partID, err := uuid.Parse(req.Uuid)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid argument \"partID\": %s", partID)
	}

	part, ok := s.storage.Get(partID.String())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "not found part by ID: %s", partID)
	}

	return &inventoryV1.GetPartResponse{
		Part: part,
	}, nil
}

// ListParts получить список запчастей по фильтру.
func (s *InventoryService) ListParts(ctx context.Context, req *inventoryV1.ListPartsRequest) (*inventoryV1.ListPartsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is empty")
	}

	partListResponse := make([]*inventoryV1.Part, 0)
	if req.Filter == nil {
		partListResponse = make([]*inventoryV1.Part, 0, len(s.storage.parts))
		for _, part := range s.storage.parts {
			partListResponse = append(partListResponse, part)
		}

		return &inventoryV1.ListPartsResponse{
			Parts: partListResponse,
		}, nil
	}

	for partID, part := range s.storage.parts {
		if req.Filter.Uuids != nil && !containsUUID(req.Filter.Uuids, partID) {
			continue
		}
		if req.Filter.Names != nil && !containsName(req.Filter.Names, part.Name) {
			continue
		}
		if req.Filter.Categories != nil && !containsCategories(req.Filter.Categories, part.Category) {
			continue
		}
		if req.Filter.ManufacturerCountries != nil && !containsManufacturerCountries(req.Filter.ManufacturerCountries, part.Manufacturer.Country) {
			continue
		}
		if req.Filter.Tags != nil && !containsTags(req.Filter.Tags, part.Tags) {
			continue
		}

		partListResponse = append(partListResponse, part)
	}
	return &inventoryV1.ListPartsResponse{
		Parts: partListResponse,
	}, nil
}

// initParts инициализирует стор запчастей.
func (s *InventoryService) initParts() {
	parts := generateParts()

	for _, part := range parts {
		s.storage.Set(part.Uuid, part)
	}
}

func generateParts() []*inventoryV1.Part {
	names := []string{
		"Main Engine",
		"Reserve Engine",
		"Thruster",
		"Fuel Tank",
		"Left Wing",
		"Right Wing",
		"Window A",
		"Window B",
		"Control Module",
		"Stabilizer",
	}

	descriptions := []string{
		"Primary propulsion unit",
		"Backup propulsion unit",
		"Thruster for fine adjustments",
		"Main fuel tank",
		"Left aerodynamic wing",
		"Right aerodynamic wing",
		"Front viewing window",
		"Side viewing window",
		"Flight control module",
		"Stabilization fin",
	}

	var parts []*inventoryV1.Part
	for i := 0; i < gofakeit.Number(1, 50); i++ {
		idx := gofakeit.Number(0, len(names)-1)
		parts = append(parts, &inventoryV1.Part{
			Uuid:          uuid.NewString(),
			Name:          names[idx],
			Description:   descriptions[idx],
			Price:         roundTo(gofakeit.Float64Range(100, 10_000)),
			StockQuantity: int64(gofakeit.Number(1, 100)),
			Category:      inventoryV1.Category(gofakeit.Number(1, 4)), //nolint:gosec // safe: gofakeit.Number returns 1..4
			Dimensions:    generateDimensions(),
			Manufacturer:  generateManufacturer(),
			Tags:          generateTags(),
			Metadata:      generateMetadata(),
			CreatedAt:     timestamppb.Now(),
		})
	}

	return parts
}

func generateDimensions() *inventoryV1.Dimensions {
	return &inventoryV1.Dimensions{
		Length: roundTo(gofakeit.Float64Range(1, 1000)),
		Width:  roundTo(gofakeit.Float64Range(1, 1000)),
		Height: roundTo(gofakeit.Float64Range(1, 1000)),
		Weight: roundTo(gofakeit.Float64Range(1, 1000)),
	}
}

func generateManufacturer() *inventoryV1.Manufacturer {
	return &inventoryV1.Manufacturer{
		Name:    gofakeit.Name(),
		Country: gofakeit.Country(),
		Website: gofakeit.URL(),
	}
}

func generateTags() []string {
	var tags []string
	for i := 0; i < gofakeit.Number(1, 10); i++ {
		tags = append(tags, gofakeit.Word())
	}

	return tags
}

func generateMetadata() map[string]*inventoryV1.Value {
	metadata := make(map[string]*inventoryV1.Value)

	for i := 0; i < gofakeit.Number(1, 10); i++ {
		metadata[gofakeit.Word()] = generateMetadataValue()
	}

	return metadata
}

func generateMetadataValue() *inventoryV1.Value {
	switch gofakeit.Number(0, 3) {
	case 0:
		return &inventoryV1.Value{
			Value: &inventoryV1.Value_StringValue{
				StringValue: gofakeit.Word(),
			},
		}

	case 1:
		return &inventoryV1.Value{
			Value: &inventoryV1.Value_Int64Value{
				Int64Value: int64(gofakeit.Number(1, 100)),
			},
		}

	case 2:
		return &inventoryV1.Value{
			Value: &inventoryV1.Value_DoubleValue{
				DoubleValue: roundTo(gofakeit.Float64Range(1, 100)),
			},
		}

	case 3:
		return &inventoryV1.Value{
			Value: &inventoryV1.Value_BoolValue{
				BoolValue: gofakeit.Bool(),
			},
		}

	default:
		return nil
	}
}

func roundTo(x float64) float64 {
	return math.Round(x*100) / 100
}

// Вохождение строки в массив
func containsString(source []string, target string) bool {
	for _, s := range source {
		if s == target {
			return true
		}
	}
	return false
}

// Проверка вхождения UUID в массив
func containsUUID(uuids []string, target string) bool {
	return containsString(uuids, target)
}

// Проверка вхождения названия детали в массив
func containsName(names []string, target string) bool {
	return containsString(names, target)
}

// Проверка вхождения тэгов в массив
func containsTags(sourceTags, target []string) bool {
	for _, sourceTagItem := range sourceTags {
		for _, t := range target {
			if sourceTagItem == t {
				return true
			}
		}
	}
	return false
}

// Проверка вхождения страны производства в массив
func containsManufacturerCountries(countries []string, target string) bool {
	return containsString(countries, target)
}

// Проверка вхождения категории в массив
func containsCategories(categories []inventoryV1.Category, target inventoryV1.Category) bool {
	for _, c := range categories {
		if c == target {
			return true
		}
	}
	return false
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
	service := NewInventoryService()
	inventoryV1.RegisterInventoryServiceServer(s, service)
	reflection.Register(s)

	go func() {
		log.Printf("🚀 gRPC InventoryService listening on %d\n", grpcPort)
		err = s.Serve(lis)
		if err != nil {
			log.Printf("failed to InventoryService: %v\n", err)
			return
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("🛑 Shutting down gRPC InventoryService...")
	s.GracefulStop()
	log.Println("✅ InventoryService stopped")
}
