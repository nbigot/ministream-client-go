package main

import (
	"context"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/google/uuid"
	. "github.com/nbigot/ministream-client-go/client"
	. "github.com/nbigot/ministream-client-go/client/types"
	. "github.com/nbigot/ministream-client-go/consumer"
	. "github.com/nbigot/ministream-client-go/demo"
	. "github.com/nbigot/ministream-client-go/producer"
)

var wg sync.WaitGroup
var logger *log.Logger = nil
var cptRecordsToSend int64 = 100000
var sendBatchSize int64 = 10000
var serverUrl string = "http://127.0.0.1:80"

func prepareProducer(ctx context.Context, client *MinistreamClient) (*APIError, *StreamProducer, StreamUUID) {
	var apiError *APIError

	if client == nil {
		client = createClient(ctx)
	}

	if apiError = client.Authenticate(ctx); apiError != nil {
		return apiError, nil, uuid.Nil
	}

	streamProperties := StreamProperties{
		"name": "benchmark 5", "project": "benchmark", "tags": "benchmark", "env": "test",
	}
	response, apiError, err1 := client.CreateStream(ctx, &streamProperties)
	if err1 != nil {
		return &APIError{Message: err1.Error()}, nil, uuid.Nil
	}
	if apiError != nil {
		return apiError, nil, uuid.Nil
	}

	producerEventHandlerDemo := NewProducerEventHandlerDemo(ctx, cptRecordsToSend, sendBatchSize)
	producer := NewStreamProducer(ctx, client, response.UUID, producerEventHandlerDemo)
	producerEventHandlerDemo.Init(producer)
	return nil, producer, response.UUID
}

func consume(ctx context.Context, client *MinistreamClient, streamUUID uuid.UUID) {
	if client == nil {
		client = createClient(ctx)
	}

	consumerHandler := NewConsumerHandlerDemo(client, cptRecordsToSend)
	consumer := CreateConsumer(ctx, streamUUID, consumerHandler)
	consumer.Run(ctx)
}

func createClient(ctx context.Context) *MinistreamClient {
	return CreateClient(
		serverUrl,
		"ministreamGOClient",
		&Credentials{Login: "benchmark", Password: "benchmark"},
		true,
		10*time.Second,
	)
}

func produceAndConsume(ctx context.Context) {
	logger.Println("Start produceAndConsume")

	var streamUUID StreamUUID
	var producer *StreamProducer
	var apiError *APIError
	if apiError, producer, streamUUID = prepareProducer(ctx, nil); apiError != nil {
		logger.Println(apiError.ToJson())
		return
	}

	logger.Printf("Created stream: %s\n", streamUUID.String())

	wg.Add(2)
	go func() {
		defer wg.Done()
		producingStartTime := time.Now()
		logger.Println("Start producing...")
		if err := producer.Run(ctx); err != nil {
			logger.Println(err.Error())
		}
		producingDuration := time.Since(producingStartTime)
		logger.Printf("Stop producing... (took %s)\n", producingDuration)
	}()
	go func() {
		defer wg.Done()
		consumingStartTime := time.Now()
		logger.Println("Start consuming...")
		consume(ctx, nil, streamUUID)
		consumingDuration := time.Since(consumingStartTime)
		logger.Printf("Stop consuming... (took %s)\n", consumingDuration)
	}()

	wg.Wait()
	logger.Println("End produceAndConsume.")
}

func main() {
	logger = log.New(os.Stdout, "App ", log.Ldate|log.Ltime|log.Lmicroseconds|log.LUTC)
	logger.Println("Start application")
	ctx := context.Background()

	// trap Ctrl+C and call cancel on the context
	ctx, cancel := context.WithCancel(ctx)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
		cancel()
	}()
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()

	produceAndConsume(ctx)
	logger.Println("End application.")
}
