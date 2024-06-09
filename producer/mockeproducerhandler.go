package ministreamproducer

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	. "github.com/nbigot/ministream-client-go/client/backoff"
	. "github.com/nbigot/ministream-client-go/client/types"
)

type MockProducerEventHandler struct {
	// implements interface ProducerEventHandler
	Logger                               *log.Logger
	cptRecordsEnqueued                   int64
	lastStartSendHttpRequest             time.Time
	lastHttpRequestDuration              time.Duration
	backPressure                         *ExpBackoff
	numberOfSuccessiveBufferingErrors    int
	maxNumberOfSuccessiveBufferingErrors int
	firstBufferingErrorDate              time.Time
	maxBufferingErrorDuration            time.Duration
	batchNumber                          int64
	totalRecordsCreated                  int64
	totalRecordsSend                     int64
	producer                             *StreamProducer
	cptRecordsToSend                     int64
	sendBatchSize                        int64
}

func (h *MockProducerEventHandler) GetLogger() *log.Logger {
	if h.Logger == nil {
		h.Logger = log.New(os.Stdout, "MockProducerEventHandler ", log.Ldate|log.Ltime|log.Lmicroseconds|log.LUTC)
	}
	return h.Logger
}

func (h *MockProducerEventHandler) OnSendError() {
	h.Logger.Println("OnSendError")
}

func (h *MockProducerEventHandler) OnPreBatchSent(batchId int, batchSize int) {
	h.batchNumber++
	h.Logger.Printf("OnPreBatchSent: batchId=%d, batchNumber=%d flushed %d records...\n", batchId, h.batchNumber, batchSize)
	h.lastStartSendHttpRequest = time.Now()
}

func (h *MockProducerEventHandler) OnPostBatchSent(batchId int, batchSize int) {
	h.totalRecordsSend += int64(batchSize)
	h.lastHttpRequestDuration = time.Since(h.lastStartSendHttpRequest)
	h.Logger.Printf(
		"OnPostBatchSent: batchId=%d, batchNumber=%d totalRecordsSend=%d flushed %d records, duration: %s\n",
		batchId, h.batchNumber, h.totalRecordsSend, batchSize, h.lastHttpRequestDuration,
	)
}

func (h *MockProducerEventHandler) OnStateChanged(state ProducerState) {
	h.Logger.Printf("OnStateChanged: new state is %d\n", state)
	if state == ProducerStateRunning {
		go h.CreateRecords()
	}
}

func (h *MockProducerEventHandler) CreateRecords() {
	h.Logger.Println("CreateRecords: start create records")
	defer h.producer.SetState(ProducerStateClosing) // ask producer to stop
	defer h.Logger.Println("CreateRecords: stop create records")

	cptRemainRecordsToSend := h.cptRecordsToSend
	sendBatchSize := h.sendBatchSize
	i := int64(1)
	var randomValue int

	for cptRemainRecordsToSend > 0 {
		if cptRemainRecordsToSend < sendBatchSize {
			sendBatchSize = cptRemainRecordsToSend
		}

		records := make([]interface{}, sendBatchSize)
		for recordIdx := int64(0); recordIdx < sendBatchSize; recordIdx++ {
			now := time.Now()
			h.totalRecordsCreated++
			randomValue = rand.Intn(10000)
			records[recordIdx] = &SimpleRecord{Date: now, Msg: fmt.Sprintf("hello world %d %d", h.totalRecordsCreated, randomValue)}
		}

		if _, err := h.producer.EnqueueRecords(records); err != nil {
			// can't enqueue records
			h.Logger.Println(err)
			return
		} else {
			i += h.sendBatchSize
			cptRemainRecordsToSend -= sendBatchSize
			h.Logger.Printf("CreateRecords: produced %d records, remain %d\n", sendBatchSize, cptRemainRecordsToSend)
		}
	}
}

func (h *MockProducerEventHandler) OnRecordsEnqueued(cptRecords int, index int, total int) error {
	// note: OnRecordsEnqueued is responsible for wait/sleep for error retry
	// handle the logic for rate control and failure when pushing the records into the buffer
	if cptRecords > 0 {
		atomic.AddInt64(&h.cptRecordsEnqueued, int64(cptRecords))
		h.numberOfSuccessiveBufferingErrors = 0
		return nil // continue to push records into the buffer
	}

	if h.numberOfSuccessiveBufferingErrors == 0 {
		h.firstBufferingErrorDate = time.Now()
	}

	h.numberOfSuccessiveBufferingErrors++
	if h.numberOfSuccessiveBufferingErrors > h.maxNumberOfSuccessiveBufferingErrors || time.Since(h.firstBufferingErrorDate) > h.maxBufferingErrorDuration {
		// stop trying to pushing records into the buffer
		return &ProducerBufferingError{
			NumberOfSuccessiveBufferingErrors:    h.numberOfSuccessiveBufferingErrors,
			MaxNumberOfSuccessiveBufferingErrors: h.maxNumberOfSuccessiveBufferingErrors,
			FirstBufferingErrorDate:              h.firstBufferingErrorDate,
			MaxBufferingErrorDuration:            h.maxBufferingErrorDuration,
			BufferingErrorDuration:               time.Since(h.firstBufferingErrorDate),
		}
	}

	// wait a little before trying again later
	h.backPressure.Wait()
	return nil // try again to push records into the buffer
}

func (h *MockProducerEventHandler) OnRecordEnqueueTimeout(records []interface{}, cptRecordsEnqueued int, cptRecordsNotEnqueued int) {
	h.Logger.Printf("OnRecordEnqueueTimeout: %d records enqueued, %d records not enqueued\n", cptRecordsEnqueued, cptRecordsNotEnqueued)
}

func (h *MockProducerEventHandler) Init(producer *StreamProducer) {
	h.producer = producer
	h.GetLogger()
}

func NewMockProducerEventHandler(ctx context.Context, cptRecordsToSend int64, sendBatchSize int64) *MockProducerEventHandler {
	if sendBatchSize > MaxPushRecordsByCall {
		panic(fmt.Sprintf("sendBatchSize must be less than %d", MaxPushRecordsByCall))
	}
	return &MockProducerEventHandler{
		batchNumber:                          0,
		totalRecordsCreated:                  0,
		totalRecordsSend:                     0,
		numberOfSuccessiveBufferingErrors:    0,
		maxNumberOfSuccessiveBufferingErrors: 1000,
		firstBufferingErrorDate:              time.Time{},
		maxBufferingErrorDuration:            time.Duration(60) * time.Second * 5, // 5 minutes
		backPressure:                         NewExpBackoff(ctx.Done(), time.Duration(10)*time.Millisecond, time.Duration(1)*time.Second),
		Logger:                               nil,
		producer:                             nil,
		cptRecordsToSend:                     cptRecordsToSend,
		sendBatchSize:                        sendBatchSize,
	}
}
