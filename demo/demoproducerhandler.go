package demo

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	. "github.com/nbigot/ministream-client-go/client/backoff"
	. "github.com/nbigot/ministream-client-go/client/types"
	. "github.com/nbigot/ministream-client-go/producer"
)

type ProducerEventHandlerDemo struct {
	// implements interface producereventhandler
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
	totalRecordsSend                     int64
	producer                             *StreamProducer
	cptRecordsToSend                     int64
	sendBatchSize                        int64
}

func (h *ProducerEventHandlerDemo) GetLogger() *log.Logger {
	if h.Logger == nil {
		h.Logger = log.New(os.Stdout, "ProducerEventHandlerDemo ", log.Ldate|log.Ltime|log.Lmicroseconds|log.LUTC)
	}
	return h.Logger
}

func (h *ProducerEventHandlerDemo) OnSendError() {
	h.Logger.Println("OnSendError")
}

func (h *ProducerEventHandlerDemo) OnPreBatchSent(batchSize int) {
	h.batchNumber++
	h.Logger.Printf("OnPreBatchSent: batchNumber=%d flushed %d records...\n", h.batchNumber, batchSize)
	h.lastStartSendHttpRequest = time.Now()
}

func (h *ProducerEventHandlerDemo) OnPostBatchSent(batchSize int) {
	h.totalRecordsSend += int64(batchSize)
	h.lastHttpRequestDuration = time.Since(h.lastStartSendHttpRequest)
	h.Logger.Printf(
		"OnPostBatchSent: batchNumber=%d totalRecordsSend=%d flushed %d records, duration: %s\n",
		h.batchNumber, h.totalRecordsSend, batchSize, h.lastHttpRequestDuration,
	)
}

func (h *ProducerEventHandlerDemo) OnStateChanged(state ProducerState) {
	h.Logger.Printf("OnStateChanged: new state is %d\n", state)
	if state == ProducerStateRunning {
		go h.CreateRecords()
	}
}

func (h *ProducerEventHandlerDemo) CreateRecords() {
	h.Logger.Println("CreateRecords: start create records")
	defer h.producer.SetState(ProducerStateClosing) // ask producer to stop
	defer h.Logger.Println("CreateRecords: stop create records")

	cptRemainRecordsToSend := h.cptRecordsToSend
	sendBatchSize := h.sendBatchSize
	i := int64(1)

	for cptRemainRecordsToSend > 0 {
		if cptRemainRecordsToSend < sendBatchSize {
			sendBatchSize = cptRemainRecordsToSend
		}

		now := time.Now()
		records := make([]interface{}, sendBatchSize)
		for recordIdx := int64(0); recordIdx < sendBatchSize; recordIdx++ {
			records[recordIdx] = &SimpleRecord{Date: now, Msg: fmt.Sprintf("hello world %d", i+recordIdx)}
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

func (h *ProducerEventHandlerDemo) OnRecordsEnqueued(cptRecords int, index int, total int) error {
	// note: OnRecordsEnqueued is responsible for wait/sleep for error retry
	// handle the logic for rate control and failure when pushing the records into the buffer
	if cptRecords > 0 {
		atomic.AddInt64(&h.cptRecordsEnqueued, int64(cptRecords))
		//h.Logger.Printf("OnRecordsEnqueued: total records enqueued: %d\n", h.cptRecordsEnqueued)
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

func (h *ProducerEventHandlerDemo) OnRecordEnqueueTimeout(records []interface{}, cptRecordsEnqueued int, cptRecordsNotEnqueued int) {
	h.Logger.Printf("OnRecordEnqueueTimeout: %d records enqueued, %d records not enqueued\n", cptRecordsEnqueued, cptRecordsNotEnqueued)
}

func (h *ProducerEventHandlerDemo) Init(producer *StreamProducer) {
	h.producer = producer
	h.GetLogger()
}

func NewProducerEventHandlerDemo(ctx context.Context, cptRecordsToSend int64, sendBatchSize int64) *ProducerEventHandlerDemo {
	return &ProducerEventHandlerDemo{
		batchNumber:                          0,
		totalRecordsSend:                     0,
		numberOfSuccessiveBufferingErrors:    0,
		maxNumberOfSuccessiveBufferingErrors: 1000,
		firstBufferingErrorDate:              time.Time{},
		maxBufferingErrorDuration:            time.Duration(60) * time.Second,
		backPressure:                         NewExpBackoff(ctx.Done(), time.Duration(10)*time.Millisecond, time.Duration(1)*time.Second),
		Logger:                               nil,
		producer:                             nil,
		cptRecordsToSend:                     cptRecordsToSend,
		sendBatchSize:                        sendBatchSize,
	}
}
