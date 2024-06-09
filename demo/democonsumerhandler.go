package demo

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	. "github.com/nbigot/ministream-client-go/client"
	. "github.com/nbigot/ministream-client-go/client/types"
	ministreamproducer "github.com/nbigot/ministream-client-go/producer"
)

const MaxRetryCounterAuthenticate = 10
const MaxRetryCounterCreateRecordsIterator = 10

type ConsumerHandlerDemo struct {
	// implements interface StreamConsumerHandler
	Client                            *MinistreamClient
	Logger                            *log.Logger
	CptRecordsProcessed               int64
	RetryCounterAuthenticate          int
	RetryCounterCreateRecordsIterator int
	StopOnCptRecordsProcessed         int64
	nextExpectedRecordID              int64
	enableCheckRecordID               bool
	getRecordsSuccessCounter          int
	mu                                sync.Mutex
}

func (h *ConsumerHandlerDemo) GetLogger() *log.Logger {
	if h.Logger == nil {
		h.Logger = log.New(os.Stdout, "ConsumerHandlerDemo ", log.Ldate|log.Ltime|log.Lmicroseconds|log.LUTC)
	}
	return h.Logger
}

func (h *ConsumerHandlerDemo) GetClient() *MinistreamClient {
	return h.Client
}

func (h *ConsumerHandlerDemo) GetRecordsIteratorParams() *RecordsIteratorParams {
	name := "benchmark 1"
	maxWaitTimeSeconds := 10 // enable long pooling (10 seconds)
	return &RecordsIteratorParams{Name: &name, IteratorType: IteratorTypeFirstMessage, MaxWaitTimeSeconds: &maxWaitTimeSeconds}
}

func (h *ConsumerHandlerDemo) OnAuthenticationSuccess() {
	h.Logger.Println("OnAuthenticationSuccess")
	h.RetryCounterAuthenticate = 0
}

func (h *ConsumerHandlerDemo) OnAuthenticationFailure(e *APIError) bool {
	h.Logger.Println("OnAuthenticationFailure")
	h.RetryCounterAuthenticate++
	switch e.Code {
	case ErrorHTTPTimeout:
		{
			if h.RetryCounterAuthenticate > MaxRetryCounterAuthenticate {
				return false
			} else {
				return true
			}
		}
	default:
		h.Logger.Printf("consumer: OnAuthenticationFailure error: %s\n", e.ToJson())
		return false
	}
}

func (h *ConsumerHandlerDemo) OnCreateRecordsIteratorSuccess() {
	h.Logger.Println("OnCreateRecordsIteratorSuccess")
	h.RetryCounterCreateRecordsIterator = 0
}

func (h *ConsumerHandlerDemo) OnCreateRecordsIteratorFailure(e *APIError) bool {
	h.Logger.Println("OnCreateRecordsIteratorFailure")
	h.RetryCounterCreateRecordsIterator++
	switch e.Code {
	case ErrorContextDeadlineExceeded:
		h.Logger.Printf("consumer: OnCreateRecordsIteratorFailure error: %s\n", e.ToJson())
		return false
	case ErrorHTTPTimeout, ErrorTimeout, ErrorTransportReadFromServerError, ErrorTooManyRequests:
		{
			if h.RetryCounterCreateRecordsIterator > MaxRetryCounterCreateRecordsIterator {
				return false
			} else {
				return true
			}
		}
	default:
		h.Logger.Printf("consumer: OnCreateRecordsIteratorFailure error: %s\n", e.ToJson())
		return false
	}
}

func (h *ConsumerHandlerDemo) OnUnexpectedError(apiError *APIError) bool {
	// TODO: handle case: iterator not found (because server has deleted it due to lifespan timeout)
	h.Logger.Printf("consumer: OnUnexpectedError: %s\n", apiError.ToJson())
	//panic(apiError) // TODO: handle timeout

	// return true to continue consumming (continue/try again) or false to stop consumming
	return false
}

func (h *ConsumerHandlerDemo) OnPause() {
	h.Logger.Println("OnPause")
}

func (h *ConsumerHandlerDemo) OnResume() {
	h.Logger.Println("OnResume")
}

func (h *ConsumerHandlerDemo) OnClose() {
	h.Logger.Printf("OnClose: total records processed: %d", h.CptRecordsProcessed)
}

func (h *ConsumerHandlerDemo) OnGetRecordsSuccess(response *GetStreamRecordsResponse) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.getRecordsSuccessCounter++

	// TODO: insert your custom action there:
	// example:
	// h.Logger.Printf("OnGetRecordsSuccess: getRecordsSuccessCounter=%d\n", h.getRecordsSuccessCounter)
	// h.Logger.Printf("%+v\n", response)
	// for record := range response.Records {
	// 	h.Logger.Printf("%+v\n", record)
	// }

	// verify each record, ensure that the record ids are in sequence.
	// the goal is to check if the records are received in the correct order.
	if h.enableCheckRecordID {
		var envelope ResponseRecordEnvelope
		var simpleRecord ministreamproducer.SimpleRecord
		for _, record := range response.Records {
			h.nextExpectedRecordID++

			// record is a map[string]interface{}
			// convert the record to a ResponseRecordEnvelope
			envelopeBytes, err := json.Marshal(record)
			if err != nil {
				h.Logger.Printf("consumer: OnGetRecordsSuccess: error, Marshal record: nextExpectedRecordID %d, record: %v\n", h.nextExpectedRecordID, record)
				// drop the record
				continue
			}
			if err := json.Unmarshal(envelopeBytes, &envelope); err != nil {
				h.Logger.Printf("consumer: OnGetRecordsSuccess: error, Unmarshal record: nextExpectedRecordID %d, expected %s\n", h.nextExpectedRecordID, record)
				// drop the record
				continue
			}

			recordBytes, err := json.Marshal(envelope.Msg)
			if err != nil {
				h.Logger.Printf("consumer: OnGetRecordsSuccess: error, Marshal record: nextExpectedRecordID %d, record: %v\n", h.nextExpectedRecordID, record)
				// drop the record
				continue
			}
			if err := json.Unmarshal(recordBytes, &simpleRecord); err != nil {
				h.Logger.Printf("consumer: OnGetRecordsSuccess: error, Unmarshal record: nextExpectedRecordID %d, expected %s\n", h.nextExpectedRecordID, record)
				// drop the record
				continue
			}

			if envelope.Id != uint64(h.nextExpectedRecordID) {
				h.Logger.Printf("consumer: OnGetRecordsSuccess: error, record id mismatch: got %d, expected %d\n", envelope.Id, h.nextExpectedRecordID)
			}

			expectedMsg := fmt.Sprintf("hello world %d ", h.nextExpectedRecordID)
			if !strings.HasPrefix(simpleRecord.Msg, expectedMsg) {
				h.Logger.Printf("consumer: OnGetRecordsSuccess: error, record id mismatch: getRecordsSuccessCounter=%d got %d, expected %s\n", h.getRecordsSuccessCounter, h.nextExpectedRecordID, simpleRecord.Msg)
			}
		}
	}

	cptRecordsProcessed := int64(len(response.Records))
	h.CptRecordsProcessed += cptRecordsProcessed
	h.Logger.Printf(
		"OnGetRecordsSuccess: getRecordsSuccessCounter=%d, new records processed=%d, total records processed=%d",
		h.getRecordsSuccessCounter, cptRecordsProcessed, h.CptRecordsProcessed,
	)
	if h.CptRecordsProcessed >= h.StopOnCptRecordsProcessed {
		// success: all records have been processed
		// return false to stop consumming (stop)
		return false
	} else {
		// return true to continue consumming (continue/try again)
		return true
	}
}

func (h *ConsumerHandlerDemo) OnGetRecordsFailure(e *APIError) bool {
	h.Logger.Println("OnGetRecordsFailure")
	return true
}

func (h *ConsumerHandlerDemo) OnStart() {
	h.Logger.Println("OnStart")
	h.CptRecordsProcessed = 0
	h.RetryCounterAuthenticate = 0
	h.RetryCounterCreateRecordsIterator = 0
}

func NewConsumerHandlerDemo(client *MinistreamClient, stopOnCptRecordsProcessed int64) *ConsumerHandlerDemo {
	return &ConsumerHandlerDemo{
		Client:                            client,
		Logger:                            nil,
		CptRecordsProcessed:               0,
		RetryCounterAuthenticate:          0,
		RetryCounterCreateRecordsIterator: 0,
		StopOnCptRecordsProcessed:         stopOnCptRecordsProcessed,
		enableCheckRecordID:               true,
		nextExpectedRecordID:              0,
	}
}
