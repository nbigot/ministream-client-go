package demo

import (
	"log"
	"os"

	. "github.com/nbigot/ministream-client-go/client"
	. "github.com/nbigot/ministream-client-go/client/types"
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

	// return true to continue consumming (continue/try again)
	// return false to stop consumming
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
	h.Logger.Println("OnGetRecords")

	// insert your custom action there:
	// example:
	// h.Logger.Printf("%+v\n", response)
	// for record := range response.Records {
	// 	h.Logger.Printf("%+v\n", record)
	// }

	cptRecordsProcessed := int64(len(response.Records))
	h.CptRecordsProcessed += cptRecordsProcessed
	h.Logger.Printf(
		"OnGetRecordsSuccess: new records processed=%d, total records processed=%d",
		cptRecordsProcessed, h.CptRecordsProcessed,
	)
	if h.CptRecordsProcessed >= h.StopOnCptRecordsProcessed {
		return false
	} else {
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
	}
}
