package ministreamproducer

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/nbigot/ministream-client-go/client/backoff"
	"github.com/nbigot/ministream-client-go/client/types"

	"github.com/google/uuid"
)

type StreamProducer struct {
	Client                types.IProducerClient
	RecordsQueue          *CircularBuffer
	WaitForBackPressure   bool
	BackPressure          *backoff.ExpBackoff
	State                 types.ProducerState
	EvHandler             ProducerEventHandler
	StreamUUID            uuid.UUID
	Batch                 *BatchRecords
	ShutdownTimeout       time.Duration
	chEvOnStateChanged    chan types.ProducerState
	chEvOnRecordsEnqueued chan struct{}
	Logger                *log.Logger
	LogLevel              int
	mu                    sync.Mutex
}

type SimpleRecord struct {
	Date time.Time `json:"date"`
	Msg  string    `json:"msg"`
}

const (
	DEBUG = iota
	INFO
	WARNING
	ERROR
)

func (p *StreamProducer) EnqueueStringRecord(record string) (int, error) {
	return p.EnqueueRecord(SimpleRecord{Date: time.Now(), Msg: record})
}

func (p *StreamProducer) EnqueueRecord(record interface{}) (int, error) {
	return p.EnqueueRecords([]interface{}{record})
}

func (p *StreamProducer) EnqueueRecords(records []interface{}) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	currentState := p.GetState()
	if currentState != types.ProducerStateRunning && currentState != types.ProducerStatePause {
		return 0, &ProducerInvalidStateError{Message: "can't enqueue records when state is not running/pause", State: currentState}
	}

	total := len(records)
	indexBegin := 0
	indexEnd := total - 1

	p.Log(INFO, "EnqueueRecords: start\n")
	for indexBegin <= indexEnd {
		cptItemsPushed := p.RecordsQueue.PushItems(records, indexBegin, indexEnd)
		p.Log(DEBUG, "EnqueueRecords: cptItemsPushed=%d\n", cptItemsPushed)
		if cptItemsPushed > 0 {
			indexBegin += cptItemsPushed
			p.chEvOnRecordsEnqueued <- struct{}{}
		}

		// note: OnRecordEnqueue is responsible for wait/sleep for error retry
		if err := p.EvHandler.OnRecordsEnqueued(cptItemsPushed, indexBegin, total); err != nil {
			// handler has decided to stop adding remaining records into the queue
			return indexBegin, err
		}
	}
	p.Log(INFO, "EnqueueRecords: end\n")

	return total, nil
}

func (p *StreamProducer) Run(ctx context.Context) error {
	if state := p.GetState(); state != types.ProducerStateInitialized {
		return &ProducerInvalidStateError{
			Message: "producer status must be: ProducerStatusInitialized", State: state,
		}
	}

	chEvBackpressureTimeout := make(chan bool, 1)
	chEvCheckForRecordsToSend := make(chan struct{}, 1)

	// create context for closing
	ctxClosing, ctxCancelClosingFunc := context.WithCancel(ctx)
	defer ctxCancelClosingFunc()

	go p.SetState(types.ProducerStateRunning)

	for {
		select {
		case <-ctx.Done():
			p.SetState(types.ProducerStateClosing)
		case <-ctxClosing.Done():
			p.FinalizeClosingState()
		case <-p.chEvOnRecordsEnqueued:
			// record(s) is/are ready to be send
			go func() { chEvCheckForRecordsToSend <- struct{}{} }()
		case <-chEvBackpressureTimeout:
			go func() { chEvCheckForRecordsToSend <- struct{}{} }()
		case <-chEvCheckForRecordsToSend:
			// security: check if the producer is still running nor closing
			if p.GetState() != types.ProducerStateRunning && p.GetState() != types.ProducerStateClosing {
				continue
			}

			p.FillRecordsBufferFromQueue()
			err := p.SendBatchRecords(ctx)
			if p.WaitForBackPressure {
				p.BackPressure.WaitAndNotify(chEvBackpressureTimeout)
			} else if err != nil {
				// error occurred while sending records
				// try to send the records again
				go func() { chEvCheckForRecordsToSend <- struct{}{} }()
			}
		case newState := <-p.chEvOnStateChanged:
			switch newState {
			case types.ProducerStateRunning:
				{
					// reset back pressure (also needed when resume pause)
					p.WaitForBackPressure = false
					p.BackPressure.Reset()
				}
			case types.ProducerStatePause:
				{
					continue
				}
			case types.ProducerStateClosing:
				{
					if p.RecordsQueue.IsEmpty() && p.Batch.IsEmpty() {
						// no records left to be sent,
						// close the producer immediately
						ctxCancelClosingFunc()
					} else {
						// some records are still in the queue give them a chance to be sent
						time.AfterFunc(p.ShutdownTimeout, func() {
							// the deadline has been exceeded
							ctxCancelClosingFunc()
						})
					}
				}
			case types.ProducerStateClosed:
				{
					// producer is closed, exit the loop
					return nil
				}
			}
		}
	}
}

func (p *StreamProducer) FinalizeClosingState() {
	if p.GetState() != types.ProducerStateClosing {
		p.Log(INFO, "FinalizeClosingState: ignore: state is not ProducerStateClosing\n")
		return
	}

	// TODO: customize with your own behavior to handle the records that are still in the queue
	// all remaining records in the queue will be lost!
	if !p.RecordsQueue.IsEmpty() {
		p.Log(ERROR, "FinalizeClosingState: %d records in queue are lost\n", p.RecordsQueue.Size())
	}
	p.RecordsQueue.Clear()

	if !p.Batch.IsEmpty() {
		p.Log(ERROR, "FinalizeClosingState: %d records in batch are lost\n", p.Batch.Size())
	}
	p.Batch.Clear()

	p.Client.Disconnect()
	p.SetState(types.ProducerStateClosed)
}

func (p *StreamProducer) FillRecordsBufferFromQueue() {
	if p.RecordsQueue.IsEmpty() || p.Batch.IsFull() {
		// trick: batch records buffer might already be pre-filled with some records
		// don't fill the batch records if it's size reaches the maximum allowed capacity
		return
	}

	// trick: records buffer might already be pre-filled,
	// therefore it must be preserved and may be filled with new records
	for record, hasNext := p.RecordsQueue.Pop(); hasNext; record, hasNext = p.RecordsQueue.Pop() {
		p.Batch.Append(record)
		if p.Batch.IsFull() {
			// stop filling the buffer if it reaches the maximum allowed capacity
			break
		}
	}
}

func (p *StreamProducer) SendBatchRecords(ctx context.Context) error {
	cptRecords := p.Batch.Size()
	if cptRecords == 0 {
		// no records to be sent
		return nil
	}

	batchId := p.Batch.GetId()
	p.EvHandler.OnPreBatchSent(batchId, cptRecords)

	// send all the records in the buffer to the server
	response, httpResponse, apiError := p.Client.PutRecords(ctx, p.StreamUUID, p.Batch.GetId(), p.Batch.GetRecords())
	if response != nil {
		p.Log(DEBUG, "SendBatchRecords response: %v\n", response)
	}
	if apiError != nil {
		switch apiError.Code {
		case types.ErrorHTTPTimeout:
			// error is due to timeout on client side
			// at this point the batch may have been sent to the server and the server may be still processing it
			// we can't be sure if the processing was/will be successful or not
			// we must retry the batch without changing the batch id, to avoid duplicates (server handles deduplication)
			// we don't need to wait for back pressure, because the server may have already processed the batch
			p.WaitForBackPressure = false
		case types.ErrorDuplicatedBatchId:
			// error is due to duplicated batch id
			// the server has already processed the batch
			// this batch must be considered as successfully sent
			p.WaitForBackPressure = false
			p.Batch.Clear()
			p.BackPressure.Reset()
			p.EvHandler.OnPostBatchSent(batchId, 0)
			return nil // pretend it's a success
		case types.ErrorTooManyRequests:
			// error is due to rate limiting on server side (retry later)
			p.WaitForBackPressure = true
			var duration uint64 = 1 // default duration (1 second)
			if httpResponse != nil {
				// try to get a rate limit from the http response
				rateLimit := backoff.RateLimitFromHttpResponse(httpResponse)
				if rateLimit.RetryAfter > 0 || httpResponse.StatusCode == 429 {
					// error is due to rate limiting (retry later)
					duration = rateLimit.RetryAfter
				}
			}
			p.BackPressure.SetDuration(time.Duration(duration) * time.Second)
		default:
			p.WaitForBackPressure = true
		}

		// failed to send records to the server
		p.EvHandler.OnPostBatchSent(batchId, 0)
		return apiError
	}

	// succeeded to send records to the server
	p.WaitForBackPressure = false
	p.Batch.Clear()
	p.BackPressure.Reset()
	p.EvHandler.OnPostBatchSent(batchId, cptRecords)
	return nil
}

func (p *StreamProducer) SetState(state types.ProducerState) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.State = state
	p.EvHandler.OnStateChanged(state)
	p.chEvOnStateChanged <- state
}

func (p *StreamProducer) GetState() types.ProducerState {
	return p.State
}

func (p *StreamProducer) Log(level int, format string, v ...interface{}) {
	if p.Logger == nil || level < p.LogLevel {
		return
	}
	p.Logger.Printf(format, v...)
}

func NewStreamProducer(ctx context.Context, logger *log.Logger, logLevel int, client types.IProducerClient, streamUUID uuid.UUID, h ProducerEventHandler) *StreamProducer {
	p := StreamProducer{
		Client:                client,
		RecordsQueue:          BuildCircularBuffer(types.DefaultRecordsQueueLen + 1),
		WaitForBackPressure:   false,
		BackPressure:          backoff.NewExpBackoff(ctx.Done(), time.Duration(200)*time.Millisecond, time.Duration(10000)*time.Millisecond),
		State:                 types.ProducerStateInitialized,
		EvHandler:             h,
		StreamUUID:            streamUUID,
		Batch:                 NewBatchRecords(types.MaxPushRecordsByCall),
		ShutdownTimeout:       30 * time.Second,
		Logger:                logger,
		LogLevel:              logLevel,
		chEvOnStateChanged:    make(chan types.ProducerState, 1),
		chEvOnRecordsEnqueued: make(chan struct{}, 1),
	}
	return &p
}
