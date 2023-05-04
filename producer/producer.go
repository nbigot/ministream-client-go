package ministreamproducer

import (
	"context"
	"sync"
	"time"

	ministreamclient "github.com/nbigot/ministream-client-go/client"
	"github.com/nbigot/ministream-client-go/client/backoff"
	"github.com/nbigot/ministream-client-go/client/types"

	"github.com/google/uuid"
)

type StreamProducer struct {
	Client                *ministreamclient.MinistreamClient
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
	mu                    sync.Mutex
}

type SimpleRecord struct {
	Date time.Time `json:"date"`
	Msg  string    `json:"msg"`
}

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

	for indexBegin <= indexEnd {
		cptItemsPushed := p.RecordsQueue.PushItems(records, indexBegin, indexEnd)
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

	go p.SetState(types.ProducerStateRunning)

	for {
		select {
		case <-ctx.Done():
			p.SetState(types.ProducerStateClosing)
		case <-p.chEvOnRecordsEnqueued:
			// record(s) is/are ready to be send
			go func() { chEvCheckForRecordsToSend <- struct{}{} }()
		case <-chEvBackpressureTimeout:
			go func() { chEvCheckForRecordsToSend <- struct{}{} }()
		case <-chEvCheckForRecordsToSend:
			p.FillRecordsBufferFromQueue()
			p.SendBatchRecords(ctx)
			if p.WaitForBackPressure {
				p.BackPressure.WaitAndNotify(chEvBackpressureTimeout)
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
					// last chance to send pending records
					p.FillRecordsBufferFromQueue()
					ctxShutdown, ctxCancelFunc := context.WithTimeout(ctx, p.ShutdownTimeout)
					defer ctxCancelFunc()
					p.SendBatchRecords(ctxShutdown)
					// all remaining records in the queue whill be lost
					p.RecordsQueue.Clear()
					p.Batch.Clear()
					p.Client.Disconnect()
					p.SetState(types.ProducerStateClosed)
				}
			case types.ProducerStateClosed:
				{
					return nil
				}
			}
		}
	}
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

func (p *StreamProducer) SendBatchRecords(ctx context.Context) {
	cptRecords := p.Batch.Size()
	if cptRecords == 0 {
		// no records to be sent
		return
	}

	p.EvHandler.OnPreBatchSent(cptRecords)

	// send all the records in the buffer to the server
	_, httpResponse, apiError := p.Client.PutRecords(ctx, p.StreamUUID, p.Batch.GetRecords())
	if apiError != nil {
		p.WaitForBackPressure = true
		if httpResponse != nil {
			rateLimit := backoff.RateLimitFromHttpResponse(httpResponse)
			if rateLimit.RetryAfter > 0 || httpResponse.StatusCode == 429 {
				// error is due to rate limiting (retry later)
				p.BackPressure.SetDuration(time.Duration(rateLimit.RetryAfter) * time.Second)
			}
		}
		// failed to send records to the server
		p.EvHandler.OnPostBatchSent(0)
		return
	}

	// succeeded to send records to the server
	p.WaitForBackPressure = false
	p.Batch.Clear()
	p.BackPressure.Reset()
	p.EvHandler.OnPostBatchSent(cptRecords)
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

func NewStreamProducer(ctx context.Context, client *ministreamclient.MinistreamClient, streamUUID uuid.UUID, h ProducerEventHandler) *StreamProducer {
	p := StreamProducer{
		Client:                client,
		RecordsQueue:          BuildCircularBuffer(types.DefaultRecordsQueueLen + 1),
		WaitForBackPressure:   false,
		BackPressure:          backoff.NewExpBackoff(ctx.Done(), time.Duration(200)*time.Millisecond, time.Duration(10000)*time.Millisecond),
		State:                 types.ProducerStateInitialized,
		EvHandler:             h,
		StreamUUID:            streamUUID,
		Batch:                 BuildBatchRecords(types.MaxPushRecordsByCall),
		ShutdownTimeout:       30 * time.Second,
		chEvOnStateChanged:    make(chan types.ProducerState, 1),
		chEvOnRecordsEnqueued: make(chan struct{}, 1),
	}
	return &p
}
