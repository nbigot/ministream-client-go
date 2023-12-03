package ministreamconsumer

import (
	"context"
	"log"
	"net/http"
	"time"

	. "github.com/nbigot/ministream-client-go/client"
	. "github.com/nbigot/ministream-client-go/client/backoff"
	. "github.com/nbigot/ministream-client-go/client/types"

	"github.com/google/uuid"
)

type StreamConsumer struct {
	client              *MinistreamClient
	logger              *log.Logger
	scanInterval        time.Duration
	maxPullRecords      Size64
	streamUUID          StreamUUID
	streamIteratorUUID  StreamIteratorUUID
	isAuthenticated     bool
	hasRecordsIterator  bool
	mustStop            bool
	Params              RecordsIteratorParams
	StatusPause         bool
	PauseDuration       time.Duration
	WaitForBackPressure bool
	BackPressure        *ExpBackoff
	RecordChannel       chan int
	GetRecordsChunks    int
	Handler             StreamConsumerHandler
}

func CreateConsumer(ctx context.Context, streamUUID StreamUUID, handler StreamConsumerHandler) *StreamConsumer {
	c := StreamConsumer{
		client:              handler.GetClient(),
		logger:              handler.GetLogger(),
		scanInterval:        250 * time.Millisecond,
		maxPullRecords:      MaxPullRecordsByCall,
		streamUUID:          streamUUID,
		isAuthenticated:     false,
		hasRecordsIterator:  false,
		mustStop:            false,
		StatusPause:         false,
		PauseDuration:       500 * time.Millisecond,
		WaitForBackPressure: false,
		BackPressure:        NewExpBackoff(ctx.Done(), time.Duration(200)*time.Millisecond, time.Duration(30)*time.Second),
		Params:              RecordsIteratorParams{IteratorType: IteratorTypeFirstMessage, MaxWaitTimeSeconds: nil},
		GetRecordsChunks:    10,
		Handler:             handler,
	}
	return &c
}

func (c *StreamConsumer) SetRecordsIteratorParams(p *RecordsIteratorParams) *APIError {
	if err := p.Validate(); err != nil {
		return err
	}

	c.Params = *p
	return nil
}

func (c *StreamConsumer) CreateRecordsIterator(ctx context.Context, p *RecordsIteratorParams) *APIError {
	if err := c.SetRecordsIteratorParams(p); err != nil {
		return err
	}

	response, apiError := c.client.CreateRecordsIterator(ctx, c.streamUUID, &c.Params)
	if apiError != nil {
		return apiError
	}

	c.streamIteratorUUID = response.StreamIteratorUUID

	c.RecordChannel = make(chan int)
	return nil
}

func (c *StreamConsumer) GetRecords(ctx context.Context, maxPullRecords int) (*GetStreamRecordsResponse, *http.Response, *APIError) {
	if c.streamIteratorUUID == uuid.Nil {
		return nil, nil, &APIError{Message: "streamIteratorUUID is not set, please create a stream iterator!"}
	}

	return c.client.GetRecords(ctx, c.streamUUID, c.streamIteratorUUID, maxPullRecords)
}

func (c *StreamConsumer) Run(ctx context.Context) *APIError {
	c.isAuthenticated = false
	c.hasRecordsIterator = false

	c.Handler.OnStart()

	for {
		if !c.EnsureIsAuthenticated(ctx) {
			if c.mustStop {
				break
			} else {
				continue
			}
		}

		if !c.EnsureHasRecordsIterator(ctx) {
			if c.mustStop {
				break
			} else {
				continue
			}
		}

		if !c.Consume(ctx) {
			if c.mustStop {
				break
			} else {
				continue
			}
		}
	}

	return c.Close(ctx)
}

func (c *StreamConsumer) EnsureIsAuthenticated(ctx context.Context) bool {
	// return true if success, false otherwise
	if c.isAuthenticated {
		return true
	}

	if apiError := c.client.Authenticate(ctx); apiError != nil {
		if ctx.Err() != nil {
			c.mustStop = true
			return false
		} else {
			if !c.Handler.OnAuthenticationFailure(apiError) || !c.BackPressure.Wait() {
				c.mustStop = true
			}
			return false
		}
	} else {
		c.isAuthenticated = true
		c.BackPressure.Reset()
		c.Handler.OnAuthenticationSuccess()
		return true
	}
}

func (c *StreamConsumer) EnsureHasRecordsIterator(ctx context.Context) bool {
	// return true if success, false otherwise
	if c.hasRecordsIterator {
		return true
	}

	if apiError := c.CreateRecordsIterator(ctx, c.Handler.GetRecordsIteratorParams()); apiError != nil {
		if ctx.Err() != nil {
			c.mustStop = true
			return false
		} else {
			if apiError.Code == ErrorJWTInvalidOrExpired {
				// need to reauthenticate
				c.isAuthenticated = false
				return false
			}

			if !c.Handler.OnCreateRecordsIteratorFailure(apiError) || !c.BackPressure.Wait() {
				c.mustStop = true
			}
			return false
		}
	} else {
		c.Handler.OnCreateRecordsIteratorSuccess()
		c.hasRecordsIterator = true
		c.BackPressure.Reset()
		return true
	}
}

func (c *StreamConsumer) Consume(ctx context.Context) bool {
	// return true if success, false otherwise
	for {
		if ctx.Err() != nil {
			c.mustStop = true
			return false
		}

		if c.StatusPause {
			// the consumer is paused
			time.Sleep(c.PauseDuration)
			continue
		}

		if c.WaitForBackPressure {
			// handle backpressure
			if !c.BackPressure.Wait() {
				// a cancel event occurred during the wait
				c.mustStop = true
				return false
			}
		}

		if !c.Poll(ctx) {
			return false
		}
	}
}

func (c *StreamConsumer) Poll(ctx context.Context) bool {
	if response, httpResponse, apiError := c.GetRecords(ctx, c.GetRecordsChunks); apiError != nil {
		if !c.Handler.OnGetRecordsFailure(apiError) {
			c.mustStop = true
			return false
		}

		switch apiError.Code {
		case ErrorJWTInvalidOrExpired:
			{
				// need to reauthenticate
				c.isAuthenticated = false
			}
		case ErrorStreamIteratorNotFound:
			{
				// need to recreate a stream iterator
				c.hasRecordsIterator = false
			}
		case ErrorHTTPTimeout:
			{
				// retry later
				c.WaitForBackPressure = true
			}
		case ErrorCantGetMessagesFromStream:
			{
				c.mustStop = true
				c.logger.Panicf("Poll: ErrorCantGetMessagesFromStream: %s\n", apiError.ToJson())
			}
		default:
			if !c.Handler.OnUnexpectedError(apiError) {
				// the handler has decided to stop the consumption
				c.mustStop = true
			} else {
				// not needed but just in case
				// will wait a little, to hopefully increase the chances of success for the next call
				c.WaitForBackPressure = true
			}
		}
		return false
	} else {
		// success
		// response is handled by the handler
		if !c.Handler.OnGetRecordsSuccess(response) {
			c.mustStop = true
			return false
		}

		// handle back pressure
		if response.Remain {
			rateLimit := RateLimitFromHttpResponse(httpResponse)
			if rateLimit.RetryAfter > 0 {
				c.WaitForBackPressure = true
			} else {
				if c.WaitForBackPressure {
					c.WaitForBackPressure = false
					c.BackPressure.Reset()
				}
			}
		} else {
			// preventive action: stream don't have any new records yet
			c.WaitForBackPressure = true
		}

		return true
	}
}

func (c *StreamConsumer) Pause() {
	c.Handler.OnPause()
	c.StatusPause = true
}

func (c *StreamConsumer) Resume() {
	c.Handler.OnResume()
	c.StatusPause = false
	c.WaitForBackPressure = false
}

func (c *StreamConsumer) GetPauseSatus() bool {
	return c.StatusPause
}

func (c *StreamConsumer) Close(ctx context.Context) *APIError {
	c.Handler.OnClose()
	if c.streamIteratorUUID == uuid.Nil {
		return nil
	}
	defer func() {
		c.streamIteratorUUID = uuid.Nil
	}()
	defer c.client.Disconnect()

	return c.client.CloseRecordsIterator(ctx, c.streamUUID, c.streamIteratorUUID)
}
