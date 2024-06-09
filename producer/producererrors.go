package ministreamproducer

import (
	"fmt"
	"time"

	"github.com/nbigot/ministream-client-go/client/types"
)

type ProducerInvalidStateError struct {
	State   types.ProducerState
	Message string
}

func (err *ProducerInvalidStateError) Error() string {
	return fmt.Sprintf("%s, invalid sate: %v", err.Message, err.State)
}

type ProducerBufferingError struct {
	NumberOfSuccessiveBufferingErrors    int
	MaxNumberOfSuccessiveBufferingErrors int
	FirstBufferingErrorDate              time.Time
	MaxBufferingErrorDuration            time.Duration
	BufferingErrorDuration               time.Duration
}

func (err *ProducerBufferingError) Error() string {
	return fmt.Sprintf(
		"NumberOfSuccessiveBufferingErrors: %d, BufferingErrorDuration: %v",
		err.NumberOfSuccessiveBufferingErrors, err.BufferingErrorDuration,
	)
}
