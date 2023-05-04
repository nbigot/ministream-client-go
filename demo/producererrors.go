package demo

import (
	"fmt"
	"time"
)

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
