package ministreamproducer

import (
	"fmt"

	"github.com/nbigot/ministream-client-go/client/types"
)

type ProducerInvalidStateError struct {
	State   types.ProducerState
	Message string
}

func (err *ProducerInvalidStateError) Error() string {
	return fmt.Sprintf("%s, invalid sate: %v", err.Message, err.State)
}
