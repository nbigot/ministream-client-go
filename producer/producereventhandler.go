package ministreamproducer

import (
	"github.com/nbigot/ministream-client-go/client/types"
)

type ProducerEventHandler interface {
	OnSendError()
	OnPreBatchSent(batchSize int)
	OnPostBatchSent(batchSize int)
	OnStateChanged(state types.ProducerState)
	OnRecordsEnqueued(cptRecords int, index int, total int) error
	OnRecordEnqueueTimeout(records []interface{}, cptRecordsEnqueued int, cptRecordsNotEnqueued int)
}
