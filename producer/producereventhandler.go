package ministreamproducer

import (
	"github.com/nbigot/ministream-client-go/client/types"
)

type ProducerEventHandler interface {
	Init(producer *StreamProducer)
	OnSendError()
	OnPreBatchSent(batchId int, batchSize int)
	OnPostBatchSent(batchId int, batchSize int)
	OnStateChanged(state types.ProducerState)
	OnRecordsEnqueued(cptRecords int, index int, total int) error
	OnRecordEnqueueTimeout(records []interface{}, cptRecordsEnqueued int, cptRecordsNotEnqueued int)
}
