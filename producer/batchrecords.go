package ministreamproducer

type BatchRecords struct {
	records []interface{}
}

func BuildBatchRecords(maxCapacity int) *BatchRecords {
	return &BatchRecords{records: make([]interface{}, 0, maxCapacity)}
}

func (b *BatchRecords) Clear() {
	b.records = b.records[:0]
}

func (b *BatchRecords) IsEmpty() bool {
	return len(b.records) == 0
}

func (b *BatchRecords) IsFull() bool {
	return len(b.records) == cap(b.records)
}

func (b *BatchRecords) Append(record interface{}) {
	if b.IsFull() {
		panic("BatchRecords is full, can't append any more records")
	}

	b.records = append(b.records, record)
}

func (b *BatchRecords) Size() int {
	return len(b.records)
}

func (b *BatchRecords) GetRecords() []interface{} {
	return b.records
}
