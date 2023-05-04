package types

type IteratorType string

// Enum values for IteratorType
const (
	IteratorTypeFirstMessage     IteratorType = "FIRST_MESSAGE"
	IteratorTypeLastMessage      IteratorType = "LAST_MESSAGE"
	IteratorTypeAfterLastMessage IteratorType = "AFTER_LAST_MESSAGE"
	IteratorTypeAtMessageId      IteratorType = "AT_MESSAGE_ID"
	IteratorTypeAfterMessageId   IteratorType = "AFTER_MESSAGE_ID"
	IteratorTypeAtTimestamp      IteratorType = "AT_TIMESTAMP"
)

// Values returns all known values for IteratorType.
func (IteratorType) Values() []IteratorType {
	return []IteratorType{
		"FIRST_MESSAGE",
		"LAST_MESSAGE",
		"AFTER_LAST_MESSAGE",
		"AT_MESSAGE_ID",
		"AFTER_MESSAGE_ID",
		"AT_TIMESTAMP",
	}
}

type ProducerState int

// Enum values for ProducerState
const (
	ProducerStateInitialized ProducerState = 0
	ProducerStateRunning     ProducerState = 1
	ProducerStatePause       ProducerState = 2
	ProducerStateClosing     ProducerState = 3
	ProducerStateClosed      ProducerState = 4
)
