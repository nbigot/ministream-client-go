package types

import (
	"time"

	"github.com/google/uuid"
)

type MessageId = uint64
type Size64 = uint64
type StreamUUID = uuid.UUID
type StreamIteratorUUID = uuid.UUID
type StreamProperties = map[string]interface{}

type APIError struct {
	Message          string             `json:"error,omitempty"`            // application-level error message, for debugging
	Details          string             `json:"details,omitempty"`          // application-level error details that best describes the error, for debugging
	Code             int                `json:"code"`                       // application-specific error code
	StreamUUID       uuid.UUID          `json:"streamUUID,omitempty"`       // stream uuid
	ValidationErrors []*ValidationError `json:"validationErrors,omitempty"` // list of errors
}

type ValidationError struct {
	FailedField string
	Tag         string
	Value       string
}

type CreateStreamResponse struct {
	FilePath     string           `json:"filepath"`
	UUID         StreamUUID       `json:"uuid" example:"4ce589e2-b483-467b-8b59-758b339801db"`
	CptMessages  Size64           `json:"cptMessages" example:"12345"`
	SizeInBytes  Size64           `json:"sizeInBytes" example:"4567890"`
	CreationDate time.Time        `json:"creationDate"`
	LastUpdate   time.Time        `json:"lastUpdate"`
	Properties   StreamProperties `json:"properties"`
	LastMsgId    MessageId        `json:"lastMsgId"`
}

type GetStreamRecordsResponse struct {
	Status             string             `json:"status"`
	Duration           time.Duration      `json:"duration"`
	Count              int                `json:"count"`
	CountErrors        int                `json:"countErrors"`
	CountSkipped       int                `json:"countSkipped"`
	Remain             bool               `json:"remain"`
	StreamUUID         StreamUUID         `json:"streamUUID"`
	StreamIteratorUUID StreamIteratorUUID `json:"streamIteratorUUID"`
	Records            []interface{}      `json:"records"`
}

type CreateRecordsIteratorResponse struct {
	Status             string             `json:"status"`
	Message            string             `json:"message"`
	StreamUUID         StreamUUID         `json:"streamUUID"`
	StreamIteratorUUID StreamIteratorUUID `json:"streamIteratorUUID"`
}

type CloseRecordsIteratorResponse struct {
	Status             string             `json:"status"`
	Message            string             `json:"message"`
	StreamUUID         StreamUUID         `json:"streamUUID"`
	StreamIteratorUUID StreamIteratorUUID `json:"streamIteratorUUID"`
}

type LoginAccountResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	JWT     string `json:"jwt"`
}

type LoginUserResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	JWT     string `json:"jwt"`
}

type PutRecordsResponse struct {
	Status     string        `json:"status"`
	Duration   time.Duration `json:"duration"`
	Count      int64         `json:"count"`
	StreamUUID StreamUUID    `json:"streamUUID"`
	MessageIds []MessageId   `json:"messageIds"`
}
