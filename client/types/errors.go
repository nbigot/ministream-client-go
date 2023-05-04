package types

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
)

func (e *APIError) CanRetry() bool {
	switch e.Code {
	case ErrorHTTPTimeout, ErrorTimeout, ErrorTransportReadFromServerError:
		return true
	default:
		return false
	}
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%d: %s", e.Code, e.Message)
}

func (e *APIError) ToJson() string {
	jsonData, _ := json.Marshal(*e)
	return string(jsonData)
}

func APIErrorFromError(err error) *APIError {
	urlError, isUrlError := err.(*url.Error)
	if isUrlError {
		err = urlError.Unwrap()
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return &APIError{Message: err.Error(), Code: ErrorContextDeadlineExceeded, Details: "ErrorContextDeadlineExceeded"}
	case isTransportReadFromServerError(err):
		return &APIError{Message: err.Error(), Code: ErrorTransportReadFromServerError, Details: "ErrorTransportReadFromServerError"}
	case isUrlError:
		errCode := ErrorURL
		if urlError.Timeout() {
			errCode = ErrorHTTPTimeout
		}
		return &APIError{Message: err.Error(), Code: errCode, Details: urlError.Error()}
	default:
		return &APIError{Message: err.Error()}
	}
}

func APIErrorFromHttpBodyResponse(body []byte) *APIError {
	var apiError APIError
	if err := json.Unmarshal(body, &apiError); err != nil {
		return &APIError{Message: ErrorCannotUnmarshalJson, Details: err.Error()}
	}

	return &apiError
}

func isTransportReadFromServerError(err error) bool {
	switch {
	case err.Error() == "EOF":
		return true
	default:
		return false
	}
}
