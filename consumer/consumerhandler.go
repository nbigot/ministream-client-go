package ministreamconsumer

import (
	"log"

	. "github.com/nbigot/ministream-client-go/client"
	. "github.com/nbigot/ministream-client-go/client/types"
)

type StreamConsumerHandler interface {
	GetLogger() *log.Logger
	GetClient() *MinistreamClient
	GetRecordsIteratorParams() *RecordsIteratorParams
	OnAuthenticationSuccess()
	OnAuthenticationFailure(e *APIError) bool
	OnCreateRecordsIteratorSuccess()
	OnCreateRecordsIteratorFailure(e *APIError) bool
	OnGetRecordsSuccess(response *GetStreamRecordsResponse) bool
	OnGetRecordsFailure(apiError *APIError) bool
	OnUnexpectedError(apiError *APIError) bool
	OnStart()
	OnPause()
	OnResume()
	OnClose()
}
