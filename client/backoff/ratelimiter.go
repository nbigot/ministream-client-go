package backoff

import (
	"net/http"
	"strconv"
)

const (
	// X-RateLimit-* headers
	xRateLimitLimit     = "X-RateLimit-Limit"
	xRateLimitRemaining = "X-RateLimit-Remaining"
	xRateLimitReset     = "X-RateLimit-Reset"
	HeaderRetryAfter    = "Retry-After"
)

type RateLimit struct {
	Limit      int
	Remaining  int
	ResetInSec uint64
	RetryAfter uint64
}

func RateLimitFromHttpResponse(resp *http.Response) *RateLimit {
	limit := resp.Header.Get(xRateLimitLimit)
	remaining := resp.Header.Get(xRateLimitRemaining)
	reset := resp.Header.Get(xRateLimitReset)
	retryAfter := resp.Header.Get(HeaderRetryAfter)

	iLimit, _ := strconv.Atoi(limit)
	iRemaining, _ := strconv.Atoi(remaining)
	iResetInSec, _ := strconv.ParseUint(reset, 10, 0)
	iRetryAfter, _ := strconv.ParseUint(retryAfter, 10, 0)

	return &RateLimit{
		Limit:      iLimit,
		Remaining:  iRemaining,
		ResetInSec: iResetInSec,
		RetryAfter: iRetryAfter,
	}
}
