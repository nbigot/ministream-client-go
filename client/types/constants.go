package types

const JWTContextKey = "jwt"
const StatusSuccess = "success"
const ErrorCannotUnmarshalJson = "can't unmarshal json"
const ErrorUnexpected = "unexpected error"

const MaxPullRecordsByCall = 10000
const MaxPushRecordsByCall = 10000
const DefaultRecordsQueueLen = 10000

const ErrorDuplicatedBatchId = 1008
const ErrorCantGetMessagesFromStream = 1020

const ErrorJWTMissingOrMalformed = 1200
const ErrorJWTInvalidOrExpired = 1201
const ErrorJWTNotEnabled = 1202
const ErrorJWTRBACUnknownRole = 1210
const ErrorRBACInvalidRule = 1211
const ErrorRBACForbidden = 1212
const ErrorAuthInternalError = 1220
const ErrorWrongCredentials = 1230

const ErrorStreamIteratorNotFound = 1031
const ErrorStreamIteratorIsBusy = 1032
const ErrorHTTPTimeout = 2000
const ErrorTimeout = 2001
const ErrorContextDeadlineExceeded = 2002
const ErrorTransportReadFromServerError = 2003
const ErrorURL = 2004
const ErrorTooManyRequests = 2005
