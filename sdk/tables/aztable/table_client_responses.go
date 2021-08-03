// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package aztable

import (
	"net/http"
	"time"
)

type TableAddEntityResponse struct {
	ClientRequestID                 string
	RequestID                       string
	Version                         string
	Date                            time.Time
	ETag                            string
	XMSContinuationNextPartitionKey string
	XMSContinuationNextRowKey       string
	PreferenceApplied               string
	ContentType                     string
}

// TableMergeReplaceEntityResponse contains the response from method TableClient.UpdateEntity and TableClient.InsertEntity.
type TableMergeReplaceEntityResponse struct {
	// ClientRequestID contains the information returned from the x-ms-client-request-id header response.
	ClientRequestID *string

	// Date contains the information returned from the Date header response.
	Date *time.Time

	// ETag contains the information returned from the ETag header response.
	ETag *string

	// RawResponse contains the underlying HTTP response.
	RawResponse *http.Response

	// RequestID contains the information returned from the x-ms-request-id header response.
	RequestID *string

	// Version contains the information returned from the x-ms-version header response.
	Version *string
}

func castMergeEntityResponse(resp TableMergeEntityResponse) *TableMergeReplaceEntityResponse {
	return &TableMergeReplaceEntityResponse{
		ClientRequestID: resp.ClientRequestID,
		Date: resp.Date,
		ETag: resp.ETag,
		RawResponse: resp.RawResponse,
		RequestID: resp.RequestID,
		Version: resp.Version,
	}
}

func castUpdateEntityResponse(resp TableUpdateEntityResponse) *TableMergeReplaceEntityResponse {
	return &TableMergeReplaceEntityResponse{
		ClientRequestID: resp.ClientRequestID,
		Date: resp.Date,
		ETag: resp.ETag,
		RawResponse: resp.RawResponse,
		RequestID: resp.RequestID,
		Version: resp.Version,
	}
}