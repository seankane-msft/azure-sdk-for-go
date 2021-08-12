// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package aztable

import (
	"net"
	"net/url"
	"strings"
)

// TableURLParts is a structure for defining the service url of a tables account
type TableURLParts struct {
	Scheme              string // Ex: "https"
	Host                string // Ex: "account.blob.core.windows.net", "10.132.141.33", "10.132.141.33:80"
	IPEndpointStyleInfo IPEndpointStyleInfo
	SAS                 SASQueryParameters
	UnparsedParams      string
}

// IPEndpointStyleInfo is used for IP endpoint style URL when working with Azure storage emulator.
// Ex: "https://10.132.141.33/accountname/containername"
type IPEndpointStyleInfo struct {
	AccountName string // "" if not using IP endpoint style
}

// isIPEndpointStyle checkes if URL's host is IP, in this case the storage account endpoint will be composed as:
// http(s)://IP(:port)/storageaccount/container/...
// As url's Host property, host could be both host or host:port
func isIPEndpointStyle(host string) bool {
	if host == "" {
		return false
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	// For IPv6, there could be case where SplitHostPort fails for cannot finding port.
	// In this case, eliminate the '[' and ']' in the URL.
	// For details about IPv6 URL, please refer to https://tools.ietf.org/html/rfc2732
	if host[0] == '[' && host[len(host)-1] == ']' {
		host = host[1 : len(host)-1]
	}
	return net.ParseIP(host) != nil
}

// NewTableURLParts parses a URL initializing TableURLParts' fields including any SAS-related parameters. Any other
// query parameters remain in the UnparsedParams field. This method overwrites all fields in the TableURLParts object.
func NewTableURLParts(u string) TableURLParts {
	uri, _ := url.Parse(u)

	up := TableURLParts{
		Scheme: uri.Scheme,
		Host:   uri.Host,
	}

	// Find the container & blob names (if any)
	if uri.Path != "" {
		path := uri.Path
		if path[0] == '/' {
			path = path[1:] // If path starts with a slash, remove it
		}
		if isIPEndpointStyle(up.Host) {
			if accountEndIndex := strings.Index(path, "/"); accountEndIndex == -1 { // Slash not found; path has account name & no container name or blob
				up.IPEndpointStyleInfo.AccountName = path
			} else {
				up.IPEndpointStyleInfo.AccountName = path[:accountEndIndex] // The account name is the part between the slashes
				path = path[accountEndIndex+1:]                             // path refers to portion after the account name now (container & blob names)
			}
		}
	}

	// Convert the query parameters to a case-sensitive map & trim whitespace
	paramsMap := uri.Query()

	up.SAS = newSASQueryParameters(paramsMap, true)
	up.UnparsedParams = paramsMap.Encode()
	return up
}

// URL returns a URL object whose fields are initialized from the TableURLParts fields. The URL's RawQuery
// field contains the SAS, snapshot, and unparsed query parameters.
func (up TableURLParts) URL() string {
	path := ""
	if isIPEndpointStyle(up.Host) && up.IPEndpointStyleInfo.AccountName != "" {
		path += "/" + up.IPEndpointStyleInfo.AccountName
	}
	rawQuery := up.UnparsedParams

	sas := up.SAS.Encode()
	if sas != "" {
		if len(rawQuery) > 0 {
			rawQuery += "&"
		}
		rawQuery += sas

		if !strings.HasSuffix(path, "/") {
			path += "/"
		}
	}
	u := url.URL{
		Scheme:   up.Scheme,
		Host:     up.Host,
		Path:     path,
		RawQuery: rawQuery,
	}
	return u.String()
}
