// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package aztable

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTableUrlParts(t *testing.T) {
	parts := TableURLParts{
		Scheme: "https",
		Host:   "fakename.table.core.windows.net",
	}
	require.Equal(t, parts.URL(), "https://fakename.table.core.windows.net")

	parts2 := TableURLParts{
		Scheme: "http",
		Host:   "fakename2.table.core.windows.net",
	}
	require.Equal(t, parts2.URL(), "http://fakename2.table.core.windows.net")

	sas := SASQueryParameters{
		signature:   "blanksig",
		startTime:   time.Now(),
		permissions: "rwld",
	}
	parts3 := TableURLParts{
		Scheme: "https",
		Host:   "fakename2.table.core.windows.net",
		SAS:    sas,
	}
	require.Equal(t, parts3.URL(), fmt.Sprintf("https://fakename2.table.core.windows.net/?%v", sas.Encode()))
}
