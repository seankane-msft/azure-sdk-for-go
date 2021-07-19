// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package aztable

import (
	"fmt"
	"hash/fnv"
	"os"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/stretchr/testify/require"
)

func createServiceUrl(cosmos bool, t *testing.T) string {
	accountName, ok := os.LookupEnv("TABLES_STORAGE_ACCOUNT_NAME")
	if !ok {
		t.Error("No environment variable found for TABLES_STORAGE_ACCOUNT_NAME")
	}
	if cosmos {
		accountName, ok = os.LookupEnv("TABLES_COSMOS_ACCOUNT_NAME")
		if !ok {
			t.Error("No environment variable found for TABLES_COSMOS_ACCOUNT_NAME")
		}
		return fmt.Sprintf("https://%v.table.cosmos.azure.com", accountName)
	}
	return fmt.Sprintf("https://%v.table.core.windows.net", accountName)
}

func createRandomNameFromSeed(prefix, name string) string {
	h := fnv.New32a()
	h.Write([]byte(name))
	return fmt.Sprintf("%v%v", prefix, h.Sum32())
}

func deferredDelete(client *TableClient) {
	_, err := client.Delete(ctx)
	if err != nil {
		fmt.Printf("There was an error deleting the table. \n\t%v\n", err.Error())
	}
}

func requireNilError(r *require.Assertions, err error) {
	if err != nil {
		fmt.Println(err)
	}
	r.Nil(err)
}

func Test_AADCreateTable(t *testing.T) {
	r := require.New(t)
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	r.Nil(err)

	serviceUrl := createServiceUrl(false, t)
	tableName := createRandomNameFromSeed("table", t.Name())

	options := TableClientOptions{Scopes: []string{"https://storage.azure.com/.default"}}
	client, err := NewTableClient(tableName, serviceUrl, cred, &options)
	r.Nil(err)
	defer deferredDelete(client)

	_, err = client.Create(ctx)
	requireNilError(r, err)
}
