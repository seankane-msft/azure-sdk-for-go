// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package aztable

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/internal/recording"
	"github.com/Azure/azure-sdk-for-go/sdk/internal/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/internal/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type tableClientLiveTests struct {
	suite.Suite
	endpointType EndpointType
	mode         recording.RecordMode
}

// Hookup to the testing framework
func TestTableClient_Storage(t *testing.T) {
	storage := tableClientLiveTests{endpointType: StorageEndpoint, mode: recording.Playback /* change to Record to re-record tests */}
	suite.Run(t, &storage)
}

// Hookup to the testing framework
func TestTableClient_Cosmos(t *testing.T) {
	cosmos := tableClientLiveTests{endpointType: CosmosEndpoint, mode: recording.Playback /* change to Record to re-record tests */}
	suite.Run(t, &cosmos)
}

func (s *tableClientLiveTests) TestServiceErrors() {
	client, delete := s.init(true)
	defer delete()

	// Create a duplicate table to produce an error
	_, err := client.Create(ctx)
	var svcErr *runtime.ResponseError
	errors.As(err, &svcErr)
	require.Equal(s.T(), svcErr.RawResponse().StatusCode, http.StatusConflict)
}

func (s *tableClientLiveTests) TestCreateTable() {
	require := require.New(s.T())
	client, delete := s.init(false)
	defer delete()

	resp, err := client.Create(ctx)

	require.NoError(err)
	require.Equal(*resp.TableResponse.TableName, client.Name)
}

func (s *tableClientLiveTests) TestAddEntity() {
	require := require.New(s.T())
	client, delete := s.init(true)
	defer delete()

	entitiesToCreate := createSimpleEntities(1, "partition")

	_, err := client.AddEntity(ctx, (*entitiesToCreate)[0])
	require.NoError(err)
}

func (s *tableClientLiveTests) TestAddComplexEntity() {
	require := require.New(s.T())
	context := getTestContext(s.T().Name())
	client, delete := s.init(true)
	defer delete()

	entitiesToCreate := createComplexEntities(context, 1, "partition")

	for _, e := range *entitiesToCreate {
		_, err := client.AddEntity(ctx, e)
		var svcErr *runtime.ResponseError
		errors.As(err, &svcErr)
		require.Nilf(err, getStringFromBody(svcErr))
	}
}

func (s *tableClientLiveTests) TestDeleteEntity() {
	require := require.New(s.T())
	client, delete := s.init(true)
	defer delete()

	entitiesToCreate := createSimpleEntities(1, "partition")

	_, err := client.AddEntity(ctx, (*entitiesToCreate)[0])
	require.NoError(err)
	_, delErr := client.DeleteEntity(ctx, (*entitiesToCreate)[0][partitionKey].(string), (*entitiesToCreate)[0][rowKey].(string), "*")
	require.NoError(delErr)
}

func (s *tableClientLiveTests) TestMergeEntity() {
	require := require.New(s.T())
	client, delete := s.init(true)
	defer delete()

	entitiesToCreate := createSimpleEntities(1, "partition")

	_, err := client.AddEntity(ctx, (*entitiesToCreate)[0])
	require.NoError(err)

	var qResp TableEntityQueryResponseResponse
	filter := "RowKey eq '1'"
	pager := client.Query(&QueryOptions{Filter: &filter})
	for pager.NextPage(ctx) {
		qResp = pager.PageResponse()
	}
	preMerge := qResp.TableEntityQueryResponse.Value[0]

	mergeProp := "MergeProperty"
	val := "foo"
	var mergeProperty = map[string]interface{}{
		partitionKey: (*entitiesToCreate)[0][partitionKey],
		rowKey:       (*entitiesToCreate)[0][rowKey],
		mergeProp:    val,
	}

	_, updateErr := client.UpdateEntity(ctx, mergeProperty, nil, Merge)
	require.NoError(updateErr)

	pager = client.Query(&QueryOptions{Filter: &filter})
	for pager.NextPage(ctx) {
		qResp = pager.PageResponse()
	}
	postMerge := qResp.TableEntityQueryResponse.Value[0]

	// The merged entity has all its properties + the merged property
	require.Equalf(len(preMerge)+1, len(postMerge), "postMerge should have one more property than preMerge")
	require.Equalf(postMerge[mergeProp], val, "%s property should equal %s", mergeProp, val)
}

func (s *tableClientLiveTests) TestUpsertEntity() {
	require := require.New(s.T())
	client, delete := s.init(true)
	defer delete()

	entitiesToCreate := createSimpleEntities(1, "partition")

	_, err := client.UpsertEntity(ctx, (*entitiesToCreate)[0], Replace)
	require.Nil(err)

	var qResp TableEntityQueryResponseResponse
	filter := "RowKey eq '1'"
	pager := client.Query(&QueryOptions{Filter: &filter})
	for pager.NextPage(ctx) {
		qResp = pager.PageResponse()
	}
	preMerge := qResp.TableEntityQueryResponse.Value[0]

	mergeProp := "MergeProperty"
	val := "foo"
	var mergeProperty = map[string]interface{}{
		partitionKey: (*entitiesToCreate)[0][partitionKey],
		rowKey:       (*entitiesToCreate)[0][rowKey],
		mergeProp:    val,
	}

	_, updateErr := client.UpsertEntity(ctx, mergeProperty, Replace)
	require.Nil(updateErr)

	pager = client.Query(&QueryOptions{Filter: &filter})
	for pager.NextPage(ctx) {
		qResp = pager.PageResponse()
	}
	postMerge := qResp.TableEntityQueryResponse.Value[0]

	// The merged entity has only the standard properties + the merged property
	require.Greater(len(preMerge), len(postMerge), "postMerge should have fewer properties than preMerge")
	require.Equalf(postMerge[mergeProp], val, "%s property should equal %s", mergeProp, val)
}

func (s *tableClientLiveTests) TestGetEntity() {
	require := require.New(s.T())
	client, delete := s.init(true)
	defer delete()

	// Add 5 entities
	entitiesToCreate := createSimpleEntities(1, "partition")
	for _, e := range *entitiesToCreate {
		_, err := client.AddEntity(ctx, e)
		require.NoError(err)
	}

	resp, err := client.GetEntity(ctx, "partition", "1")
	require.Nil(err)
	e := resp.Value
	_, ok := e[partitionKey].(string)
	require.True(ok)
	_, ok = e[rowKey].(string)
	require.True(ok)
	_, ok = e[timestamp].(string)
	require.True(ok)
	_, ok = e[etagOdata].(string)
	require.True(ok)
	_, ok = e["StringProp"].(string)
	require.True(ok)
	//TODO: fix when serialization is implemented
	_, ok = e["IntProp"].(float64)
	require.True(ok)
	_, ok = e["BoolProp"].(bool)
	require.True(ok)
}

func (s *tableClientLiveTests) TestQuerySimpleEntity() {
	require := require.New(s.T())
	client, delete := s.init(true)
	defer delete()

	// Add 5 entities
	entitiesToCreate := createSimpleEntities(5, "partition")
	for _, e := range *entitiesToCreate {
		_, err := client.AddEntity(ctx, e)
		require.NoError(err)
	}

	filter := "RowKey lt '5'"
	expectedCount := 4
	var resp TableEntityQueryResponseResponse
	var models []simpleEntity
	pager := client.Query(&QueryOptions{Filter: &filter})
	for pager.NextPage(ctx) {
		resp = pager.PageResponse()
		models = make([]simpleEntity, len(resp.TableEntityQueryResponse.Value))
		err := resp.TableEntityQueryResponse.AsModels(&models)
		require.NoError(err)
		require.Equal(len(resp.TableEntityQueryResponse.Value), expectedCount)
	}
	resp = pager.PageResponse()
	require.Nil(pager.Err())
	for i, e := range resp.TableEntityQueryResponse.Value {
		_, ok := e[partitionKey].(string)
		require.True(ok)
		require.Equal(e[partitionKey], models[i].PartitionKey)
		_, ok = e[rowKey].(string)
		require.True(ok)
		require.Equal(e[rowKey], models[i].RowKey)
		_, ok = e[timestamp].(string)
		require.True(ok)
		_, ok = e[etagOdata].(string)
		require.True(ok)
		require.Equal(e[etagOdata], models[i].ETag)
		_, ok = e["StringProp"].(string)
		require.True(ok)
		//TODO: fix when serialization is implemented
		_, ok = e["IntProp"].(float64)
		require.Equal(int(e["IntProp"].(float64)), models[i].IntProp)
		require.True(ok)
		_, ok = e["BoolProp"].(bool)
		require.Equal((*entitiesToCreate)[i]["BoolProp"], e["BoolProp"])
		require.Equal(e["BoolProp"], models[i].BoolProp)
		require.True(ok)
	}
}

func (s *tableClientLiveTests) TestQueryComplexEntity() {
	require := require.New(s.T())
	context := getTestContext(s.T().Name())
	client, delete := s.init(true)
	defer delete()

	// Add 5 entities
	entitiesToCreate := createComplexMapEntities(context, 5, "partition")
	for _, e := range *entitiesToCreate {
		_, err := client.AddEntity(ctx, e)
		require.NoError(err)
	}

	filter := "RowKey lt '5'"
	expectedCount := 4
	var resp TableEntityQueryResponseResponse
	pager := client.Query(&QueryOptions{Filter: &filter})
	for pager.NextPage(ctx) {
		resp = pager.PageResponse()
		require.Equal(expectedCount, len(resp.TableEntityQueryResponse.Value))
	}
	resp = pager.PageResponse()
	require.NoError(pager.Err())
	for _, e := range resp.TableEntityQueryResponse.Value {
		_, ok := e[partitionKey].(string)
		require.True(ok)
		_, ok = e[rowKey].(string)
		require.True(ok)
		_, ok = e[timestamp].(string)
		require.True(ok)
		_, ok = e[etagOdata].(string)
		require.True(ok)
		_, ok = e["StringProp"].(string)
		require.True(ok)
		//TODO: fix when serialization is implemented
		_, ok = e["IntProp"].(float64)
		require.True(ok)
		_, ok = e["BoolProp"].(bool)
		require.True(ok)
		_, ok = e["SomeBinaryProperty"].([]byte)
		require.True(ok)
		_, ok = e["SomeDateProperty"].(time.Time)
		require.True(ok)
		_, ok = e["SomeDoubleProperty0"].(float64)
		require.True(ok)
		_, ok = e["SomeDoubleProperty1"].(float64)
		require.True(ok)
		_, ok = e["SomeGuidProperty"].(uuid.UUID)
		require.True(ok)
		_, ok = e["SomeInt64Property"].(int64)
		require.True(ok)
		//TODO: fix when serialization is implemented
		_, ok = e["SomeIntProperty"].(float64)
		require.True(ok)
		_, ok = e["SomeStringProperty"].(string)
		require.True(ok)
	}
}

func (s *tableClientLiveTests) TestBatchAdd() {
	require := require.New(s.T())
	context := getTestContext(s.T().Name())
	client, delete := s.init(true)
	defer delete()

	entitiesToCreate := createComplexMapEntities(context, 10, "partition")
	batch := make([]TableTransactionAction, 10)

	for i, e := range *entitiesToCreate {
		batch[i] = TableTransactionAction{ActionType: Add, Entity: e}
	}

	resp, err := client.submitTransactionInternal(ctx, &batch, context.recording.UUID(), context.recording.UUID(), nil)
	require.NoError(err)
	for i := 0; i < len(*resp.TransactionResponses); i++ {
		r := (*resp.TransactionResponses)[i]
		require.Equal(r.StatusCode, http.StatusNoContent)
	}
}

func (s *tableClientLiveTests) TestBatchMixed() {
	require := require.New(s.T())
	context := getTestContext(s.T().Name())
	client, delete := s.init(true)
	defer delete()

	entitiesToCreate := createComplexMapEntities(context, 5, "partition")
	batch := make([]TableTransactionAction, 3)

	// Add the first 3 entities.
	for i := range batch {
		batch[i] = TableTransactionAction{ActionType: Add, Entity: (*entitiesToCreate)[i]}
	}

	resp, err := client.submitTransactionInternal(ctx, &batch, context.recording.UUID(), context.recording.UUID(), nil)
	require.Nil(err)
	for i := 0; i < len(*resp.TransactionResponses); i++ {
		r := (*resp.TransactionResponses)[i]
		require.Equal(http.StatusNoContent, r.StatusCode)
	}

	var qResp TableEntityQueryResponseResponse
	filter := "RowKey eq '1'"
	pager := client.Query(&QueryOptions{Filter: &filter})
	for pager.NextPage(ctx) {
		qResp = pager.PageResponse()
	}
	preMerge := qResp.TableEntityQueryResponse.Value[0]

	// create a new batch slice.
	batch = make([]TableTransactionAction, 5)

	// create a merge action for the first added entity
	mergeProp := "MergeProperty"
	val := "foo"
	var mergeProperty = map[string]interface{}{
		partitionKey: (*entitiesToCreate)[0][partitionKey],
		rowKey:       (*entitiesToCreate)[0][rowKey],
		mergeProp:    val,
	}
	batch[0] = TableTransactionAction{ActionType: UpdateMerge, Entity: mergeProperty, ETag: (*resp.TransactionResponses)[0].Header.Get(etag)}

	// create a delete action for the second added entity
	batch[1] = TableTransactionAction{ActionType: Delete, Entity: (*entitiesToCreate)[1]}

	// create an upsert action to replace the third added entity with a new value
	replaceProp := "ReplaceProperty"
	var replaceProperties = map[string]interface{}{
		partitionKey: (*entitiesToCreate)[2][partitionKey],
		rowKey:       (*entitiesToCreate)[2][rowKey],
		replaceProp:  val,
	}
	batch[2] = TableTransactionAction{ActionType: UpsertReplace, Entity: replaceProperties}

	// Add the remaining 2 entities.
	batch[3] = TableTransactionAction{ActionType: Add, Entity: (*entitiesToCreate)[3]}
	batch[4] = TableTransactionAction{ActionType: Add, Entity: (*entitiesToCreate)[4]}

	//batch = batch[1:]

	resp, err = client.submitTransactionInternal(ctx, &batch, context.recording.UUID(), context.recording.UUID(), nil)
	require.Nil(err)

	for i := 0; i < len(*resp.TransactionResponses); i++ {
		r := (*resp.TransactionResponses)[i]
		require.Equal(http.StatusNoContent, r.StatusCode)

	}

	pager = client.Query(&QueryOptions{Filter: &filter})
	for pager.NextPage(ctx) {
		qResp = pager.PageResponse()
	}
	postMerge := qResp.TableEntityQueryResponse.Value[0]

	// The merged entity has all its properties + the merged property
	require.Equalf(len(preMerge)+1, len(postMerge), "postMerge should have one more property than preMerge")
	require.Equalf(postMerge[mergeProp], val, "%s property should equal %s", mergeProp, val)
}

func (s *tableClientLiveTests) TestBatchError() {
	require := require.New(s.T())
	context := getTestContext(s.T().Name())
	client, delete := s.init(true)
	defer delete()

	entitiesToCreate := createComplexMapEntities(context, 3, "partition")

	// Create the batch.
	batch := make([]TableTransactionAction, 0, 3)

	// Sending an empty batch throws.
	_, err := client.submitTransactionInternal(ctx, &batch, context.recording.UUID(), context.recording.UUID(), nil)
	require.Error(err)
	require.Equal(error_empty_transaction, err.Error())

	// Add the last entity to the table prior to adding it as part of the batch to cause a batch failure.
	_, err = client.AddEntity(ctx, (*entitiesToCreate)[2])
	require.NoError(err)

	// Add the entities to the batch
	for i := 0; i < cap(batch); i++ {
		batch = append(batch, TableTransactionAction{ActionType: Add, Entity: (*entitiesToCreate)[i]})
	}

	resp, err := client.submitTransactionInternal(ctx, &batch, context.recording.UUID(), context.recording.UUID(), nil)
	require.Error(err)
	te, ok := err.(*TableTransactionError)
	require.Truef(ok, "err should be of type TableTransactionError")
	require.Equal("EntityAlreadyExists", te.OdataError.Code)
	require.Equal(2, te.FailedEntityIndex)
	require.Equal(http.StatusConflict, (*resp.TransactionResponses)[0].StatusCode)
}

func (s *tableClientLiveTests) TestInvalidEntity() {
	require := require.New(s.T())
	client, delete := s.init(true)
	defer delete()

	badEntity := &map[string]interface{}{
		"Value":  10,
		"String": "stringystring",
	}

	_, err := client.AddEntity(ctx, *badEntity)

	require.Error(err)
	require.Contains(err.Error(), partitionKeyRowKeyError.Error())
}

// setup the test environment
func (s *tableClientLiveTests) BeforeTest(suite string, test string) {
	recordedTestSetup(s.T(), s.T().Name(), s.endpointType, s.mode)
}

// teardown the test context
func (s *tableClientLiveTests) AfterTest(suite string, test string) {
	recordedTestTeardown(s.T().Name())
}

func (s *tableClientLiveTests) init(doCreate bool) (*TableClient, func()) {
	require := require.New(s.T())
	context := getTestContext(s.T().Name())
	tableName, _ := getTableName(context)
	client := context.client.NewTableClient(tableName)
	if doCreate {
		_, err := client.Create(ctx)
		require.NoError(err)
	}
	return client, func() {
		_, err := client.Delete(ctx)
		if err != nil {
			fmt.Printf("Error deleting table. %v\n", err.Error())
		}
	}
}

func getStringFromBody(e *runtime.ResponseError) string {
	if e == nil {
		return "Error is nil"
	}
	r := e.RawResponse()
	body := bytes.Buffer{}
	b := r.Body
	b.Close()
	if b != nil {
		_, err := body.ReadFrom(b)
		if err != nil {
			return "<emtpy body>"
		}
		_ = ioutil.NopCloser(&body)
	}
	return body.String()
}
