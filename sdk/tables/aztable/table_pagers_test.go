// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package aztable

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/internal/uuid"
	"github.com/stretchr/testify/require"
)

func TestCastAndRemoveAnnotations(t *testing.T) {
	require := require.New(t)

	r := &http.Response{Body: closerFromString(complexPayload)}
	resp := azcore.Response{Response: r}

	var val map[string]interface{}
	err := resp.UnmarshalAsJSON(&val)
	require.NoError(err)
	err = castAndRemoveAnnotations(&val)
	require.NoError(err)
	// assert all odata annotations are removed.
	for k := range val {
		require.NotContains(k, OdataType)
	}

	require.IsType(time.Now(), val["SomeDateProperty"])
	require.IsType([]byte{}, val["SomeBinaryProperty"])
	require.IsType(float64(0), val["SomeDoubleProperty0"])
}

func TestToOdataAnnotatedDictionary(t *testing.T) {
	require := require.New(t)

	var val = createComplexEntityMap()
	err := toOdataAnnotatedDictionary(&val)
	require.NoError(err)
	// assert all odata annotations are removed.
	for k := range odataHintProps {
		_, ok := val[k]
		require.Truef(ok, fmt.Sprintf("map does not contain %s", k))
		iSuffix := strings.Index(k, OdataType)
		if iSuffix > 0 {
			// Get the name of the property that this odataType key describes.
			valueKey := k[0:iSuffix]
			if !strings.Contains(valueKey, "SomeDoubleProperty") {
				require.IsTypef("", val[valueKey], fmt.Sprintf("should be type string %s", valueKey))
			}
		}
		_, ok = val[odataType(k)]
		require.Truef(ok, fmt.Sprintf("map does not contain %s", odataType(k)))
	}
}

func BenchmarkUnMarshal_AsJson_CastAndRemove_Map(b *testing.B) {
	require := require.New(b)
	b.ReportAllocs()
	bt := []byte(complexPayload)
	for i := 0; i < b.N; i++ {
		var val = make(map[string]interface{})
		err := json.Unmarshal(bt, &val)
		require.NoError(err)
		err = castAndRemoveAnnotations(&val)
		require.NoError(err)
		require.Equal("somePartition", val["PartitionKey"])
	}
}

func BenchmarkUnMarshal_FromMap_Entity(b *testing.B) {
	require := require.New(b)

	bt := []byte(complexPayload)
	for i := 0; i < b.N; i++ {
		var val = make(map[string]interface{})
		err := json.Unmarshal(bt, &val)
		if err != nil {
			panic(err)
		}
		result := complexEntity{}
		err = EntityMapAsModel(val, &result)
		require.NoError(err)
		require.Equal("somePartition", result.PartitionKey)
	}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func BenchmarkMarshal_Entity_ToMap_ToOdataDict_Map(b *testing.B) {
	ent := createComplexEntity()
	for i := 0; i < b.N; i++ {
		m, _ := toMap(ent)
		err := toOdataAnnotatedDictionary(m)
		check(err)
		_, err = json.Marshal(m)
		check(err)
	}
}

func BenchmarkMarshal_Map_ToOdataDict_Map(b *testing.B) {
	ent := createComplexEntityMap()
	for i := 0; i < b.N; i++ {
		err := toOdataAnnotatedDictionary(&ent)
		check(err)
		_, err = json.Marshal(ent)
		check(err)
	}
}

func TestToMap(t *testing.T) {
	require := require.New(t)

	ent := createComplexEntity()

	entMap, err := toMap(ent)
	require.NoError(err)

	// Validate that we have all the @odata.type properties for types []byte, int64, float64, time.Time, and uuid
	for k, v := range odataHintProps {
		vv, ok := (*entMap)[odataType(k)]
		require.Truef(ok, "Should have found map key of name '%s'", odataType(k))
		require.Equal(v, vv)
	}

	// validate all the types were properly casted / converted
	require.Equal(ent.PartitionKey, (*entMap)["PartitionKey"])
	require.Equal(ent.RowKey, (*entMap)["RowKey"])
	require.Equal(base64.StdEncoding.EncodeToString(ent.SomeBinaryProperty), string((*entMap)["SomeBinaryProperty"].(string)))
	ts, _ := time.Parse(ISO8601, (*entMap)["SomeDateProperty"].(string))
	require.Equal(ent.SomeDateProperty.UTC().Format(ISO8601), ts.Format(ISO8601))
	require.Equal(ent.SomeDoubleProperty0, (*entMap)["SomeDoubleProperty0"])
	require.Equal(ent.SomeDoubleProperty1, (*entMap)["SomeDoubleProperty1"])
	var u uuid.UUID = ent.SomeGuidProperty
	require.Equal(u.String(), (*entMap)["SomeGuidProperty"].(string))
	require.Equal(strconv.FormatInt(ent.SomeInt64Property, 10), (*entMap)["SomeInt64Property"].(string))
	require.Equal(ent.SomeIntProperty, (*entMap)["SomeIntProperty"])
	require.Equal(ent.SomeStringProperty, (*entMap)["SomeStringProperty"])
	require.Equal(*ent.SomePtrStringProperty, (*entMap)["SomePtrStringProperty"])
}

func TestToMapWithMap(t *testing.T) {
	require := require.New(t)

	ent := createComplexEntityMap()

	entMap, err := toMap(ent)
	require.NoError(err)

	// Validate that we have all the @odata.type properties for types []byte, int64, float64, time.Time, and uuid
	for k, v := range odataHintProps {
		vv, ok := (*entMap)[odataType(k)]
		require.Truef(ok, "Should have found map key of name '%s'", odataType(k))
		require.Equal(v, vv)
	}

	require.Equal(&ent, entMap)
}

func TestEntitySerialization(t *testing.T) {
	require := require.New(t)

	ent := createComplexEntity()

	b, err := json.Marshal(ent)
	require.NoError(err)
	require.NotEmpty(b)
	s := string(b)
	//require.FailNow(s)
	require.NotEmpty(s)
}

func TestDeserializeFromMap(t *testing.T) {
	require := require.New(t)

	expected := createComplexEntity()
	bt := []byte(complexPayload)
	var val = make(map[string]interface{})
	err := json.Unmarshal(bt, &val)
	require.NoError(err)
	result := complexEntity{}
	// tt := reflect.TypeOf(complexEntity{})
	// err := fromMap(tt, getTypeValueMap(tt), &val, reflect.ValueOf(&result).Elem())
	err = EntityMapAsModel(val, &result)
	require.NoError(err)
	require.EqualValues(expected, result)
}

func createComplexEntity() complexEntity {
	sp := "some pointer to string"
	t, _ := time.Parse(ISO8601, "2021-03-23T18:29:15.9686039Z")
	t2, _ := time.Parse(ISO8601, "2020-01-01T01:02:00Z")
	b, _ := base64.StdEncoding.DecodeString("AQIDBAU=")
	var e = complexEntity{
		PartitionKey:          "somePartition",
		ETag:                  "W/\"datetime'2021-04-05T05%3A02%3A40.7371784Z'\"",
		RowKey:                "01",
		Timestamp:             t,
		SomeBinaryProperty:    b,
		SomeDateProperty:      t2,
		SomeDoubleProperty0:   float64(1.0),
		SomeDoubleProperty1:   float64(1.5),
		SomeGuidProperty:      uuid.Parse("0d391d16-97f1-4b9a-be68-4cc871f90001"),
		SomeInt64Property:     int64(math.MaxInt64),
		SomeIntProperty:       42,
		SomeStringProperty:    "This is table entity number 01",
		SomePtrStringProperty: &sp}
	return e
}

func createComplexEntityMap() map[string]interface{} {
	sp := "some pointer to string"
	t, _ := time.Parse(ISO8601, "2021-03-23T18:29:15.9686039Z")
	t2, _ := time.Parse(ISO8601, "2020-01-01T01:02:00Z")
	b, _ := base64.StdEncoding.DecodeString("AQIDBAU=")
	var e = map[string]interface{}{
		"PartitionKey":          "somePartition",
		"ETag":                  "W/\"datetime'2021-04-05T05%3A02%3A40.7371784Z'\"",
		"RowKey":                "01",
		"Timestamp":             t,
		"SomeBinaryProperty":    b,
		"SomeDateProperty":      t2,
		"SomeDoubleProperty0":   float64(1.0),
		"SomeDoubleProperty1":   float64(1.5),
		"SomeGuidProperty":      uuid.Parse("0d391d16-97f1-4b9a-be68-4cc871f90001"),
		"SomeInt64Property":     int64(math.MaxInt64),
		"SomeIntProperty":       42,
		"SomeStringProperty":    "This is table entity number 01",
		"SomePtrStringProperty": &sp}
	return e
}

func closerFromString(content string) io.ReadCloser {
	return ioutil.NopCloser(strings.NewReader(content))
}

var odataHintProps = map[string]string{
	"SomeBinaryProperty":  edmBinary,
	"SomeDateProperty":    edmDateTime,
	"SomeDoubleProperty0": edmDouble,
	"SomeDoubleProperty1": edmDouble,
	"SomeGuidProperty":    edmGuid,
	"SomeInt64Property":   edmInt64}

var complexPayload = "{\"odata.etag\": \"W/\\\"datetime'2021-04-05T05%3A02%3A40.7371784Z'\\\"\"," +
	"\"PartitionKey\": \"somePartition\"," +
	"\"RowKey\": \"01\"," +
	"\"Timestamp\": \"2021-03-23T18:29:15.9686039Z\"," +
	"\"SomeBinaryProperty@odata.type\": \"Edm.Binary\"," +
	"\"SomeBinaryProperty\": \"AQIDBAU=\"," +
	"\"SomeDateProperty@odata.type\": \"Edm.DateTime\"," +
	"\"SomeDateProperty\": \"2020-01-01T01:02:00Z\"," +
	"\"SomeDoubleProperty0\": 1.0," +
	"\"SomeDoubleProperty1\": 1.5," +
	"\"SomeGuidProperty@odata.type\": \"Edm.Guid\"," +
	"\"SomeGuidProperty\": \"0d391d16-97f1-4b9a-be68-4cc871f90001\"," +
	"\"SomeInt64Property@odata.type\": \"Edm.Int64\"," +
	"\"SomeInt64Property\": \"" + strconv.FormatInt(math.MaxInt64, 10) + "\"," +
	"\"SomeIntProperty\": 42," +
	"\"SomeStringProperty\": \"This is table entity number 01\"," +
	"\"SomePtrStringProperty\": \"some pointer to string\"  }"
