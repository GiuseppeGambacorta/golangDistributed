package log

import (
	"io/ioutil"
	"os"
	"testing"

	api "Distribuiti/progLog/01_StructureDataWithProtobuf/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	scenarios := map[string]func(t *testing.T, log *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	}

	for scenario, test := range scenarios {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			config := Config{}
			config.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, config)
			require.NoError(t, err)
			t.Cleanup(func() { _ = log.Close() })
			test(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	appended := &api.Record{Value: []byte("hello world")}
	offset, err := log.Append(appended)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	read, err := log.Read(offset)
	require.NoError(t, err)
	require.Equal(t, appended.Value, read.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	require.Error(t, err)
}

func testInitExisting(t *testing.T, original *Log) {
	appended := &api.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		_, err := original.Append(appended)
		require.NoError(t, err)
	}
	require.NoError(t, original.Close())

	offset, err := original.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)
	offset, err = original.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), offset)

	reopened, err := NewLog(original.Dir, original.Config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	offset, err = reopened.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)
	offset, err = reopened.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), offset)
}

func testReader(t *testing.T, log *Log) {
	appended := &api.Record{Value: []byte("hello world")}
	offset, err := log.Append(appended)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	reader := log.Reader()
	data, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	read := &api.Record{}
	require.NoError(t, proto.Unmarshal(data[SizeOfLenghtData:], read))
	require.Equal(t, appended.Value, read.Value)
}

func testTruncate(t *testing.T, log *Log) {
	appended := &api.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		_, err := log.Append(appended)
		require.NoError(t, err)
	}

	require.NoError(t, log.Truncate(1))
	_, err := log.Read(0)
	require.Error(t, err)
}
