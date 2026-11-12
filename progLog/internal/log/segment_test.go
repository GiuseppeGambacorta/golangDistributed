package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	api "Distribuiti/progLog/StructureDataWithProtobuf/api/v1"

	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, err := ioutil.TempDir("", "segment-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}
	config := Config{}
	config.Segment.MaxStoreBytes = 1024
	config.Segment.MaxIndexBytes = entryWidth * 3
	s, err := newSegment(dir, 16, config)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset)
	require.False(t, s.IsMaxed())

	for offset := uint64(0); offset < 3; offset++ {
		writtenOffset, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+offset, writtenOffset)
		got, err := s.Read(writtenOffset)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)
	require.True(t, s.IsMaxed())
	require.NoError(t, s.Close())

	config.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	config.Segment.MaxIndexBytes = 1024
	s, err = newSegment(dir, 16, config)
	require.NoError(t, err)
	require.True(t, s.IsMaxed())
	require.NoError(t, s.Remove())

	s, err = newSegment(dir, 16, config)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
	require.NoError(t, s.Close())
}
