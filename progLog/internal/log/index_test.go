package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	indexFile, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(indexFile.Name())

	config := Config{}
	config.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(indexFile, config)
	require.NoError(t, err)
	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, indexFile.Name(), idx.Name())

	entries := []struct {
		offset   uint32
		position uint64
	}{
		{offset: 0, position: 0},
		{offset: 1, position: 10},
	}

	for _, want := range entries {
		err = idx.Write(want.offset, want.position)
		require.NoError(t, err)
		_, position, err := idx.Read(int64(want.offset))
		require.NoError(t, err)
		require.Equal(t, want.position, position)
	}

	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	require.NoError(t, idx.Close())

	indexFile, err = os.OpenFile(indexFile.Name(), os.O_RDWR, 0600)
	require.NoError(t, err)
	idx, err = newIndex(indexFile, config)
	require.NoError(t, err)
	offset, position, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), offset)
	require.Equal(t, entries[1].position, position)
	require.NoError(t, idx.Close())
}
