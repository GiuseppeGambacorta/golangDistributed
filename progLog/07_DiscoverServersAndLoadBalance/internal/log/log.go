package log

import (
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "Distribuiti/progLog/07_DiscoverServersAndLoadBalance/api/v1"
)

type Log struct {
	mu            sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, config Config) (*Log, error) {
	if config.Segment.MaxStoreBytes == 0 {
		config.Segment.MaxStoreBytes = 1024
	}
	if config.Segment.MaxIndexBytes == 0 {
		config.Segment.MaxIndexBytes = 1024
	}
	log := &Log{Dir: dir, Config: config}
	return log, log.setup()
}

func (log *Log) setup() error {
	files, err := os.ReadDir(log.Dir)
	if err != nil {
		return err
	}
	baseOffsetSet := make(map[uint64]struct{})
	for _, file := range files {
		baseOffsetText := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		baseOffset, err := strconv.ParseUint(baseOffsetText, 10, 64)
		if err != nil {
			continue
		}
		baseOffsetSet[baseOffset] = struct{}{}
	}
	baseOffsets := make([]uint64, 0, len(baseOffsetSet))
	for baseOffset := range baseOffsetSet {
		baseOffsets = append(baseOffsets, baseOffset)
	}
	sort.Slice(baseOffsets, func(i, j int) bool { return baseOffsets[i] < baseOffsets[j] })
	for _, baseOffset := range baseOffsets {
		if err := log.newSegment(baseOffset); err != nil {
			return err
		}
	}
	if len(log.segments) == 0 {
		if err := log.newSegment(log.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

func (log *Log) Append(record *api.Record) (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	offset, err := log.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if log.activeSegment.IsMaxed() {
		err = log.newSegment(offset + 1)
	}
	return offset, err
}

func (log *Log) Read(offset uint64) (*api.Record, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()
	var selectedSegment *segment
	for _, segment := range log.segments {
		if segment.baseOffset <= offset {
			selectedSegment = segment
		}
	}
	if selectedSegment == nil || selectedSegment.nextOffset <= offset {
		return nil, api.ErrOffsetOutOfRange{Offset: offset}
	}
	return selectedSegment.Read(offset)
}

func (log *Log) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()
	for _, segment := range log.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (log *Log) Remove() error {
	if err := log.Close(); err != nil {
		return err
	}
	return os.RemoveAll(log.Dir)
}

func (log *Log) Reset() error {
	if err := log.Remove(); err != nil {
		return err
	}
	if err := os.MkdirAll(log.Dir, 0755); err != nil {
		return err
	}
	log.segments = nil
	log.activeSegment = nil
	return log.setup()
}

func (log *Log) LowestOffset() (uint64, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()
	if len(log.segments) == 0 {
		return 0, io.EOF
	}
	return log.segments[0].baseOffset, nil
}

func (log *Log) HighestOffset() (uint64, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()
	if len(log.segments) == 0 {
		return 0, io.EOF
	}

	nextOffset := log.segments[len(log.segments)-1].nextOffset
	if nextOffset == 0 {
		return 0, nil
	}
	return nextOffset - 1, nil
}

// NextOffset returns the first offset not currently present in the log. It is
// useful as a starting point for pull replication.
func (log *Log) NextOffset() uint64 {
	log.mu.RLock()
	defer log.mu.RUnlock()
	if len(log.segments) == 0 {
		return log.Config.Segment.InitialOffset
	}
	return log.segments[len(log.segments)-1].nextOffset
}

func (log *Log) Truncate(lowest uint64) error {
	log.mu.Lock()
	defer log.mu.Unlock()
	remainingSegments := make([]*segment, 0, len(log.segments))
	for _, segment := range log.segments {
		if segment.nextOffset <= lowest+1 {
			if err := segment.Remove(); err != nil {
				return err
			}
			continue
		}
		remainingSegments = append(remainingSegments, segment)
	}
	log.segments = remainingSegments
	if len(remainingSegments) > 0 {
		log.activeSegment = remainingSegments[len(remainingSegments)-1]
	}
	return nil
}

func (log *Log) Reader() io.Reader {
	log.mu.RLock()
	defer log.mu.RUnlock()
	readers := make([]io.Reader, len(log.segments))
	for i, segment := range log.segments {
		readers[i] = &originReader{store: segment.store}
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	offset int64
}

func (reader *originReader) Read(buffer []byte) (int, error) {
	read, err := reader.ReadAt(buffer, reader.offset)
	reader.offset += int64(read)
	return read, err
}

func (log *Log) newSegment(offset uint64) error {
	segment, err := newSegment(log.Dir, offset, log.Config)
	if err != nil {
		return err
	}
	log.segments = append(log.segments, segment)
	log.activeSegment = segment
	return nil
}
