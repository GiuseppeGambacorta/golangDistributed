package main

import "sync"

type Store struct {
	data map[string]string
	mu   sync.RWMutex
	wal  *WAL
}

func NewStore(walPath string) (*Store, error) {
	store := &Store{
		data: make(map[string]string),
	}

	if walPath != "" {
		wal, err := NewWAL(walPath)
		if err != nil {
			return nil, err
		}
		store.wal = wal

		err = ReplayWAL(walPath, store)
		if err != nil {
			return nil, err
		}
	}

	return store, nil
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.wal != nil {
		s.wal.WriteEntry("SET", key, value)
	}

	s.data[key] = value
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.wal != nil {
		s.wal.WriteEntry("DELETE", key, "")
	}

	delete(s.data, key)
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, exists := s.data[key]
	return value, exists
}

func (s *Store) getAllKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

func (s *Store) Close() {
	if s.wal != nil {
		s.wal.Close()
	}
}
