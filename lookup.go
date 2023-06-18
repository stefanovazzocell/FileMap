package filemap

import (
	"encoding/json"
	"errors"
)

var (
	// Error returned when looking up a key that doesn't exist
	ErrNotFound = errors.New("key not found")
)

// Looks up a single value in the FM
func (fm *FileMap[K, V]) Lookup(key K) (*V, error) {
	// Search index
	fm.indexMu.RLock()
	defer fm.indexMu.RUnlock()
	position, ok := fm.index[key]
	if !ok {
		return nil, ErrNotFound
	}
	// Search fm file
	fm.fileMu.Lock()
	defer fm.fileMu.Unlock()
	if _, err := fm.file.Seek(position, 0); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(fm.file)
	var value V
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return &value, nil
}

// Looks up a single value in the FM
func (fm *FileMap[K, V]) LookupMany(keys []K) (map[K]V, error) {
	positions := make(map[K]int64, len(keys))
	results := make(map[K]V, len(keys))
	// Search index
	fm.indexMu.RLock()
	defer fm.indexMu.RUnlock()
	for _, key := range keys {
		position, ok := fm.index[key]
		if ok {
			positions[key] = position
		}
	}
	// Search fm file
	fm.fileMu.Lock()
	defer fm.fileMu.Unlock()
	for key, position := range positions {
		if _, err := fm.file.Seek(position, 0); err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(fm.file)
		var value V
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		results[key] = value
	}
	return results, nil
}
