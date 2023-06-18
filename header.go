package filemap

import (
	"encoding/binary"
	"io"
)

const (
	// The database file version
	Version uint64 = 0
)

// A header for a FileMap
type fmHeader struct {
	version     uint64 // The version
	dataOffset  uint64 // The position in which to find the first byte of data
	indexOffset uint64 // The position in which to find the first byte of the index
}

// Returns a new header for the FileMap
func newHeader() fmHeader {
	return fmHeader{
		version:     Version,
		dataOffset:  0,
		indexOffset: 0,
	}
}

// Reads a header from a reader
func newHeaderFromReader(r io.Reader) (fmHeader, error) {
	headerBytes := make([]byte, 24)
	if _, err := r.Read(headerBytes); err != nil {
		return fmHeader{}, err
	}
	return fmHeader{
		version:     binary.BigEndian.Uint64(headerBytes),
		dataOffset:  binary.BigEndian.Uint64(headerBytes[8:16]),
		indexOffset: binary.BigEndian.Uint64(headerBytes[16:24]),
	}, nil
}

// Updates a header with the data and index information
func (header *fmHeader) Update(data int64, index int64) {
	header.dataOffset = uint64(data)
	header.indexOffset = uint64(index)
}

// Returns the bytes representation of a header
func (header *fmHeader) Bytes() []byte {
	headerBytes := make([]byte, 24)
	if header.version != 0 {
		binary.BigEndian.PutUint64(headerBytes, header.version)
	}
	if header.dataOffset != 0 {
		binary.BigEndian.PutUint64(headerBytes[8:16], header.dataOffset)
	}
	if header.indexOffset != 0 {
		binary.BigEndian.PutUint64(headerBytes[16:24], header.indexOffset)
	}
	return headerBytes
}
