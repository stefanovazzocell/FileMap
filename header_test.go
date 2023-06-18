package filemap

import (
	"bytes"
	"reflect"
	"testing"
)

func TestHeader(t *testing.T) {
	t.Parallel()
	header := newHeader()
	if header.version != Version || header.dataOffset != 0 || header.indexOffset != 0 {
		t.Fatalf("newHeader() returned unexpected %v", header)
	}
	header.Update(0x123456789abcdef, 0x123456789abcdef*3)
	if header.version != Version || header.dataOffset != 0x123456789abcdef || header.indexOffset != 0x123456789abcdef*3 {
		t.Fatalf("Update() returned unexpected %v", header)
	}
	reader := bytes.NewReader(header.Bytes())
	parsedHeader, err := newHeaderFromReader(reader)
	if err != nil {
		t.Fatalf("Got error while running newHeaderFromReader: %v", err)
	}
	if !reflect.DeepEqual(header, parsedHeader) {
		t.Fatalf("Header %v doesn't match original %v", parsedHeader, header)
	}
}
