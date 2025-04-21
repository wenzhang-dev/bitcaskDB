package bitcask

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRecord_EmptyNs(t *testing.T) {
	// mock global options
	oldOpts := gOpts
	gOpts = &Options{
		NsSize:   0,
		EtagSize: 0,
	}
	defer func() {
		gOpts = oldOpts
	}()

	// testcase
	record := &Record{
		Ns:    nil,
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
		Meta:  NewMeta(nil),
	}

	backStore := make([]byte, DefaultRecordBufferSize)
	baseTime := uint64(time.Now().Unix())
	encoded, err := record.Encode(backStore, baseTime)
	assert.Nil(t, err)

	decoded, err := RecordFromBytes(encoded, baseTime)
	assert.Nil(t, err)

	// check
	assert.Equal(t, len(decoded.Ns), 0)
	assert.Equal(t, decoded.Key, record.Key)
	assert.Equal(t, decoded.Value, record.Value)
}

func TestRecord_EncodingDecoding(t *testing.T) {
	// mock global options
	oldOpts := gOpts
	gOpts = &Options{
		NsSize:   DefaultNsSize,
		EtagSize: DefaultEtagSize,
	}
	defer func() {
		gOpts = oldOpts
	}()

	// testcase
	ns := sha1Bytes("test-ns")
	etag := sha1Bytes("etag")
	baseTime := uint64(time.Now().Unix())

	tests := []struct {
		name    string
		record  *Record
		wantErr bool
	}{
		{
			name: "Normal case with full metadata and value",
			record: &Record{
				Ns:    ns[:],
				Key:   []byte("test-key"),
				Meta:  NewMeta(map[string]string{"foo": "bar"}).SetExpire(baseTime + 60).SetEtag(etag[:]).SetTombstone(true),
				Value: []byte("hello world"),
			},
			wantErr: false,
		},
		{
			name: "Nil APP Meta",
			record: &Record{
				Ns:    ns[:],
				Key:   []byte("test-key"),
				Meta:  NewMeta(nil).SetExpire(baseTime + 61).SetEtag(etag[:]).SetTombstone(false),
				Value: []byte("hello world"),
			},
			wantErr: false,
		},
		{
			name: "Nil Value",
			record: &Record{
				Ns:    ns[:],
				Key:   []byte("test-key"),
				Meta:  NewMeta(map[string]string{"foo": "bar"}).SetExpire(baseTime + 62).SetEtag(etag[:]).SetTombstone(true),
				Value: []byte{},
			},
			wantErr: false,
		},
		{
			name: "Nil APP Meta, Nil Value, Nil Etag and No Expire",
			record: &Record{
				Ns:    ns[:],
				Key:   []byte("test-key"),
				Meta:  NewMeta(nil),
				Value: []byte{},
			},
			wantErr: false,
		},
	}

	backStore := make([]byte, DefaultRecordBufferSize)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := tt.record.Encode(backStore, baseTime)
			if (err != nil) != tt.wantErr {
				t.Errorf("Encode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return // No need to proceed if encoding is expected to fail
			}

			decodedRecord, err := RecordFromBytes(encoded, baseTime)
			assert.NoError(t, err, "RecordFromBytes should not fail")

			// Ensure Namespace is correctly restored
			assert.Equal(t, tt.record.Ns, decodedRecord.Ns, "Namespace mismatch")

			// Ensure Key is correctly restored
			assert.Equal(t, tt.record.Key, decodedRecord.Key, "Key mismatch")

			// Ensure Value is correctly restored
			assert.Equal(t, tt.record.Value, decodedRecord.Value, "Value mismatch")

			// Check Meta field
			assert.NotNil(t, decodedRecord.Meta, "Meta should not be nil")
			assert.Equal(t, tt.record.Meta.Flags, decodedRecord.Meta.Flags, "Flags mismatch")
			assert.Equal(t, tt.record.Meta.Expire, decodedRecord.Meta.Expire, "Expire mismatch")

			if len(tt.record.Meta.Etag) != 0 {
				assert.Equal(t, tt.record.Meta.Etag, decodedRecord.Meta.Etag, "Etag mismatch")
			}

			if tt.record.Meta.AppMeta == nil {
				assert.Nil(t, decodedRecord.Meta.AppMeta, "AppMeta should be nil")
			} else {
				assert.Equal(t, tt.record.Meta.AppMeta, decodedRecord.Meta.AppMeta, "AppMeta mismatch")
			}
		})
	}
}
