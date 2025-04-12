package bitcask

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRecordEncodingDecoding(t *testing.T) {
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
				Ns:  ns[:],
				Key: []byte("test-key"),
				Meta: &Meta{
					AppMeta: map[string]string{"foo": "bar"},
					Expire:  baseTime + 60,
					Etag:    etag[:],
					Flags:   1,
				},
				Value: []byte("hello world"),
			},
			wantErr: false,
		},
		{
			name: "Nil APP Meta",
			record: &Record{
				Ns:  ns[:],
				Key: []byte("test-key"),
				Meta: &Meta{
					Expire: baseTime + 61,
					Etag:   etag[:],
					Flags:  0,
				},
				Value: []byte("hello world"),
			},
			wantErr: false,
		},
		{
			name: "Nil Value",
			record: &Record{
				Ns:  ns[:],
				Key: []byte("test-key"),
				Meta: &Meta{
					AppMeta: map[string]string{"foo": "bar"},
					Expire:  baseTime + 62,
					Etag:    etag[:],
					Flags:   1,
				},
				Value: []byte{},
			},
			wantErr: false,
		},
		{
			name: "Nil APP Meta, Nil Value, Nil Etag and No Expire",
			record: &Record{
				Ns:  ns[:],
				Key: []byte("test-key"),
				Meta: &Meta{
					Expire: MetaNoExpire,
					Etag:   etag[:],
					Flags:  0,
				},
				Value: []byte{},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := tt.record.Encode(baseTime)
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
			assert.Equal(t, tt.record.Meta.Etag, decodedRecord.Meta.Etag, "Etag mismatch")

			if tt.record.Meta.AppMeta == nil {
				assert.Nil(t, decodedRecord.Meta.AppMeta, "AppMeta should be nil")
			} else {
				assert.Equal(t, tt.record.Meta.AppMeta, decodedRecord.Meta.AppMeta, "AppMeta mismatch")
			}
		})
	}
}
