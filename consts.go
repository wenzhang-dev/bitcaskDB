package bitcask

const (
	DefaultNsSize   = 20
	DefaultEtagSize = 20

	// trigger one compaction per 60 second
	DefaultCompactionTriggerInterval = 60

	DefaultCheckDiskUsageInterval = 20

	DefaultCompactionPickerRatio = 0.4

	DefaultRecordBufferSize = 64 * 1024 // 64KB

	DefaultLogMaxSize = 20 // 20MB

	DefaultLogFile = "db.log"
)
