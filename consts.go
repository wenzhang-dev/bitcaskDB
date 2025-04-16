package bitcask

const (
	DefaultNsSize   = 20
	DefaultEtagSize = 20

	// trigger one compaction per 60 second
	DefaultCompactionTriggerInterval = 60

	DefaultCheckDiskUsageInterval = 20

	DefaultCompactionPickerRatio = 0.4
)
