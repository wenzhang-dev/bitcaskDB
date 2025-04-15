package bitcask

const (
	NsSize   = 20
	EtagSize = 20

	// trigger one compaction per 60 second
	CompactionTriggerInterval = 60

	CheckDiskUsageInterval = 20

	CompactionPickerRatio = 0.4
)
