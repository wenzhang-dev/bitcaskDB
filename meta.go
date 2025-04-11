package bitcask

const (
	TombstoneFlagBit = 0
	MetaNoExpire     = 0
)

type Meta struct {
	// user specified meta data
	AppMeta     map[string]string
	AppMetaSize int

	// control meta data
	Expire uint64
	Etag   []byte

	// bitmap flag
	Flags uint8
}

func NewMeta(appMeta map[string]string) *Meta {
	// FIXME: insufficient
	size := 0
	for k, v := range appMeta {
		size += len(k) + len(v)
	}

	return &Meta{
		AppMeta:     appMeta,
		AppMetaSize: size,
		Expire:      MetaNoExpire,
		Etag:        nil,
		Flags:       0,
	}
}

func NewMetaWithTombstone() *Meta {
	meta := NewMeta(nil)
	return meta.SetTombstone(true)
}

func (m *Meta) SetExpire(expire uint64) *Meta {
	m.Expire = expire
	return m
}

func (m *Meta) SetEtag(etag []byte) *Meta {
	m.Etag = etag
	return m
}

func (m *Meta) SetTombstone(enable bool) *Meta {
	if enable {
		m.Flags |= (uint8)(1 << TombstoneFlagBit)
	} else {
		m.Flags &= ^(uint8)(1 << TombstoneFlagBit)
	}
	return m
}

func (m *Meta) AppMetadata() map[string]string {
	return m.AppMeta
}

func (m *Meta) IsTombstone() bool {
	return m.Flags&(1<<TombstoneFlagBit) != 0
}

func (m *Meta) AppMetaApproximateSize() int {
	return m.AppMetaSize
}
