type DataBaseHeaderConfig struct {
	HeaderString               [16]byte
	PageSize                   uint16
	FileFormatWriteVersion     byte
	FileFormatReadVersion      byte
	ReservedSpacePerPage       byte
	MaxEmbeddedPayloadFraction byte
	MinEmbeddedPayloadFraction byte
	LeafPayloadFraction        byte
	FileChangeCounter          uint32
	DatabaseSizeInPages        uint32
	FirstFreelistTrunkPage     uint32
	TotalFreelistPages         uint32
	SchemaCookie               uint32
	SchemaFormatNumber         uint32
	DefaultPageCacheSize       uint32
	LargestRootBtreePage       uint32
	TextEncoding               uint32
	UserVersion                uint32
	IncrementalVacuumMode      uint32
	ApplicationID              uint32
	ReservedForExpansion       [20]byte
	VersionValidForNumber      uint32
	SQLiteVersionNumber        uint32
}
