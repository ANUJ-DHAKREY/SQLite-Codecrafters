package main

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
type queryToken struct {
	selectClauseParts []string
	tableName         string
	filterColumnName  string
	filterValue       string
}

type indexMetadata struct {
	indexName string
	columns   []string
}
type objectMetadata struct {
	coreObject          SQLITE_MASTER_STRUCT
	isIntegerPrimaryKey bool
	columns             []string
	indexes             []indexMetadata
}
type SQLITE_MASTER_STRUCT struct {
	objectType    string
	objectName    string
	objectTblName string
	rootPage      uint32
	createSQL     string
}

type IndexInteriorCell struct {
	//index interior page cell structure
	// A 4-byte big-endian page number which is the left child pointer.
	// A varint which is the total number of bytes of key payload, including any overflow
	// The initial portion of the payload that does not spill to overflow pages.
	// A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
	leftChildPointer uint32
	payloadSize      uint32
	payload          []byte
	overflowPage     uint32
	key              []byte
}

type TableInTeriorCell struct {
	leftChildPointer uint32
	rowId            int64
}

type IndexLeafCell struct {
	payloadSize  uint32
	payload      []byte
	rowId        int64
	overflowPage uint32
	key          []byte
}
type pageHeaderStruct struct {
	pageType         byte
	firstFreeBlock   uint16
	numberOfCells    uint16
	startOfCellArea  uint16
	numberOfFragment byte
	rightPagePointer uint32
}
