package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

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

type cellOffSets struct {
	cellStartOffSet uint16
	cellEndOffSet   uint16
}

const TABLE_SQLITE_SCHEMA = "sqlite_schema"

var databaseFilePath string
var DatabaseHeader DataBaseHeaderConfig

func readDatabaseHeader(file *os.File) ([]byte, error) {

	header := make([]byte, 100)
	_, err := file.Read(header)
	if err != nil {
		return nil, err
	}

	return header, nil
}
func setDatabaseHeaderConfig(config *DataBaseHeaderConfig, data []byte) {
	if len(data) < 100 {
		log.Fatal("Corrupted database header or insufficient data length")
	}
	config.HeaderString = [16]byte(data[:16])
	config.PageSize = binary.BigEndian.Uint16(data[16:18])
	config.FileFormatWriteVersion = data[18]
	config.FileFormatReadVersion = data[19]
	config.ReservedSpacePerPage = data[20]
	config.MaxEmbeddedPayloadFraction = data[21]
	config.MinEmbeddedPayloadFraction = data[22]
	config.LeafPayloadFraction = data[23]
	config.FileChangeCounter = binary.BigEndian.Uint32(data[24:28])
	config.DatabaseSizeInPages = binary.BigEndian.Uint32(data[28:32])
	config.FirstFreelistTrunkPage = binary.BigEndian.Uint32(data[32:36])
	config.TotalFreelistPages = binary.BigEndian.Uint32(data[36:40])
	config.SchemaCookie = binary.BigEndian.Uint32(data[40:44])
	config.SchemaFormatNumber = binary.BigEndian.Uint32(data[44:48])
	config.DefaultPageCacheSize = binary.BigEndian.Uint32(data[48:52])
	config.LargestRootBtreePage = binary.BigEndian.Uint32(data[52:56])
	config.TextEncoding = binary.BigEndian.Uint32(data[56:60])
	config.UserVersion = binary.BigEndian.Uint32(data[60:64])
	config.IncrementalVacuumMode = binary.BigEndian.Uint32(data[64:68])
	config.ApplicationID = binary.BigEndian.Uint32(data[68:72])
	copy(config.ReservedForExpansion[:], data[72:92])
	config.VersionValidForNumber = binary.BigEndian.Uint32(data[92:96])
	config.SQLiteVersionNumber = binary.BigEndian.Uint32(data[96:100])
}

func getPageContent(pageNumber uint32, file *os.File) ([]byte, error) {
	if pageNumber < 1 {
		return nil, fmt.Errorf("invalid page number: %d", pageNumber)
	}
	offset := (int64(pageNumber-1) * int64(DatabaseHeader.PageSize))
	_, err := file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	pageContent := make([]byte, DatabaseHeader.PageSize)
	_, err = file.Read(pageContent)
	if err != nil {
		return nil, err
	}
	return pageContent, nil
}
func decodeVarint(arr []byte) (int64, int) {
	var value int64
	var i int
	length := len(arr)
	for i = 0; i < length && i < 9; i++ {
		currentByte := arr[i]
		if i == 8 {
			value = value << 8
			value = value | int64(currentByte)
			return value, i + 1
		}
		//most significant bit
		msb := currentByte >> 7
		value = value << 7
		rest7Bits := currentByte & 127
		value = (value | int64(rest7Bits))
		if msb == 0 {
			break
		}
	}
	return value, i + 1
}

func getTableColumnArray(tableName string) []string {
	var columnsNameArray []string
	if tableName == TABLE_SQLITE_SCHEMA {
		return []string{"type", "name", "tbl_name", "rootpage", "sql"}
	} else {
		//get the row details from the sqlite schema
		//parse the create table sql command
		//and return the column names from them
		return columnsNameArray
	}
}

func getColumnValue(rowContent []byte, serialType uint64) (interface{}, uint64) {
	switch {
	case serialType >= 12 && serialType%2 == 0:
		contentSize := (serialType - 12) / 2
		return rowContent[:contentSize], contentSize
	case serialType >= 13 && serialType%2 == 1:
		contentSize := (serialType - 13) / 2
		return rowContent[:contentSize], contentSize
	}

	switch serialType {
	case 0:
		return nil, 0
	case 1:
		return rowContent[:1], 1
	case 2:
		return rowContent[:2], 2
	case 3:
		return rowContent[:3], 3
	case 4:
		return rowContent[:4], 4
	case 5:
		return rowContent[:6], 6
	case 6:
		return rowContent[:8], 8
	case 7:
		return rowContent[:8], 8
	case 8:
		return 0, 0
	case 9:
		return 1, 0
	case 10, 11:
		return nil, 0
	default:
		log.Fatal("invalid serial type")
	}
	return nil, 0
}

// for now we are considering that all roq data is contained in a single page
func parseCellData(cellContent []byte, tableColumnArray []string) map[string]interface{} {
	payloadSize, n := decodeVarint(cellContent)
	_, m := decodeVarint(cellContent[n:])
	payload := cellContent[n+m : n+m+int(payloadSize)]
	headerSize, k := decodeVarint(payload)
	header := payload[k:headerSize]
	var serialTypes []uint64
	for j := 0; j < int(headerSize) && len(serialTypes) < len(tableColumnArray); {
		serialType, l := decodeVarint(header[j:])
		serialTypes = append(serialTypes, uint64(serialType))
		j = j + l
	}

	payloadBody := payload[headerSize:]
	payloadIndex := 0
	rowData := make(map[string]interface{})

	for i, serialType := range serialTypes {
		unparsedBytes := payloadBody[payloadIndex:]
		val, k := getColumnValue(unparsedBytes, serialType)
		payloadIndex += int(k)
		rowData[tableColumnArray[i]] = val
	}

	return rowData
}

func main() {
	databaseFilePath = os.Args[1]
	command := os.Args[2]

	file, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	dataBaseHeaderContent, err := readDatabaseHeader(file)
	if err != nil {
		log.Fatal(err)
	}

	setDatabaseHeaderConfig(&DatabaseHeader, dataBaseHeaderContent)
	switch command {
	case ".dbinfo":

		firstPageContent, err := getPageContent(1, file)
		if err != nil {
			log.Fatal(err)
		}

		firstPageHeader := firstPageContent[100:108]
		tableCount := binary.BigEndian.Uint16(firstPageHeader[3:5])

		// You can use print statements as follows for debugging, they'll be visible when running tests.
		fmt.Fprintln(os.Stderr, "Logs from your program will appear here!")

		fmt.Printf("database page size: %v", DatabaseHeader.PageSize)
		fmt.Printf("\nnumber of tables: %v\n", tableCount)
	case ".tables":
		//considering the data only in the first page for now
		firstPageContent, err := getPageContent(1, file)
		if err != nil {
			log.Fatal(err)
		}
		//means this page is a leaf page
		//a 8 byte header + n cell pointers + cell content area
		firstPageHeader := firstPageContent[100:108]
		numberOfCells := binary.BigEndian.Uint16(firstPageHeader[3:5])
		cellOffSetsArray := firstPageContent[108 : 108+2*numberOfCells]
		sortedCellPointers := make([]uint16, numberOfCells)
		for i := 0; i < int(numberOfCells); i++ {
			sortedCellPointers[i] = binary.BigEndian.Uint16(cellOffSetsArray[i*2 : (i*2)+2])
		}
		sort.Slice(sortedCellPointers, func(i, j int) bool {
			return sortedCellPointers[i] < sortedCellPointers[j]
		})
		var cellOffSetsArraySorted []cellOffSets
		for i := 0; i < int(numberOfCells); i++ {
			var cellOffset cellOffSets
			cellOffset.cellStartOffSet = sortedCellPointers[i]
			if i == int(numberOfCells)-1 {
				cellOffset.cellEndOffSet = DatabaseHeader.PageSize - uint16(DatabaseHeader.ReservedSpacePerPage)
			} else {
				cellOffset.cellEndOffSet = sortedCellPointers[i+1]
			}
			cellOffSetsArraySorted = append(cellOffSetsArraySorted, cellOffset)
		}
		var resultedRowsArray []string
		for i := 0; i < int(numberOfCells); i++ {
			cellContent := firstPageContent[cellOffSetsArraySorted[i].cellStartOffSet:cellOffSetsArraySorted[i].cellEndOffSet]
			//considering that the this is a table leaf cell and payload do not oveflow pages
			// table leaf cell format:
			// payload size (varint) + rowid (varint) + payload
			// payload format:
			// header size (varint) + serial type array + column values
			// the sqlite master table has the following columns:
			//get the payload size

			columnToBeFetched := []string{"name"}
			tableName := TABLE_SQLITE_SCHEMA

			tableColumnArray := getTableColumnArray(tableName)
			var filterColumns []string
			getRowDataMap := parseCellData(cellContent, tableColumnArray)
			// for key, value := range getRowDataMap {
			// 	printInterface(key, value)
			// }
			for _, columnName := range columnToBeFetched {
				filterColumns = append(filterColumns, string(getRowDataMap[columnName].([]byte)))
			}
			resultedRowsArray = append(resultedRowsArray, strings.Join(filterColumns, " "))
		}
		sort.Strings(resultedRowsArray)
		fmt.Print(strings.Join(resultedRowsArray, " "))
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
