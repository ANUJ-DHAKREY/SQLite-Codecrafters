package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
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
type queryToken struct {
	selectClauseParts []string
	tableName         string
	filterColumnName  string
	filterValue       string
}
type objectMetadata struct {
	coreObject SQLITE_MASTER_STRUCT
	columns    []string
	indexes    [][]string
}
type SQLITE_MASTER_STRUCT struct {
	objectType    string
	objectName    string
	objectTblName string
	rootPage      uint32
	createSQL     string
}

const TABLE_SQLITE_SCHEMA = "sqlite_schema"

var SQLITE_MASTER_COLUMNS = []string{"type", "name", "tbl_name", "rootpage", "sql"}
var databaseFilePath string
var DatabaseHeader DataBaseHeaderConfig
var parsedObjectsMetadataMap = make(map[string]objectMetadata)
var KEYWORDS_TO_IGNORE = []string{"PRIMARY", "FOREIGN", "CONSTRAINT", "CHECK", "UNIQUE", "REFERENCES", "GENERATED"}

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

func parseColumnNamesFromCreateSQL(schemaCreationSql string) []string {
	startIndex := strings.Index(schemaCreationSql, "(")
	endIndex := strings.LastIndex(schemaCreationSql, ")")
	if startIndex == -1 || endIndex == -1 || endIndex <= startIndex {
		return []string{}
	}
	columnsDefStr := schemaCreationSql[startIndex+1 : endIndex]
	columnsDefParts := strings.Split(columnsDefStr, ",")
	var columnNames []string
	for _, colDef := range columnsDefParts {
		colDef = strings.TrimSpace(colDef)
		if colDef == "" {
			continue
		}
		colDefParts := strings.Split(colDef, " ")
		if len(colDefParts) > 0 {
			columnName := strings.TrimSpace(colDefParts[0])
			ignore := false
			for _, keyword := range KEYWORDS_TO_IGNORE {
				if strings.ToUpper(columnName) == keyword {
					ignore = true
					break
				}
			}
			if !ignore {
				columnNames = append(columnNames, columnName)
			}
		}
	}
	return columnNames
}

func getIndexColumnsFromIndexCreateSQL(indexesCreateSQL []string) [][]string {
	var indexColumns [][]string
	for _, createSQL := range indexesCreateSQL {
		startIndex := strings.Index(createSQL, "(")
		endIndex := strings.LastIndex(createSQL, ")")
		if startIndex == -1 || endIndex == -1 || endIndex <= startIndex {
			continue
		}
		columnsStr := createSQL[startIndex+1 : endIndex]
		columnsParts := strings.Split(columnsStr, ",")
		indexColumns = append(indexColumns, columnsParts)
	}
	return indexColumns
}
func getIndexesForTable(tableName string, objectsMetadataMap map[string]objectMetadata) [][]string {
	var indexes [][]string
	var indexesCreateSQL []string
	for _, objectMeta := range objectsMetadataMap {
		if objectMeta.coreObject.objectType == "index" && objectMeta.coreObject.objectTblName == tableName {
			if objectMeta.coreObject.createSQL != "" {
				indexesCreateSQL = append(indexesCreateSQL, objectMeta.coreObject.createSQL)
			}
		}
	}
	indexes = getIndexColumnsFromIndexCreateSQL(indexesCreateSQL)
	return indexes
}
func getObjectMetaData(objectName map[string]interface{}) objectMetadata {
	// for now we are considering object type table only
	var objectMetadata objectMetadata
	if objectTypeValue, exist := objectName["type"]; exist && objectTypeValue != nil {
		objectMetadata.coreObject.objectType = string(objectTypeValue.([]byte))
	}
	if nameValue, exist := objectName["name"]; exist && nameValue != nil {
		objectMetadata.coreObject.objectName = string(nameValue.([]byte))
	}
	if tblNameValue, exist := objectName["tbl_name"]; exist && tblNameValue != nil {
		objectMetadata.coreObject.objectTblName = string(tblNameValue.([]byte))
	}
	if rootPageValue, exist := objectName["rootpage"]; exist && rootPageValue != nil {
		objectMetadata.coreObject.rootPage = binary.BigEndian.Uint32(rootPageValue.([]byte))
	}
	if createSQLValue, exist := objectName["sql"]; exist && createSQLValue != nil {
		objectMetadata.coreObject.createSQL = string(createSQLValue.([]byte))
	}

	objectMetadata.columns = parseColumnNamesFromCreateSQL(objectMetadata.coreObject.createSQL)
	objectMetadata.indexes = getIndexesForTable(objectMetadata.coreObject.objectName, parsedObjectsMetadataMap)
	return objectMetadata
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

// write your own parsing logic here
//assumptions
// the command is a always a select query
// there can be single, multiple column names or count(*) in the select clause
//only one where condition which is an equality condition // add support for other operators later
// no joins
// no nested queries
// no aggergation functions except count(*)
// no group by or order by clauses
// no limit or offset clauses
// the data is always contained in the single page
//sample command : SELECT name, color FROM apples WHERE color = 'Yellow'

// trim the command and split by spaces
func parseQuery(sql string) queryToken {
	//write a better parser later
	sql = strings.TrimSpace(sql)
	var queryToken queryToken
	commandParts := strings.Split(sql, " ")
	for i := 0; i < len(commandParts); i++ {
		part := commandParts[i]
		if strings.ToUpper(part) == "SELECT" {
			j := i + 1
			for j < len(commandParts) && strings.ToUpper(commandParts[j]) != "FROM" {
				queryToken.selectClauseParts = append(queryToken.selectClauseParts, commandParts[j])
				j++
			}
			i = j - 1
		} else if strings.ToUpper(part) == "FROM" {
			if i+1 < len(commandParts) {
				queryToken.tableName = commandParts[i+1]
				i = i + 1
			}
		} else if strings.ToUpper(part) == "WHERE" {
			if i+3 < len(commandParts) {
				queryToken.filterColumnName = strings.TrimSpace(commandParts[i+1])
				operand := strings.TrimSpace(commandParts[i+2])
				if operand != "=" {
					fmt.Println("Only equality operand is supported in where clause")
					os.Exit(1)
				}
				filterValue := strings.Trim(commandParts[i+3], "'")
				if filterValue == "" {
					fmt.Println("Invalid where condition value")
					os.Exit(1)
				}
			}
		}
	}
	return queryToken
}

func getCellOffSetsFromPage(pageContent []byte) []cellOffSets {
	if pageContent[0] != 0x0D {
		log.Fatal("only leaf pages are supported currently")
	}
	pageHeader := pageContent[0:8]
	numberOfCells := binary.BigEndian.Uint16(pageHeader[3:5])
	cellOffSetsArray := pageContent[8 : 8+2*numberOfCells]
	var cellOffSetsArrayParsed []cellOffSets
	for i := 0; i < int(numberOfCells); i++ {
		var cellOffset cellOffSets
		cellOffset.cellStartOffSet = binary.BigEndian.Uint16(cellOffSetsArray[i*2 : (i*2)+2])
		if i == 0 {
			cellOffset.cellEndOffSet = DatabaseHeader.PageSize - uint16(DatabaseHeader.ReservedSpacePerPage)
		} else {
			cellOffset.cellEndOffSet = binary.BigEndian.Uint16(cellOffSetsArray[(i-1)*2 : ((i-1)*2)+2])
		}
		cellOffSetsArrayParsed = append(cellOffSetsArrayParsed, cellOffset)
	}
	return cellOffSetsArrayParsed
}
func loadSQLiteSchema(file *os.File) []map[string]interface{} {
	firstPageContent, err := getPageContent(1, file)
	if err != nil {
		log.Fatal(err)
	}
	cellOffSetsArray := getCellOffSetsFromPage(firstPageContent[100:])
	var resultedRowsArray []map[string]interface{}
	for i := 0; i < len(cellOffSetsArray); i++ {
		cellContent := firstPageContent[cellOffSetsArray[i].cellStartOffSet:cellOffSetsArray[i].cellEndOffSet]
		rowDataMap := parseCellData(cellContent, SQLITE_MASTER_COLUMNS)
		resultedRowsArray = append(resultedRowsArray, rowDataMap)
	}
	return resultedRowsArray
}

func loadAllObjectMetadata(file *os.File) map[string]objectMetadata {
	sqliteSchemaContent := loadSQLiteSchema(file)
	objectsMetadataMap := make(map[string]objectMetadata)
	for _, row := range sqliteSchemaContent {
		nameColumnValue, exist := row["name"]
		if exist && nameColumnValue != nil {
			objectName := string(nameColumnValue.([]byte))
			objectMetaData := getObjectMetaData(row)
			objectsMetadataMap[objectName] = objectMetaData
		}
	}
	return objectsMetadataMap
}

func getParsedTableData(tableRawData []byte, tableColumnArray []string) []map[string]interface{} {
	cellOffSetsArray := getCellOffSetsFromPage(tableRawData)
	var parsedTableData []map[string]interface{}
	for i := 0; i < len(cellOffSetsArray); i++ {
		cellContent := tableRawData[cellOffSetsArray[i].cellStartOffSet:cellOffSetsArray[i].cellEndOffSet]
		rowDataMap := parseCellData(cellContent, tableColumnArray)
		parsedTableData = append(parsedTableData, rowDataMap)
	}
	return parsedTableData
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
		sqliteSchemaContent := loadSQLiteSchema(file)
		var tableNames []string
		for _, row := range sqliteSchemaContent {
			nameColumnValue, exist := row["name"]
			if exist && nameColumnValue != nil {
				tableNames = append(tableNames, string(nameColumnValue.([]byte)))
			}
		}
		sort.Strings(tableNames)
		fmt.Print(strings.Join(tableNames, " "))
	default:

		queryToken := parseQuery(command)
		parsedObjectsMetadataMap = loadAllObjectMetadata(file)
		tableMetadata, exist := parsedObjectsMetadataMap[queryToken.tableName]
		if !exist {
			fmt.Println("Table not found:", queryToken.tableName)
			os.Exit(1)
		}

		tableRawData, err := getPageContent(tableMetadata.coreObject.rootPage, file)
		if err != nil {
			log.Fatal(err)
		}

		parsedTableData := getParsedTableData(tableRawData, tableMetadata.columns)

		//filter the data based on where condition if present
		var filteredData []map[string]interface{}
		if queryToken.filterColumnName != "" {
			for _, row := range parsedTableData {
				if val, exist := row[queryToken.filterColumnName]; exist {
					strVal, ok := val.([]byte)
					if ok && string(strVal) == queryToken.filterValue {
						filteredData = append(filteredData, row)
					}
				}
			}
		} else {
			filteredData = parsedTableData
		}

		//prepare the result based on select clause parts
		if len(queryToken.selectClauseParts) == 1 && strings.ToUpper(queryToken.selectClauseParts[0]) == "COUNT(*)" {
			fmt.Println(len(filteredData))
			return
		} else {
			var resultRows []string
			for _, row := range filteredData {
				var selectedValues []string
				for _, colPart := range queryToken.selectClauseParts {
					colPart = strings.TrimSpace(colPart)
					if val, exist := row[colPart]; exist {
						strVal, ok := val.([]byte)
						if ok {
							selectedValues = append(selectedValues, string(strVal))
						} else {
							selectedValues = append(selectedValues, fmt.Sprintf("%v", val))
						}
					} else {
						selectedValues = append(selectedValues, "NULL")
					}
				}
				resultRows = append(resultRows, strings.Join(selectedValues, "|"))
			}
			for _, resultRow := range resultRows {
				fmt.Println(resultRow)
			}
			return
		}
	}
}
