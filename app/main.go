package main

import (
	"bytes"
	"codecrafters-sqlite-go/app/types"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

var databaseFilePath string
var DatabaseHeader types.DataBaseHeaderConfig

func readDatabaseHeader() ([]byte, error) {
	file, err := os.Open(databaseFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	header := make([]byte, 100)
	_, err = file.Read(header)
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

func readFirstPageContent() []byte {
	file, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	pageSize := DatabaseHeader.PageSize
	firstPageContent := make([]byte, pageSize)
	_, err = file.Read(firstPageContent)
	if err != nil {
		log.Fatal(err)
	}
	return firstPageContent
}
func main() {
	databaseFilePath = os.Args[1]
	command := os.Args[2]

	dataBaseHeaderContent, err := readDatabaseHeader()
	if err != nil {
		log.Fatal(err)
	}
	setDatabaseHeaderConfig(&DatabaseHeader, dataBaseHeaderContent)
	switch command {
	case ".dbinfo":

		firstPageContent := readFirstPageContent()
		firstPageHeader := firstPageContent[100:112]
		if err != nil {
			log.Fatal(err)
		}

		var tableCount uint16
		binary.Read(bytes.NewReader(firstPageHeader[3:5]), binary.BigEndian, &tableCount)

		// You can use print statements as follows for debugging, they'll be visible when running tests.
		fmt.Fprintln(os.Stderr, "Logs from your program will appear here!")

		fmt.Printf("database page size: %v", DatabaseHeader.PageSize)
		fmt.Printf("\nnumber of tables: %v\n", tableCount)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
