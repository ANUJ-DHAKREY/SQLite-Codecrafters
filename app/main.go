package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		databaseHeader := make([]byte, 100)

		_, err = databaseFile.Read(databaseHeader)
		if err != nil {
			log.Fatal(err)
		}

		var pageSize uint16
		if err := binary.Read(bytes.NewReader(databaseHeader[16:18]), binary.BigEndian, &pageSize); err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}

		firstPageHeader := make([]byte, 12)
		_, err = databaseFile.Read(firstPageHeader)
		if err != nil {
			log.Fatal(err)
		}
		var tableCount uint16
		binary.Read(bytes.NewReader(firstPageHeader[3:5]), binary.BigEndian, &tableCount)

		// You can use print statements as follows for debugging, they'll be visible when running tests.
		fmt.Fprintln(os.Stderr, "Logs from your program will appear here!")

		fmt.Printf("database page size: %v", pageSize)
		fmt.Printf("\nnumber of tables: %v\n", tableCount)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
