/*
Usage:
$ go run miner.go "./minerjson/miner1.json"

Assumptions:
- Listens on localhost IP, TCP port 1234
*/

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

import m "./miner"

/////////////////////////////////////////////////////////////////////////////////////////////////
func main() {
	fmt.Println("Starting server")
	// Unmarshall settings json
	settingsLocation := os.Args[1]
	settingsFile, err := os.Open(settingsLocation)
	if err != nil {
		fmt.Println(err)
	}
	var settings m.Settings
	byteArray, _ := ioutil.ReadAll(settingsFile)
	json.Unmarshal(byteArray, &settings)
	settingsFile.Close()
	// Start RPC server
	m.StartMiner(settings)
}
