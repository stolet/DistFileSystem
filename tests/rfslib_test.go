package tests

import (
	"testing"
	"time"
)

import m "../miner"
import "../rfslib"

var settings = m.Settings{
	MinedCoinsPerOpBlock: 2,
	MinedCoinsPerNoOpBlock: 1,
	NumCoinsPerFileCreate: 2,
	GenOpBLockTimeout: 5,
	GenesisBlockHash: "83218ac34c1834c26781fe4bde918ee4",
	PowPerOpBlock: 4,
	PowPerNoOpBlock: 4,
	ConfirmsPerFileCreate: 4,
	ConfirmsPerFileAppend: 2,
	MinerID: "Stacy",
	PeerMinersAddrs: []string{},
	IncomingMinersAddr: "127.0.0.1:5050",
	OutgoingMinersIP: "127.0.0.1",
	IncomingClientsAddr: "127.0.0.1:9090",
}

func TestCreate(t *testing.T) {
	go m.StartMiner(settings)
	time.Sleep(1 * time.Second)
	rfs, _ := rfslib.Initialize("127.0.0.1:1111", "127.0.0.1:9090")
	err1 := rfs.CreateFile("File1")
	if err1 != nil {
		t.Error(err1)
	}
}

func TestAppend(t *testing.T) {
	customSettings := settings
	customSettings.IncomingClientsAddr = "127.0.0.1:9091"
	customSettings.IncomingMinersAddr = "127.0.0.1:5051"
	go m.StartMiner(customSettings)
	time.Sleep(3 * time.Second)
	rfs, _ := rfslib.Initialize("127.0.0.1:1112", "127.0.0.1:9091")
	rfs.CreateFile("File1")
	var record rfslib.Record = [512]byte{5,5,5}
	recordNum1, err1 := rfs.AppendRec("File1", &record)
	recordNum2, err2 := rfs.AppendRec("File1", &record)

	if err1 != nil {
		t.Error(err1)
	}
	if recordNum1 != 0 {
		t.Error("Returns wrong record number for first append")
	}

	if err2 != nil {
		t.Error(err2)
	}
	if recordNum2 != 1 {
		t.Error("Returns wrong record number for second append")
	}
}

func TestMultipleOps(t *testing.T) {
	customSettings := settings
	customSettings.IncomingClientsAddr = "127.0.0.1:9092"
	customSettings.IncomingMinersAddr = "127.0.0.1:5052"
	go m.StartMiner(customSettings)
	time.Sleep(1 * time.Second)
	rfs, _ := rfslib.Initialize("127.0.0.1:1112", "127.0.0.1:9091")
	go rfs.CreateFile("File1")
	time.Sleep(1 * time.Second)
	var record rfslib.Record = [512]byte{5,5,5}
	go rfs.AppendRec("File1", &record)
    go rfs.AppendRec("File1", &record)
	time.Sleep(1 * time.Second)
	recordNum, err := rfs.AppendRec("File1", &record)
	if err != nil {
		t.Error(err)
	}
	if recordNum != 2 {
		t.Error("Returns wrong record number for first append")
	}
}
