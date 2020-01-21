package miner

import (
	"errors"
	"fmt"
	"math"
	"time"
)

import bc "../blockchain"

// Contains filename
type FileDoesNotExistError string

func (e FileDoesNotExistError) Error() string {
	return fmt.Sprintf("RFS: Cannot open file [%s] in D mode as it does not exist locally", string(e))
}

// Contains filename
type FileExistsError string

func (e FileExistsError) Error() string {
	return fmt.Sprintf("RFS: Cannot create file with filename [%s] as it already exists", string(e))
}

// Contains filename
type FileMaxLenReachedError string

func (e FileMaxLenReachedError) Error() string {
	return fmt.Sprintf("RFS: File [%s] has reached its maximum length", string(e))
}

type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("RFS: Disconnected from the miner [%s]", string(e))
}


type ClientApi int

type ReadRecArgs struct {
	FileName  string
	RecordNum uint16
}

type AppendRecArgs struct {
	FileName   string
	FileRecord [512]byte
}

func (c *ClientApi) CreateFile(args *string, reply *bool) error {
	if !isConnected() {
		return DisconnectedError(MinerInstance.MinerSettings.MinerID)
	}
	*reply = true
	op := bc.CreateOperation{Filename: *args, MinerId: MinerInstance.MinerSettings.MinerID, Timestamp: time.Now()}
	MinerInstance.BlockchainMux.Lock()
	MinerInstance.MinerMux.Lock()
	fmt.Println("Got call to CreateFile")
	sendOperation(op)
	operationValidChannel := make(chan ValidMessage, 100)
	operationDoneChannel := make(chan bool, 100)
	opAndChannels := OpAndChannels{Op: op, ValidationChannel: &operationValidChannel, ConfirmationChannel: &operationDoneChannel, OperationFromMiner: true}
	MinerInstance.OpsBundle = append(MinerInstance.OpsBundle, opAndChannels)
	MinerInstance.BlockchainMux.Unlock()
	MinerInstance.MinerMux.Unlock()
	opValid := <-operationValidChannel
	if opValid.ErrorOccured {
		var fileExists = FileExistsError(*args)
		return errors.New(fileExists.Error())
	}
	<-operationDoneChannel // Waits until block has been added to blockchain before returning
	return nil
}

func (c *ClientApi) ListFiles(args *string, reply *[]string) error {
	if !isConnected() {
		return DisconnectedError(MinerInstance.MinerSettings.MinerID)
	}
	fmt.Println("Got call to ListFiles")
	MinerInstance.BlockchainMux.Lock()
	defer MinerInstance.BlockchainMux.Unlock()
	*reply = MinerInstance.LocalBlockchain.GetFileList()
	return nil
}

func (c *ClientApi) TotalRecs(fname *string, reply *uint16) error {
	if !isConnected() {
		return DisconnectedError(MinerInstance.MinerSettings.MinerID)
	}
	fmt.Println("Got call to TotalRecs")
	MinerInstance.BlockchainMux.Lock()
	defer MinerInstance.BlockchainMux.Unlock()
	if !MinerInstance.LocalBlockchain.FileExistsOnLongestChain(*fname){
		return FileDoesNotExistError(*fname)
	}
	*reply = uint16(MinerInstance.LocalBlockchain.FindIndexOfLastRecord(*fname) + 1)
	return nil
}

func (c *ClientApi) ReadRec(args *ReadRecArgs, reply *[512]byte) error {
	if !isConnected() {
		return DisconnectedError(MinerInstance.MinerSettings.MinerID)
	}
	fmt.Println("Got call to ReadRec")
	MinerInstance.BlockchainMux.Lock()
	defer MinerInstance.BlockchainMux.Unlock()

	if !MinerInstance.LocalBlockchain.FileExistsOnLongestChain(args.FileName){
		return FileDoesNotExistError(args.FileName)
	}
	record := MinerInstance.LocalBlockchain.GetRecord(args.FileName, args.RecordNum)
	if record == nil {
		return errors.New("retry")
	} else {
		val := [512]byte(*record)
		*reply = val
	}
	return nil
}

func (c *ClientApi) AppendRec(args *AppendRecArgs, reply *uint16) error {
	if !isConnected() {
		return DisconnectedError(MinerInstance.MinerSettings.MinerID)
	}
	op := bc.AppendOperation{Timestamp: time.Now(), Filename: (*args).FileName, Record: (*args).FileRecord, MinerId: MinerInstance.MinerSettings.MinerID}
	MinerInstance.BlockchainMux.Lock()
	MinerInstance.MinerMux.Lock()
	fmt.Println("Got call to append file")
	sendOperation(op)
	operationValidChannel := make(chan ValidMessage, 100)
	operationDoneChannel := make(chan bool, 100)
	opAndChannels := OpAndChannels{Op: op, ValidationChannel: &operationValidChannel, ConfirmationChannel: &operationDoneChannel, OperationFromMiner: true}
	MinerInstance.OpsBundle = append(MinerInstance.OpsBundle, opAndChannels)
	MinerInstance.BlockchainMux.Unlock()
	MinerInstance.MinerMux.Unlock()
	opValid := <-operationValidChannel
	if opValid.ErrorOccured {
		if opValid.Type == FILEDOESNOTEXIST {
			*reply = 0
			var fileDoesNotExist = FileDoesNotExistError((*args).FileName)
			return errors.New(fileDoesNotExist.Error())
		} else if opValid.Type == FILEMAXLENREACHED {
			*reply = math.MaxUint16
			var maxLenReached = FileMaxLenReachedError((*args).FileName)
			return errors.New(maxLenReached.Error())
		}
	}
	<-operationDoneChannel // Waits until block has been added to blockchain before returning
	*reply = opValid.Op.(bc.AppendOperation).GetRecordNum()
	return nil
}

func isConnected() bool {
	MinerInstance.MinerConnectionsLock.Lock()
	defer MinerInstance.MinerConnectionsLock.Unlock()
	for _, peer := range MinerInstance.MinerConnections {
		if peer.connected == true {
			return true
		}
	}
	return false
}
