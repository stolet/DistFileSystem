/*

This package specifies the application's interface to the distributed
records system (RFS) to be used in project 1 of UBC CS 416 2018W1.

You are not allowed to change this API, but you do have to implement
it.

*/

package rfslib

import (
	"fmt"
	"github.com/DistributedClocks/GoVector/govec"
	"github.com/DistributedClocks/GoVector/govec/vrpc"
	"net/rpc"
	"strings"
	"time"
	"unicode/utf8"
)

// A Record is the unit of file access (reading/appending) in RFS.
type Record [512]byte

////////////////////////////////////////////////////////////////////////////////////////////
// <ERROR DEFINITIONS>

// These type definitions allow the application to explicitly check
// for the kind of error that occurred. Each API call below lists the
// errors that it is allowed to raise.
//
// Also see:
// https://blog.golang.org/error-handling-and-go
// https://blog.golang.org/errors-are-values

// Contains minerAddr
type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("RFS: Disconnected from the miner [%s]", string(e))
}

// Contains filename. The *only* constraint on filenames in RFS is
// that must be at most 64 bytes long.
type BadFilenameError string

func (e BadFilenameError) Error() string {
	return fmt.Sprintf("RFS: Filename [%s] has the wrong length", string(e))
}

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

// </ERROR DEFINITIONS>
////////////////////////////////////////////////////////////////////////////////////////////

type FileSystem struct {
	MinerAddr  string
	LocalAddr  string
	Connection *rpc.Client
}

type ReadRecArgs struct {
	FileName  string
	RecordNum uint16
}

type AppendRecArgs struct {
	FileName   string
	FileRecord *Record
}

// Represents a connection to the RFS system.
type RFS interface {
	// Creates a new empty RFS file with name fname.
	//
	// Can return the following errors:
	// - DisconnectedError
	// - FileExistsError
	// - BadFilenameError
	CreateFile(fname string) (err error)

	// Returns a slice of strings containing filenames of all the
	// existing files in RFS.
	//
	// Can return the following errors:
	// - DisconnectedError
	ListFiles() (fnames []string, err error)

	// Returns the total number of records in a file with filename
	// fname.
	//
	// Can return the following errors:
	// - DisconnectedError
	// - FileDoesNotExistError
	TotalRecs(fname string) (numRecs uint16, err error)

	// Reads a record from file fname at position recordNum into
	// memory pointed to by record. Returns a non-nil error if the
	// read was unsuccessful. If a record at this index does not yet
	// exist, this call must block until the record at this index
	// exists, and then return the record.
	//
	// Can return the following errors:
	// - DisconnectedError
	// - FileDoesNotExistError
	ReadRec(fname string, recordNum uint16, record *Record) (err error)

	// Appends a new record to a file with name fname with the
	// contents pointed to by record. Returns the position of the
	// record that was just appended as recordNum. Returns a non-nil
	// error if the operation was unsuccessful.
	//
	// Can return the following errors:
	// - DisconnectedError
	// - FileDoesNotExistError
	// - FileMaxLenReachedError
	AppendRec(fname string, record *Record) (recordNum uint16, err error)
}

// The constructor for a new RFS object instance. Takes the miner's
// IP:port address string as parameter, and the localAddr which is the
// local IP:port to use to establish the connection to the miner.
//
// The returned rfs instance is singleton: an application is expected
// to interact with just one rfs at a time.
//
// This call should only succeed if the connection to the miner
// succeeds. This call can return the following errors:
// - Networking errors related to localAddr or minerAddr
func Initialize(localAddr string, minerAddr string) (rfs RFS, err error) {
	// For now return a DisconnectedError
	logfilename := strings.Replace(localAddr, ":", "-", 1)
	fmt.Println(logfilename)
	logger := govec.InitGoVector("client", logfilename, govec.GetDefaultConfig())
	fmt.Println()
	options := govec.GetDefaultLogOptions()
	client, err := vrpc.RPCDial("tcp", minerAddr, logger, options)
	if err != nil {
		return rfs, DisconnectedError(minerAddr)
	}
	rfs = FileSystem{MinerAddr: minerAddr, LocalAddr: localAddr, Connection: client}
	return rfs, nil
}

func (fs FileSystem) CreateFile(fname string) (err error) {
	// TODO: Handle DisconnectedError
	if utf8.RuneCountInString(fname) > 64 {
		fmt.Println(err)
		return err
	}
	var reply bool
	err = fs.Connection.Call("ClientApi.CreateFile", &fname, &reply)
	if reply {
		fmt.Println("Created file: ", fname)
		return nil
	} else {
		fmt.Println(err)
		return err
	}
	return err
}

func (fs FileSystem) ListFiles() (fnames []string, err error) {
	err = fs.Connection.Call("ClientApi.ListFiles", "", &fnames)
	fmt.Println("List of files: ", fnames)
	return fnames, err
}

func (fs FileSystem) TotalRecs(fname string) (numRecs uint16, err error) {
	err = fs.Connection.Call("ClientApi.TotalRecs", &fname, &numRecs)
	return numRecs, err
}

func (fs FileSystem) ReadRec(fname string, recordNum uint16, record *Record) (err error) {
	args := ReadRecArgs{FileName: fname, RecordNum: recordNum}

	for {
		e := fs.Connection.Call("ClientApi.ReadRec", &args, record)
		if e != nil && e.Error() == "retry" {
			time.Sleep(time.Second * 10)
		} else if e != nil {
			return e
		} else {
			break
		}
	}

	return nil
}

func (fs FileSystem) AppendRec(fname string, record *Record) (recordNum uint16, err error) {
	// TODO: Handle DisconnectedError )
	args := AppendRecArgs{FileName: fname, FileRecord: record}
	err = fs.Connection.Call("ClientApi.AppendRec", &args, &recordNum)
	if err != nil {
		if recordNum == 0 {
			fmt.Println(err)
			return recordNum, err
		}
		if recordNum == 65535 {
			fmt.Println(err)
			return recordNum, err
		}
	}
	fmt.Println("This is the number of the appended file: ", recordNum)
	return recordNum, nil
}
