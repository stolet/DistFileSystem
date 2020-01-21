package blockchain

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"time"
)

type Record [512]byte
type OperationType uint16

const (
	CREATE OperationType = iota
	APPEND
)

type Operation interface {
	OperationType() OperationType
	GetMinerID() string
	GetFileName() string
	GetTimestamp() time.Time
}

func GetOperationHash(operation Operation) string {
	h := md5.New()
	var serialized []byte
	buffer := bytes.NewBuffer(serialized)
	b,_ := json.Marshal(operation)
	buffer.Write(b)

	h.Write(buffer.Bytes())

	encoding := hex.EncodeToString(h.Sum(nil))
	return encoding
}

type AppendOperation struct {
	Timestamp time.Time
	Filename  string
	Record    Record
	RecordNum uint16
	MinerId   string
}

func (o AppendOperation) OperationType() OperationType {
	return APPEND
}

func (o AppendOperation) GetFileName() string {
	return o.Filename
}

func (o AppendOperation) GetMinerID() string {
	return o.MinerId
}

func (o AppendOperation) GetRecord() Record {
	return o.Record
}

func (o AppendOperation) GetRecordNum() uint16 {
	return o.RecordNum
}

func (o AppendOperation) GetTimestamp() time.Time {
	return o.Timestamp
}

func (o *AppendOperation) SetRecordNum(recnum uint16) {
	o.RecordNum = recnum
}

type CreateOperation struct {
	Timestamp time.Time
	Filename string
	MinerId  string
}

func (CreateOperation) OperationType() OperationType {
	return CREATE
}

func (o CreateOperation) GetFileName() string {
	return o.Filename
}

func (o CreateOperation) GetMinerID() string {
	return o.MinerId
}

func (o CreateOperation) GetTimestamp() time.Time {
	return o.Timestamp
}

func (o CreateOperation) GetRecordNum() uint16 {
	return 0
}
