package blockchain

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
)

type Block struct {
	PrevHash   string
	Operations []Operation
	MinerId    string
	Nonce      uint32
}

func (b *Block) Validate(zeroCount uint8) bool {
	hash := b.Hash()
	allZeros := true
	for j := len(hash) - int(zeroCount); j < len(hash); j++ {
		if hash[j] != '0' {
			allZeros = false
			break
		}
	}
	return allZeros
}

func (block *Block) computeBlockCosts(costMap map[string]int, CreateCost uint8) {
	for _, op := range block.Operations {
		if op.OperationType() == CREATE {
			costMap[op.GetMinerID()] -= int(CreateCost)
		} else {
			costMap[op.GetMinerID()] -= 1
		}
	}
}

func (block *Block) Hash() string {
	h := md5.New()
	var serialized []byte
	buffer := bytes.NewBuffer(serialized)
	buffer.WriteString(block.PrevHash)
	buffer.WriteString(" ")
	buffer.WriteString(block.MinerId)
	buffer.WriteString(" ")
	if len(block.Operations) > 0 {
		b, _ := json.Marshal(block.Operations)
		buffer.Write(b)
	} else {
		buffer.WriteString(" ")
	}

	binary.Write(buffer, binary.BigEndian, block.Nonce)

	h.Write(buffer.Bytes())

	encoding := hex.EncodeToString(h.Sum(nil))
	return encoding
}

func (block *Block) FindNonce(zeroCount uint8) (hash string) {
	for i := uint32(0); i < math.MaxUint32; i++ {
		block.Nonce = i
		hash := block.Hash()
		allZeros := true
		for j := len(hash) - int(zeroCount); j < len(hash); j++ {
			if hash[j] != '0' {
				allZeros = false
				break
			}
		}
		if allZeros {
			return hash
		}
	}
	return ""
}
