package blockchain

import (
	"errors"
	"math/rand"
	"time"
)

type Blockchain struct {
	Blocks                 map[string]Block
	ChildrenMap            map[string]map[string]struct{}
	GenesisBlockHash       string
	PowPerOpBlock          uint8
	PowPerNoOpBlock        uint8
	MinerId                string
	CoinMap                map[string]map[string]int
	LongestChainHash       string
	NumCoinsPerFileCreate  uint8
	MinedCoinsPerNoOpBlock uint8
	MinedCoinsPerOpBlock   uint8
}

func (b *Blockchain) Validate() bool {
	for _, block := range b.Blocks {
		var zeroCount uint8
		if len(block.Operations) == 0 {
			zeroCount = b.PowPerNoOpBlock
		} else {
			zeroCount = b.PowPerOpBlock
		}

		if !block.Validate(zeroCount) {
			return false
		}

		if !b.ValidateBlockOperations(block) {
			return false
		}
		_, ok := b.Blocks[block.PrevHash]
		return block.PrevHash == b.GenesisBlockHash || ok

	}

	for _, value := range b.CreateCoinMap() {
		if value < 0 {
			return false
		}
	}
	return true
}

func (b *Blockchain) ValidateZeroCount(block Block) bool {
	var zeroCount uint8
	if len(block.Operations) == 0 {
		zeroCount = b.PowPerNoOpBlock
	} else {
		zeroCount = b.PowPerOpBlock
	}

	return block.Validate(zeroCount)
}

func (b *Blockchain) Add(block Block) error {
	if b.Blocks == nil {
		b.Blocks = make(map[string]Block)
		b.ChildrenMap = make(map[string]map[string]struct{})
	}
	hash := block.Hash()
	if _, ok := b.Blocks[hash]; ok {
		return errors.New("Block already added")
	} else if b.ValidPrevHash(block) && b.ValidateZeroCount(block) {
		m := make(map[string]int)
		block.computeBlockCosts(m, b.NumCoinsPerFileCreate)
		for key, value := range m {
			if b.CoinMap[block.PrevHash][key]-value < 0 {
				return errors.New("Miner " + key + " does not have enough coins for operation")
			}
		}

		if !b.ValidateBlockOperations(block) {
			return errors.New("Operation validation failed")
		}

		hash := block.Hash()
		b.Blocks[hash] = block
		b.AddBlockCosts(hash, block)

		if b.ChildrenMap[block.PrevHash] == nil {
			b.ChildrenMap[block.PrevHash] = make(map[string]struct{})
		}

		b.ChildrenMap[block.PrevHash][hash] = struct{}{}
		b.UpdateLongestChainHash()
		return nil
	} else {
		if !b.ValidPrevHash(block) {
			return errors.New("invalid prevHash")
		} else {
			return errors.New("invalid nonce")
		}
	}
}

func (b *Blockchain) ValidateBlockOperations(block Block) bool {
	return b.ValidateOperations(block.Operations, block.PrevHash)
}

func (b *Blockchain) ValidateOperations(ops []Operation, hash string) bool {
	createMap := make(map[string]struct{})
	appendMap := make(map[string]uint16)

	for _, op := range ops {
		if op.OperationType() == CREATE {
			if _, ok := createMap[op.GetFileName()]; ok {
				return false
			} else {
				createMap[op.GetFileName()] = struct{}{}
				if b.FileExists(op.GetFileName(), hash) {
					return false
				}
			}
		} else {
			if recordNum, ok := appendMap[op.GetFileName()]; ok {
				if op.(AppendOperation).GetRecordNum() == recordNum+1 {
					appendMap[op.GetFileName()] += 1
				} else {
					return false
				}
			} else {
				appendMap[op.GetFileName()] = op.(AppendOperation).GetRecordNum()
				if !b.ValidateAppendOperation(op.(AppendOperation), hash) {
					return false
				}
				if _, ok := createMap[op.GetFileName()]; !ok && !b.FileExists(op.GetFileName(), hash) {
					return false
				}
			}
		}
	}
	return true
}

func (b *Blockchain) ValidateAppendOperation(operation AppendOperation, hash string) bool {

	for hash != b.GenesisBlockHash {
		prevBlock := b.Blocks[hash]

		for i := range prevBlock.Operations {
			op := prevBlock.Operations[len(prevBlock.Operations)-1-i]
			if op.OperationType() == APPEND && op.GetFileName() == operation.GetFileName() {
				recordNum := op.(AppendOperation).GetRecordNum()
				if operation.GetRecordNum() != recordNum+1 {
					return false
				} else {
					return true
				}
			}
		}
		hash = prevBlock.PrevHash
	}
	return operation.GetRecordNum() == 0
}

func (b *Blockchain) ValidateCreateOperation(operation CreateOperation, hash string) bool {
	for hash != b.GenesisBlockHash {
		prevBlock := b.Blocks[hash]

		for _, op := range prevBlock.Operations {
			if op.GetFileName() == operation.GetFileName() {
				return false
			}
		}

		hash = prevBlock.PrevHash
	}
	return true
}

func (b *Blockchain) ValidateOperation(operation Operation, hash string) bool {
	if operation.OperationType() == CREATE {
		return b.ValidateCreateOperation(operation.(CreateOperation), hash)
	} else {
		return b.ValidateAppendOperation(operation.(AppendOperation), hash)
	}
}

func (b *Blockchain) ValidPrevHash(block Block) bool {
	_, ok := b.Blocks[block.PrevHash]
	return block.PrevHash == b.GenesisBlockHash || ok
}

func (b *Blockchain) AddBlockCosts(hash string, block Block) {
	if b.CoinMap == nil {
		b.CoinMap = make(map[string]map[string]int)
	}
	targetMap := make(map[string]int)

	// Copy from the original map to the target map
	for key, value := range b.CoinMap[block.PrevHash] {
		targetMap[key] = value
	}

	block.computeBlockCosts(targetMap, b.NumCoinsPerFileCreate)
	b.CoinMap[hash] = targetMap
	if len(block.Operations) == 0 {
		b.CoinMap[hash][block.MinerId] += int(b.MinedCoinsPerNoOpBlock)
	} else {
		b.CoinMap[hash][block.MinerId] += int(b.MinedCoinsPerOpBlock)
	}
}

func (b *Blockchain) UpdateLongestChainHash() {
	if len(b.Blocks) == 0 {
		b.LongestChainHash = b.GenesisBlockHash
	} else {
		_, hashes := findDeepest(b.ChildrenMap, b.GenesisBlockHash, 0)
		rand.Seed(time.Now().Unix()) // initialize global pseudo random generator
		b.LongestChainHash = hashes[rand.Intn(len(hashes))]
	}
}

func (b *Blockchain) GetLongestChainHash() string {
	if b.LongestChainHash != "" {
		return b.LongestChainHash
	} else {
		b.UpdateLongestChainHash()
		return b.LongestChainHash
	}
}

func (b *Blockchain) GetLongestChains() []string {
	if len(b.Blocks) == 0 {
		return []string{b.GenesisBlockHash}
	} else {
		_, hashes := findDeepest(b.ChildrenMap, b.GenesisBlockHash, 0)
		return hashes
	}
}

func findDeepest(tree map[string]map[string]struct{}, node string, level int) (depth int, hashes []string) {
	if tree[node] == nil {
		return level, []string{node}
	} else {
		max := 0
		var maxHashes []string
		for key, _ := range tree[node] {
			depth, hashes := findDeepest(tree, key, level+1)
			if depth > max {
				maxHashes = hashes
				max = depth
			} else if depth == max {
				maxHashes = append(maxHashes, hashes...)
				max = depth
			}
		}
		return max, maxHashes
	}
}

func (b *Blockchain) BuildRecapList(existingHashes map[string]struct{}) []Block {
	return b.buildRecapList(b.GenesisBlockHash, existingHashes)
}

func (b *Blockchain) buildRecapList(node string, existingHashes map[string]struct{}) []Block {
	if b.ChildrenMap[node] == nil {
		block, ok := b.Blocks[node]
		if ok {
			return []Block{block}
		} else {
			return []Block{}
		}
	} else {
		var recapList []Block
		_, alreadyOnPeer := existingHashes[node]
		if block, ok := b.Blocks[node]; !alreadyOnPeer && ok {
			recapList = append(recapList, block)
		}
		for key, _ := range b.ChildrenMap[node] {
			recapList = append(recapList, b.buildRecapList(key, existingHashes)...)
		}

		return recapList
	}
}

func (b *Blockchain) GetHashList() map[string]struct{} {
	hashes := make(map[string]struct{})
	for key, _ := range b.Blocks {
		hashes[key] = struct{}{}
	}
	return hashes
}

func (b *Blockchain) CreateCoinMap() map[string]int {
	m := make(map[string]int)
	for _, block := range b.Blocks {
		block.computeBlockCosts(m, b.NumCoinsPerFileCreate)
		m[block.MinerId] += 1
	}

	return m
}

func (b *Blockchain) MineNopBlock() Block {
	return b.MineBlock([]Operation{})
}

func (b *Blockchain) MineNopBlockWithHash(hash string) Block {
	return b.MineBlockWithHash(hash, []Operation{})
}

func (b *Blockchain) MineBlockWithHash(hash string, ops []Operation) Block {
	block := Block{hash, ops, b.MinerId, 0}
	if len(ops) == 0 {
		block.FindNonce(b.PowPerNoOpBlock)
	} else {
		block.FindNonce(b.PowPerOpBlock)
	}

	return block
}

func (b *Blockchain) MineBlock(ops []Operation) Block {
	return b.MineBlockWithHash(b.GetLongestChainHash(), ops)
}

// Function to be used for testing only. IT CREATES A BLOCK THAT IS NOT ON THE LONGEST CHAIN!
func (b *Blockchain) SpoofMineBlock(ops []Operation, hash string) Block {
	block := Block{hash, ops, b.MinerId, 0}
	if len(ops) == 0 {
		block.FindNonce(b.PowPerNoOpBlock)
	} else {
		block.FindNonce(b.PowPerOpBlock)
	}

	return block
}

func (b *Blockchain) FileExistsOnLongestChain(filename string) bool { // Helper to check if a file being created already exists in longest chain
	return b.FileExists(filename, b.GetLongestChainHash())
}

func (b *Blockchain) FileExists(filename string, currentHash string) bool { // Helper to check if a file being created already exists in longest chain
	for currentHash != b.GenesisBlockHash {
		currentBlock := b.Blocks[currentHash]
		listOfOperations := currentBlock.Operations
		for _, op := range listOfOperations {
			if op.GetFileName() == filename {
				return true
			}
		}
		currentHash = currentBlock.PrevHash
	}
	return false
}

// Returns -1 if there are no records for a file
func (b *Blockchain) FindIndexOfLastRecord(filename string) int {
	currentHash := b.GetLongestChainHash()
	maxIndex := -1
	for currentHash != b.GenesisBlockHash {
		currentBlock := b.Blocks[currentHash]
		listOfOperations := currentBlock.Operations
		for _, op := range listOfOperations {
			if op.GetFileName() == filename && op.OperationType() == APPEND {
				//if int(op.(AppendOperation).GetRecordNum()) < maxIndex {
				//	maxIndex = int(op.(AppendOperation).GetRecordNum())
				//}
				maxIndex++
			}
		}
		currentHash = currentBlock.PrevHash
	}
	return maxIndex
}

func (b *Blockchain) GetFileList() []string {
	currentHash := b.GetLongestChainHash()
	var files []string
	for currentHash != b.GenesisBlockHash {
		currentBlock := b.Blocks[currentHash]
		listOfOperations := currentBlock.Operations
		for _, op := range listOfOperations {
			if op.OperationType() == CREATE {
				files = append(files, op.GetFileName())
			}
		}
		currentHash = currentBlock.PrevHash
	}
	return files
}

func (b *Blockchain) GetRecord(fname string, recordNum uint16) *Record {
	currentHash := b.GetLongestChainHash()
	for currentHash != b.GenesisBlockHash {
		currentBlock := b.Blocks[currentHash]
		listOfOperations := currentBlock.Operations
		for _, op := range listOfOperations {
			if op.OperationType() == APPEND && op.GetFileName() == fname && op.(AppendOperation).RecordNum == recordNum {
				records := op.(AppendOperation).Record
				return &records
			}
		}
		currentHash = currentBlock.PrevHash
	}
	return nil
}

func (b *Blockchain) GetCurrentCoinCount() int {
	return b.GetCoinCount(b.MinerId, b.GetLongestChainHash())
}

func (b *Blockchain) GetCoinCount(minerId string, hash string) int {
	coinsAtHash, ok := b.CoinMap[hash]
	if !ok {
		return 0
	}
	if val, ok := coinsAtHash[minerId]; ok {
		return val
	} else {
		return 0
	}
}

func (b *Blockchain) ChainAtLeastThisLong(length uint8) bool {
	var i uint8 = 0
	hash := b.GetLongestChainHash()
	for i < length && b.GenesisBlockHash != hash {
		hash = b.Blocks[hash].PrevHash
		i++
	}
	if i == length {
		return true
	} else {
		return false
	}
}
