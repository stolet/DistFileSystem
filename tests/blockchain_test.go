package tests

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"
)
import bc "../blockchain"

func TestMineAndValidateBlockchain(t *testing.T) {
	b := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 3, PowPerNoOpBlock: 4, MinerId: "miner1"}
	b.Add(b.MineBlock([]bc.Operation{}))
	block := b.MineBlock([]bc.Operation{bc.CreateOperation{time.Now(), "testFile", "miner1"}})
	b.Add(block)
	b.Validate()
	if !b.Validate() {
		t.Error("Validation Failed")
	}
}

func TestMineAndValidateTwoBlocksBlockchain(t *testing.T) {
	b := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 3, PowPerNoOpBlock: 4, MinerId: "miner1"}
	b.Add(b.MineBlock([]bc.Operation{}))
	block1 := b.MineBlock([]bc.Operation{bc.CreateOperation{time.Now(), "testFile", "miner1"}})
	b.Add(block1)
	b.Validate()
	if !b.Validate() {
		t.Error("Validation Failed")
	}

	block2 := b.MineBlock([]bc.Operation{bc.CreateOperation{time.Now(), "testFile2", "miner1"},
		bc.AppendOperation{time.Now(), "test", bc.Record{1, 2, 3, 4}, 0, "miner1"}})
	b.Add(block2)
	b.Validate()
	if !b.Validate() {
		t.Error("Validation Failed")
	}
}

func TestCostmap(t *testing.T) {
	b := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 3, PowPerNoOpBlock: 4, MinerId: "miner1", NumCoinsPerFileCreate: 2, MinedCoinsPerNoOpBlock: 1, MinedCoinsPerOpBlock: 1}
	block1 := b.MineBlock([]bc.Operation{})
	b.Add(block1)
	b.Validate()
	if !b.Validate() {
		t.Error("Validation Failed")
	}

	block2 := b.MineBlock([]bc.Operation{bc.CreateOperation{time.Now(), "testFile2", "miner1"}})
	b.Add(block2)
	b.Validate()
	if !b.Validate() {
		t.Error("Validation Failed")
	}

	block3 := b.MineBlock([]bc.Operation{})
	b.Add(block3)
	b.Validate()
	if !b.Validate() {
		t.Error("Validation Failed")
	}
	coinMap := b.CreateCoinMap()
	if val, ok := coinMap["miner1"]; !ok || val != 1 {
		t.Error("Incorrect map entry")
	}

	if b.GetCurrentCoinCount() != 1 {
		t.Error("Incorrect coin count")
	}
}
func TestCoinCounting(t *testing.T) {
	b := bc.Blockchain{GenesisBlockHash: "83218ac34c1834c26781fe4bde918ee4", PowPerOpBlock: 3, PowPerNoOpBlock: 4, MinerId: "Stacy", NumCoinsPerFileCreate: 2, MinedCoinsPerNoOpBlock: 1, MinedCoinsPerOpBlock: 1}
	if 0 != b.GetCurrentCoinCount() {
		t.Error("Wrong coin count ", b.GetCurrentCoinCount())
	}

	for i := 0 ; i<5; i++{
		b.Add(b.MineNopBlock())
		if i+1 != b.GetCurrentCoinCount(){
			t.Error("Wrong coin count ", b.GetCurrentCoinCount())
		}
	}
	block1 := b.MineBlock([]bc.Operation{bc.CreateOperation{time.Now(), "test1", "Stacy"}})
	b.Add(block1)
	if 4 != b.GetCurrentCoinCount() {
		t.Error("Wrong coin count ", b.GetCurrentCoinCount())
	}
	block2 := b.MineBlock([]bc.Operation{bc.CreateOperation{time.Now(), "test2", "Stacy"}})
	b.Add(block2)

	if 3 != b.GetCurrentCoinCount() {
		t.Error("Wrong coin count ", b.GetCurrentCoinCount())
	}
	b.Add(b.MineNopBlock())


	if 4 != b.GetCurrentCoinCount() {
		t.Error("Wrong coin count ", b.GetCurrentCoinCount())
	}
}

func TestCostValidation(t *testing.T) {
	b := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 3, PowPerNoOpBlock: 4, MinerId: "miner1", NumCoinsPerFileCreate: 1}
	block1 := b.MineBlock([]bc.Operation{bc.CreateOperation{time.Now(), "test", "miner1"}})
	b.Add(block1)
	if b.Add(block1) == nil {
		t.Error("Validation should have gailed")
	}

	b.Add(b.MineNopBlock())
	block2 := b.MineBlock([]bc.Operation{bc.CreateOperation{time.Now(), "testFile2", "miner1"}})
	if err := b.Add(block2); err != nil {
		t.Error("Validation Failed: ", err.Error())
	}
}

func TestFindLongestChain(t *testing.T) {
	blockchain1 := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 2, PowPerNoOpBlock: 3, MinerId: "miner1"}
	blockchain2 := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 2, PowPerNoOpBlock: 3, MinerId: "miner2"}

	for i := 0; i < 2; i++ {
		block := blockchain1.MineNopBlock()
		blockchain1.Add(block)

		if !blockchain1.Validate() {
			t.Error("Validation Failed")
		}
	}

	var finalHash string
	for i := 0; i < 4; i++ {
		block := blockchain2.MineNopBlock()
		blockchain1.Add(block)
		blockchain2.Add(block)
		finalHash = block.Hash()

		if !blockchain1.Validate() {
			t.Error("Validation Failed")
		}
	}

	if blockchain1.GetLongestChainHash() != finalHash {
		t.Error("Wrong final hash")
	}

	if blockchain1.GetLongestChainHash() != blockchain2.GetLongestChainHash() {
		t.Error("Blockchains don't agree")
	}
}

func TestFindLongestChains(t *testing.T) {
	blockchain1 := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 2, PowPerNoOpBlock: 3, MinerId: "miner1"}
	blockchain2 := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 2, PowPerNoOpBlock: 3, MinerId: "miner2"}

	var finalHash1 string
	for i := 0; i < 4; i++ {
		block := blockchain1.MineNopBlock()
		blockchain1.Add(block)
		finalHash1 = block.Hash()

		if !blockchain1.Validate() {
			t.Error("Validation Failed")
		}
	}

	var finalHash2 string
	for i := 0; i < 4; i++ {
		block := blockchain2.MineNopBlock()
		blockchain1.Add(block)
		blockchain2.Add(block)
		finalHash2 = block.Hash()

		if !blockchain1.Validate() {
			t.Error("Validation Failed")
		}
	}

	observedHashes := blockchain1.GetLongestChains()
	sort.Strings(observedHashes)
	expectedHashes := []string{finalHash1, finalHash2}
	sort.Strings(expectedHashes)

	if !reflect.DeepEqual(observedHashes, expectedHashes) {
		t.Error("Doesn't find every longest path")
	}
}

func TestAddValidation(t *testing.T) {
	b := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 2, PowPerNoOpBlock: 3, MinerId: "miner1"}
	block1 := b.MineBlock([]bc.Operation{bc.CreateOperation{time.Now(), "testFile1", "miner2"}})
	err := b.Add(block1)

	if err != nil {
		t.Error("Should have failed")
	}
}

func TestFileExists(t *testing.T) {
	// Create blockchain
	blockchain := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 3, PowPerNoOpBlock: 4, MinerId: "miner1"}

	// Mine no ops for coins
	for i := 0; i < 5; i++ {
		block := blockchain.MineNopBlock()
		blockchain.Add(block)

		if !blockchain.Validate() {
			t.Error("Validation Failed")
		}
	}

	// Create operations that will be used for blocks 1 to 4
	ops1 := []bc.Operation{bc.CreateOperation{Filename: "File1", MinerId: "miner1"},
		bc.CreateOperation{Filename: "File2", MinerId: "miner1"},
		bc.AppendOperation{Filename: "File2", Record: bc.Record{}, RecordNum: 0, MinerId: "miner1"},
		bc.CreateOperation{Filename: "File3", MinerId: "miner1"}}
	ops2 := []bc.Operation{bc.CreateOperation{Filename: "File4", MinerId: "miner2"}}
	ops3 := []bc.Operation{bc.AppendOperation{Filename: "File4", Record: bc.Record{}, RecordNum: 2, MinerId: "miner1"}}
	ops4 := []bc.Operation{bc.CreateOperation{Filename: "File5", MinerId: "miner1"},
		bc.CreateOperation{Filename: "File6", MinerId: "miner1"},
		bc.CreateOperation{Filename: "File7", MinerId: "miner1"}}

	// Create blocks from operations above and add them to the blockchain
	block1 := blockchain.MineBlock(ops1)
	blockchain.Add(block1)
	block2 := blockchain.MineBlock(ops2)
	blockchain.Add(block2)
	block3 := blockchain.MineBlock(ops3)
	blockchain.Add(block3)
	block4 := blockchain.SpoofMineBlock(ops4, "test")
	blockchain.Add(block4)

	file1Exists := blockchain.FileExistsOnLongestChain("File1")
	file3Exists := blockchain.FileExistsOnLongestChain("File1")
	file4Exists := blockchain.FileExistsOnLongestChain("File4")
	file99Exists := blockchain.FileExistsOnLongestChain("File99")
	file5Exists := blockchain.FileExistsOnLongestChain("File5")

	if file1Exists == false {
		t.Error("Could not find File1")
	}
	if file3Exists == false {
		t.Error("Could not find File3")
	}
	if file4Exists == false {
		t.Error("Could not find File4")
	}

	if file99Exists == true {
		t.Error("Found File99 (an unexisting file)")
	}
	if file5Exists == true {
		t.Error("Found File5 (a file that is not on the longest chain)")
	}

}

func TestFindIndexOfLastRecord(t *testing.T) {
	// Create blockchain
	blockchain := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 2, PowPerNoOpBlock: 3, MinerId: "miner1"}

	// Mine no ops for coins
	blockchain2 := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 2, PowPerNoOpBlock: 3, MinerId: "miner2"}
	for i := 0; i < 5; i++ {
		block1 := blockchain.MineNopBlock()
		blockchain2.Add(block1)
		blockchain.Add(block1)
		block2 := blockchain2.MineNopBlock()
		blockchain2.Add(block2)
		blockchain.Add(block2)

		if !blockchain.Validate() {
			t.Error("Validation Failed")
		}
	}

	// Create operations that will be used for blocks 1 to 4
	ops1 := []bc.Operation{bc.CreateOperation{Filename: "File1", MinerId: "miner1"},
		bc.CreateOperation{Filename: "File2", MinerId: "miner1"},
		bc.AppendOperation{Filename: "File2", Record: bc.Record{}, RecordNum: 0, MinerId: "miner1"}}
	ops2 := []bc.Operation{bc.CreateOperation{Filename: "File4", MinerId: "miner2"},
		bc.AppendOperation{Filename: "File2", Record: bc.Record{}, RecordNum: 1, MinerId: "miner2"},
		bc.AppendOperation{Filename: "File2", Record: bc.Record{}, RecordNum: 2, MinerId: "miner2"}}
	ops3 := []bc.Operation{bc.AppendOperation{Filename: "File4", Record: bc.Record{}, RecordNum: 0, MinerId: "miner3"}}
	ops4 := []bc.Operation{bc.CreateOperation{Filename: "File5", MinerId: "miner4"},
		bc.CreateOperation{Filename: "File6", MinerId: "miner4"},
		bc.CreateOperation{Filename: "File7", MinerId: "miner4"}}

	// Create blocks from operations above and add them to the blockchain
	block1 := blockchain.MineBlock(ops1)
	err := blockchain.Add(block1)
	if err != nil {
		t.Error(err.Error())
	}
	block2 := blockchain.MineBlock(ops2)
	err = blockchain.Add(block2)
	if err != nil {
		t.Error(err.Error())
	}
	block3 := blockchain.MineBlock(ops3)
	blockchain.Add(block3)
	block4 := blockchain.SpoofMineBlock(ops4, "test")
	blockchain.Add(block4)

	file2Index := blockchain.FindIndexOfLastRecord("File2")
	file4Index := blockchain.FindIndexOfLastRecord("File4")
	file99Index := blockchain.FindIndexOfLastRecord("File99")
	file5Index := blockchain.FindIndexOfLastRecord("File5")
	file1Index := blockchain.FindIndexOfLastRecord("File1")

	if file2Index != 2 {
		fmt.Println(file2Index)
		t.Error("Did not find proper index for File2")
	}
	if file4Index != 0 {
		fmt.Println(file4Index)
		t.Error("Did not find proper index for File4")
	}

	if file99Index != -1 {
		t.Error("Did not return -1 for non-existing file")
	}
	if file5Index != -1 {
		t.Error("Did not return -1 for file not in longest chain")
	}
	if file1Index != -1 {
		t.Error("Did not return -1 for file with no appends")
	}

}

func TestOperationValidation(t *testing.T) {
	b := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 2, PowPerNoOpBlock: 3, MinerId: "miner1"}
	b.Add(b.MineNopBlock())
	b.Add(b.MineNopBlock())
	b.Add(b.MineNopBlock())

	block1 := b.MineBlock([]bc.Operation{bc.CreateOperation{time.Now(), "testFile", "miner1"}})

	if b.Add(block1) != nil {
		t.Error("Validation Failed")
	}

	block2 := b.MineBlock([]bc.Operation{bc.CreateOperation{time.Now(), "testFile2", "miner1"},
		bc.AppendOperation{time.Now(), "testFile2", bc.Record{1, 2, 3, 4}, 0, "miner1"},
		bc.AppendOperation{time.Now(), "testFile2", bc.Record{1, 2, 3, 4}, 1, "miner1"}})

	if b.Add(block2) != nil {
		t.Error("Validation Failed")
	}

	block3 := b.MineBlock([]bc.Operation{bc.CreateOperation{time.Now(), "testFile", "miner1"}})

	if b.Add(block3) == nil {
		t.Error("Validation should have failed")
	}

	block4 := b.MineBlock([]bc.Operation{bc.AppendOperation{time.Now(), "testFile3", bc.Record{1, 2, 3, 4}, 0, "miner1"}})

	if b.Add(block4) == nil {
		t.Error("Validation should have failed")
	}

	block5 := b.MineBlock([]bc.Operation{bc.AppendOperation{time.Now(), "testFile2", bc.Record{1, 2, 3, 4}, 5, "miner1"}})

	if b.Add(block5) == nil {
		t.Error("Validation should have failed")
	}

	block6 := b.MineBlock([]bc.Operation{bc.AppendOperation{time.Now(), "testFile5", bc.Record{1, 2, 3, 4}, 1, "miner1"}, bc.AppendOperation{time.Now(), "testFile5", bc.Record{1, 2, 3, 4}, 0, "miner1"}})

	if b.Add(block6) == nil {
		t.Error("Validation should have failed")
	}
}

func TestGetFileList(t *testing.T) {
	b := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 2, PowPerNoOpBlock: 3, MinerId: "miner1"}
	b.Add(b.MineNopBlock())
	b.Add(b.MineNopBlock())
	b.Add(b.MineNopBlock())
	block1 := b.MineBlock([]bc.Operation{bc.CreateOperation{time.Now(), "testFile1", "miner1"}})
	b.Add(block1)
	block2 := b.MineBlock([]bc.Operation{
		bc.CreateOperation{time.Now(), "testFile2", "miner1"},
		bc.CreateOperation{time.Now(), "testFile3", "miner1"}})
	b.Add(block2)

	observedFiles := b.GetFileList()
	sort.Strings(observedFiles)
	expectedFiles := []string{"testFile1", "testFile2", "testFile3"}
	sort.Strings(expectedFiles)

	if !reflect.DeepEqual(observedFiles, expectedFiles) {
		t.Error("Doesn't list files")
	}

}

func TestRecap(t *testing.T) {
	blockchain1 := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 2, PowPerNoOpBlock: 3, MinerId: "miner1"}
	blockchain2 := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 2, PowPerNoOpBlock: 3, MinerId: "miner2"}
	blockchain3 := bc.Blockchain{GenesisBlockHash: "test", PowPerOpBlock: 2, PowPerNoOpBlock: 3, MinerId: "miner3"}

	block1 := blockchain1.MineNopBlock()
	blockchain1.Add(block1)
	blockchain3.Add(block1)
	fmt.Println(block1)

	for i := 0; i < 4; i++ {
		block := blockchain1.MineNopBlock()
		blockchain1.Add(block)

		if !blockchain1.Validate() {
			t.Error("Validation Failed")
		}
	}

	block2 := blockchain2.MineNopBlock()
	blockchain1.Add(block2)
	blockchain2.Add(block2)
	blockchain3.Add(block2)

	for i := 0; i < 3; i++ {
		block := blockchain2.MineNopBlock()
		blockchain1.Add(block)
		blockchain2.Add(block)

		if !blockchain1.Validate() {
			t.Error("Validation Failed")
		}
	}

	hashList := blockchain3.GetHashList()
	recap := blockchain1.BuildRecapList(hashList)

	for _, val := range recap {
		err := blockchain3.Add(val)
		if err != nil {
			t.Error(err)
		}
	}
	if blockchain3.GetLongestChainHash() != blockchain1.GetLongestChainHash() {
		t.Error("Recap didn't work")
	}
}
