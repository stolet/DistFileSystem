package miner

import (
	"fmt"
	"github.com/DistributedClocks/GoVector/govec"
	"github.com/DistributedClocks/GoVector/govec/vrpc"
	"log"
	"math"
	"net"
	"net/rpc"
	"reflect"
	"sync"
	"time"
)

import bc "../blockchain"

var MinerInstance Miner

type Miner struct {
	BlockchainMux        *sync.Mutex
	MinerMux             *sync.Mutex                // Lock
	MinerSettings        Settings                   // Settings for this miner
	TimeoutInitialized   bool                       // Flag that tells us if timeout was initialized or not
	LocalBlockchain      bc.Blockchain              // The copy of the blockchain of this miner
	OpsBundle            []OpAndChannels            // List of operations to be added to a block
	OpsWaitingForConf    []ConfirmationQueueElement // List of operations that have not been confirmed
	MinerConnections     map[string]*Peer           // Connections to peer miners
	ReceivedOps          map[string]bool            // Map of hashes of Received Ops
	ReceivedBlocks       map[string]bool            // Map of hashes of Received Ops
	MinerConnectionsLock *sync.Mutex                // Map lock
	ReceivedBlocksLock   *sync.Mutex                // Map lock
	ReceivedOpLock       *sync.Mutex                // Map lock
	RecapChannel         chan []bc.Block            // Channel that receives operations to be added to a block
	TimeoutChannel       chan bool                  // Channel that notifies miner that a timeout happened and the operations in OpsBundle should be turned into a block
	IncomingMinerOps     chan bc.Operation          // Operations that came from another miner
	IncomingMinerBlocks  chan bc.Block              // Blocks that came from another miner
	StopMining           chan bool
	ContinueMining       chan bool
}

type Settings struct {
	MinedCoinsPerOpBlock   uint8
	MinedCoinsPerNoOpBlock uint8
	NumCoinsPerFileCreate  uint8
	GenOpBLockTimeout      uint8
	GenesisBlockHash       string
	PowPerOpBlock          uint8
	PowPerNoOpBlock        uint8
	ConfirmsPerFileCreate  uint8
	ConfirmsPerFileAppend  uint8
	MinerID                string
	PeerMinersAddrs        []string
	IncomingMinersAddr     string
	OutgoingMinersIP       string
	IncomingClientsAddr    string
}

type OpAndChannels struct {
	Op                  bc.Operation
	ValidationChannel   *chan ValidMessage
	ConfirmationChannel *chan bool
	OperationFromMiner  bool
}

type ErrorType uint16

const (
	FILEALREADYEXISTS ErrorType = iota
	FILEDOESNOTEXIST
	FILEMAXLENREACHED
	NOERRORS
)

type ValidMessage struct {
	ErrorOccured bool
	Type         ErrorType
	Op           bc.Operation
}

type ConfirmationQueueElement struct {
	OpAndCh   OpAndChannels
	BlockHash string
	Counter   int
}

// Helper that returns a Blockchain struct with the appropriate settings
func createBlockchain(settings Settings) bc.Blockchain {
	return bc.Blockchain{
		Blocks:                 make(map[string]bc.Block),
		ChildrenMap:            make(map[string]map[string]struct{}),
		GenesisBlockHash:       settings.GenesisBlockHash,
		PowPerOpBlock:          settings.PowPerOpBlock,
		PowPerNoOpBlock:        settings.PowPerNoOpBlock,
		MinerId:                settings.MinerID,
		CoinMap:                make(map[string]map[string]int),
		NumCoinsPerFileCreate:  settings.NumCoinsPerFileCreate,
		MinedCoinsPerNoOpBlock: settings.MinedCoinsPerNoOpBlock,
		MinedCoinsPerOpBlock:   settings.MinedCoinsPerOpBlock,
	}
}

// Gives index of an operation in regards to the position that it's in the bundle waiting for timeout.
// - e.g If there are three append ops to filename in the queue, this function will return 2
func getIndexOfLastRecordInMiner(filename string, index int) int {
	var maxIndex int = 0
	for i, op := range MinerInstance.OpsBundle {
		if i == index {
			break
		}
		if op.Op.OperationType() == bc.APPEND && op.Op.GetFileName() == filename {
			maxIndex++
		}
	}
	return maxIndex
}

func fileExistsInMiner(filename string, index int) bool {
	exists := false
	for i, op := range MinerInstance.OpsBundle {
		if i == index {
			break
		}
		if op.Op.OperationType() == bc.CREATE && op.Op.GetFileName() == filename {
			return true
		}
	}
	return exists
}

/* Confirms all append operations in the block that is 'ConfirmsPerFileAppend' blocks away from the top of
   the longest chain.
 */
func confirmAppends() { // Helper to check if a file being created already exists in longest chain
	hash := MinerInstance.LocalBlockchain.GetLongestChainHash()
	depth := 0
	for hash != MinerInstance.LocalBlockchain.GenesisBlockHash {
		currentBlock := MinerInstance.LocalBlockchain.Blocks[hash]
		if int(MinerInstance.MinerSettings.ConfirmsPerFileAppend) <= depth {
			for i := 0; i < len(MinerInstance.OpsWaitingForConf); i++ {
				elt := MinerInstance.OpsWaitingForConf[i]
				if elt.Counter == 0 && !opOnLongestChain(elt.OpAndCh.Op) {
					if len(MinerInstance.OpsWaitingForConf) == i+1 {
						MinerInstance.OpsBundle = append(MinerInstance.OpsBundle, elt.OpAndCh)
						MinerInstance.OpsWaitingForConf = []ConfirmationQueueElement{}
						i--
					} else {
						MinerInstance.OpsBundle = append(MinerInstance.OpsBundle, elt.OpAndCh)
						MinerInstance.OpsWaitingForConf = append(MinerInstance.OpsWaitingForConf[:i], MinerInstance.OpsWaitingForConf[i+1:]...)
						i--
					}
				} else if elt.BlockHash == hash && elt.OpAndCh.Op.OperationType() == bc.APPEND && len(MinerInstance.OpsWaitingForConf) == i+1 {
					fmt.Println("Confirm this append: ", elt.OpAndCh.Op.GetFileName())
					*elt.OpAndCh.ConfirmationChannel <- true
					MinerInstance.OpsWaitingForConf = []ConfirmationQueueElement{}
					i--
				} else if elt.BlockHash == hash && elt.OpAndCh.Op.OperationType() == bc.APPEND {
					fmt.Println("Confirm this append: ", elt.OpAndCh.Op.GetFileName())
					*elt.OpAndCh.ConfirmationChannel <- true
					MinerInstance.OpsWaitingForConf = append(MinerInstance.OpsWaitingForConf[:i], MinerInstance.OpsWaitingForConf[i+1:]...)
					i--
				}
			}
		}
		depth++
		hash = currentBlock.PrevHash
	}
	decreaseCounterAppends()
}

func decreaseCounterAppends() {
	for i := 0; i < len(MinerInstance.OpsWaitingForConf); i++ {
		elt := MinerInstance.OpsWaitingForConf[i]
		if elt.OpAndCh.Op.OperationType() == bc.APPEND {
			elt.Counter = elt.Counter - 1
			MinerInstance.OpsWaitingForConf[i] = elt
		}
	}
}

func decreaseCounterCreates() {
	for i := 0; i < len(MinerInstance.OpsWaitingForConf); i++ {
		elt := MinerInstance.OpsWaitingForConf[i]
		if elt.OpAndCh.Op.OperationType() == bc.CREATE {
			elt.Counter = elt.Counter - 1
			MinerInstance.OpsWaitingForConf[i] = elt
		}
	}
}

func confirmCreates() { // Helper to check if a file being created already exists in longest chain
	hash := MinerInstance.LocalBlockchain.GetLongestChainHash()
	depth := 0
	for hash != MinerInstance.LocalBlockchain.GenesisBlockHash {
		currentBlock := MinerInstance.LocalBlockchain.Blocks[hash]
		if int(MinerInstance.MinerSettings.ConfirmsPerFileCreate) <= depth {
			for i := 0; i < len(MinerInstance.OpsWaitingForConf); i++ {
				elt := MinerInstance.OpsWaitingForConf[i]
				if elt.Counter <= 0 && !opOnLongestChain(elt.OpAndCh.Op) {
					if len(MinerInstance.OpsWaitingForConf) == i+1 {
						MinerInstance.OpsBundle = append(MinerInstance.OpsBundle, elt.OpAndCh)
						MinerInstance.OpsWaitingForConf = []ConfirmationQueueElement{}
						i--
					} else {
						MinerInstance.OpsBundle = append(MinerInstance.OpsBundle, elt.OpAndCh)
						MinerInstance.OpsWaitingForConf = append(MinerInstance.OpsWaitingForConf[:i], MinerInstance.OpsWaitingForConf[i+1:]...)
						i--
					}
				} else if elt.BlockHash == hash && elt.OpAndCh.Op.OperationType() == bc.CREATE && len(MinerInstance.OpsWaitingForConf) == i+1 {
					fmt.Println("Confirm this create: ", elt.OpAndCh.Op.GetFileName())
					*elt.OpAndCh.ConfirmationChannel <- true
					MinerInstance.OpsWaitingForConf = []ConfirmationQueueElement{}
					i--
				} else if elt.BlockHash == hash && elt.OpAndCh.Op.OperationType() == bc.CREATE {
					fmt.Println("Confirm this create: ", elt.OpAndCh.Op.GetFileName())
					*elt.OpAndCh.ConfirmationChannel <- true
					MinerInstance.OpsWaitingForConf = append(MinerInstance.OpsWaitingForConf[:i], MinerInstance.OpsWaitingForConf[i+1:]...)
					i--
				}
			}
		}
		depth++
		hash = currentBlock.PrevHash
	}
	decreaseCounterCreates()
}

func opOnLongestChain(op1 bc.Operation) bool {
	currentHash := MinerInstance.LocalBlockchain.GetLongestChainHash()
	for currentHash != MinerInstance.LocalBlockchain.GenesisBlockHash {
		currentBlock := MinerInstance.LocalBlockchain.Blocks[currentHash]
		listOfOperations := currentBlock.Operations
		for _, op2 := range listOfOperations {
			if reflect.DeepEqual(op1, op2) {
				return true
			}
		}
		currentHash = currentBlock.PrevHash
	}
	return false
}

/* Listens to new operations arriving from clients and adds them to the bundle of operations
   waiting for the timeout to happen before being added to the blockchain. This function also
   listens for a timeout message to be received. When a timeout is received, this function adds
   the operations in the current bundle to the blockchain and confirms any create or append ops
   that are now confirmed. The appropriate processes are then notified that their operation is
   confirmed and that they can now return to caller.
 */
func initializeOperationBundling() {
	for {
		select {
		case <-MinerInstance.TimeoutChannel: // MineBlock and add to local blockchain on timeout
			MinerInstance.StopMining <- true
			MinerInstance.BlockchainMux.Lock()
			MinerInstance.MinerMux.Lock()
			fmt.Println("Mine op block")
			MinerInstance.OpsBundle = filterInvalidOpsFromBundle(MinerInstance.OpsBundle)
			setRecordNumOfAppends()
			opsAndChannelsForMining := getOpsForMining()
			opsForMining := getOpsFromOpsAndChannels(opsAndChannelsForMining)
			MinerInstance.MinerMux.Unlock()
			MinerInstance.BlockchainMux.Unlock()
			block := MinerInstance.LocalBlockchain.MineBlock(opsForMining)
			MinerInstance.BlockchainMux.Lock()
			fmt.Println("Adding ", block.Hash(), " to blockchain: ", block)
			err := MinerInstance.LocalBlockchain.Add(block)
			if err != nil {
				fmt.Println(err)
			}
			sendBlock(block)
			MinerInstance.MinerMux.Lock()
			addOpsToConfirmationQueue(opsAndChannelsForMining, block.Hash())
			confirmCreates()
			confirmAppends()
			MinerInstance.TimeoutInitialized = false
			fmt.Println("Number of coins (mined op) = ", MinerInstance.LocalBlockchain.GetCurrentCoinCount())
			initializeTimeoutIfWeHaveCoinsForOps()
			MinerInstance.MinerMux.Unlock()
			MinerInstance.BlockchainMux.Unlock()
			MinerInstance.ContinueMining <- true
		case incomingOp := <-MinerInstance.IncomingMinerOps:
			// Handle incoming ops here
			MinerInstance.BlockchainMux.Lock()
			hash := MinerInstance.LocalBlockchain.GetLongestChainHash()
			if MinerInstance.LocalBlockchain.ValidateOperation(incomingOp, hash) {
				MinerInstance.MinerMux.Lock()
				opAndChannels := OpAndChannels{Op: incomingOp, ValidationChannel: nil, ConfirmationChannel: nil, OperationFromMiner: false}
				MinerInstance.OpsBundle = append(MinerInstance.OpsBundle, opAndChannels)
				MinerInstance.MinerMux.Unlock()
			}
			MinerInstance.BlockchainMux.Unlock()
		case blocks := <-MinerInstance.RecapChannel:
			MinerInstance.BlockchainMux.Lock()
			fmt.Println("Recapping ", len(blocks), " blocks")
			for _, block := range blocks {
				fmt.Println("recap: ", block, " which has hash ", block.Hash())
				err := MinerInstance.LocalBlockchain.Add(block)
				if err != nil {
					fmt.Println(err)
				}
			}
			MinerInstance.BlockchainMux.Unlock()
		case incomingBlock := <-MinerInstance.IncomingMinerBlocks:
			// Handle incoming block here
			MinerInstance.BlockchainMux.Lock()
			if MinerInstance.LocalBlockchain.ValidPrevHash(incomingBlock) {
				err := MinerInstance.LocalBlockchain.Add(incomingBlock)
				if err != nil {
					fmt.Println("Adding ", incomingBlock.Hash(), " to blockchain failed: ", incomingBlock)
					fmt.Println(err)
				} else {
					fmt.Println("Adding to blockchain:", incomingBlock)
				}
			} else {
				sendRecapRequest(MinerInstance.LocalBlockchain.GetHashList())
			}

			MinerInstance.MinerMux.Lock()
			confirmCreates()
			confirmAppends()
			MinerInstance.MinerMux.Unlock()
			MinerInstance.BlockchainMux.Unlock()
		}
	}
}

func filterInvalidOpsFromBundle(opsBundle []OpAndChannels) []OpAndChannels {
	for i := 0; i < len(opsBundle); i++ {
		elt := opsBundle[i]
		if elt.Op.OperationType() == bc.CREATE {
			validMessage := checkCreateValidity(elt.Op, i)
			if !validMessage.ErrorOccured {
				fmt.Println("Create ", elt.Op.GetFileName(), " is valid")
				if elt.OperationFromMiner {
					*elt.ValidationChannel <- validMessage
				}
			} else {
				if len(opsBundle) == i+1 {
					fmt.Println("Create ", elt.Op.GetFileName(), " not valid")
					if elt.OperationFromMiner {
						*elt.ValidationChannel <- validMessage
					}
					opsBundle = []OpAndChannels{}
					i--
				} else {
					fmt.Println("Create ", elt.Op.GetFileName(), " not valid")
					if elt.OperationFromMiner {
						*elt.ValidationChannel <- validMessage
					}
					opsBundle = append(opsBundle[:i], opsBundle[i+1:]...)
					i--
				}
			}
		} else if elt.Op.OperationType() == bc.APPEND {
			validMessage := checkAppendValidity(elt.Op, i)
			if !validMessage.ErrorOccured {
				fmt.Println("Append ", elt.Op.GetFileName(), " is valid")
				if elt.OperationFromMiner {
					*elt.ValidationChannel <- validMessage
				}
			} else {
				if len(opsBundle) == i+1 {
					fmt.Println("Append ", elt.Op.GetFileName(), " not valid")
					if elt.OperationFromMiner {
						*elt.ValidationChannel <- validMessage
					}
					opsBundle = []OpAndChannels{}
					i--
				} else {
					fmt.Println("Append ", elt.Op.GetFileName(), " not valid")
					if elt.OperationFromMiner {
						*elt.ValidationChannel <- validMessage
					}
					opsBundle = append(opsBundle[:i], opsBundle[i+1:]...)
					i--
				}
			}
		}
	}
	return opsBundle
}

func checkCreateValidity(op bc.Operation, i int) ValidMessage {
	fileExists := MinerInstance.LocalBlockchain.FileExistsOnLongestChain(op.GetFileName()) || fileExistsInMiner(op.GetFileName(), i)
	if !fileExists {
		return ValidMessage{ErrorOccured: false, Type: NOERRORS, Op: op}
	} else {
		return ValidMessage{ErrorOccured: true, Type: FILEALREADYEXISTS, Op: op}
	}
}

func checkAppendValidity(op bc.Operation, i int) ValidMessage {
	fileExists := MinerInstance.LocalBlockchain.FileExistsOnLongestChain(op.GetFileName()) || fileExistsInMiner(op.GetFileName(), i)
	if fileExists {
		indexOfLastRecord := MinerInstance.LocalBlockchain.FindIndexOfLastRecord(op.GetFileName()) + getIndexOfLastRecordInMiner(op.GetFileName(), i) + 1
		newAppend := bc.AppendOperation{Filename: op.GetFileName(), MinerId: op.GetMinerID(), Timestamp: op.GetTimestamp(), RecordNum: uint16(indexOfLastRecord), Record: op.(bc.AppendOperation).GetRecord()}
		if uint16(indexOfLastRecord) >= math.MaxUint16 {
			return ValidMessage{ErrorOccured: true, Type: FILEMAXLENREACHED, Op: newAppend}
		} else {
			return ValidMessage{ErrorOccured: false, Type: NOERRORS, Op: newAppend}
		}
	} else {
		return ValidMessage{ErrorOccured: true, Type: FILEDOESNOTEXIST, Op: op}
	}
}

func setRecordNumOfAppends() {
	for i, elt := range MinerInstance.OpsBundle {
		if elt.Op.OperationType() == bc.APPEND {
			indexOfLastRecord := MinerInstance.LocalBlockchain.FindIndexOfLastRecord(elt.Op.GetFileName()) + getIndexOfLastRecordInMiner(elt.Op.GetFileName(), i) + 1
			newAppend := bc.AppendOperation{
				Timestamp: elt.Op.GetTimestamp(),
				Filename:  elt.Op.GetFileName(),
				Record:    elt.Op.(bc.AppendOperation).GetRecord(),
				RecordNum: uint16(indexOfLastRecord),
				MinerId:   elt.Op.GetMinerID()}
			MinerInstance.OpsBundle[i] = OpAndChannels{Op: newAppend, ValidationChannel: elt.ValidationChannel, ConfirmationChannel: elt.ConfirmationChannel, OperationFromMiner: elt.OperationFromMiner}
		}
	}
}

func getOpsForMining() []OpAndChannels {
	opsToMine := []OpAndChannels{}
	availableMoney := MinerInstance.LocalBlockchain.GetCurrentCoinCount()
	for i := 0; i < len(MinerInstance.OpsBundle); i++ {
		elt := MinerInstance.OpsBundle[i]

		if availableMoney <= getCostForOp(elt.Op) {
			break
		} else {
			opsToMine = append(opsToMine, elt)
			if len(MinerInstance.OpsBundle) == i+1 {
				MinerInstance.OpsBundle = []OpAndChannels{}
			} else {
				MinerInstance.OpsBundle = append(MinerInstance.OpsBundle[:i], MinerInstance.OpsBundle[i+1:]...)
			}
			if elt.OperationFromMiner {
				availableMoney = availableMoney - getCostForOp(elt.Op)
				if elt.Op.OperationType() == bc.CREATE {
					fmt.Println("Number of coins (Create file): ", availableMoney)
				} else {
					fmt.Println("Number of coins (Append file): ", availableMoney)
				}
			}
			i--
		}
	}
	for i := 0; i < len(MinerInstance.OpsBundle); i++ {
		elt := MinerInstance.OpsBundle[i]
		if !elt.OperationFromMiner {
			if len(MinerInstance.OpsBundle) == i+1 {
				opsToMine = append(opsToMine, elt)
				MinerInstance.OpsBundle = []OpAndChannels{}
			} else {
				opsToMine = append(opsToMine, elt)
				MinerInstance.OpsBundle = append(MinerInstance.OpsBundle[:i], MinerInstance.OpsBundle[i+1:]...)
			}
		}
	}
	return opsToMine
}

func getOpsFromOpsAndChannels(opsAndChannels []OpAndChannels) []bc.Operation {
	ops := []bc.Operation{}
	for _, elt := range opsAndChannels {
		ops = append(ops, elt.Op)
	}
	return ops
}

func startMiningNops() {
	for {
		select {
		case <-MinerInstance.StopMining:
			<-MinerInstance.ContinueMining
		default:
			MinerInstance.BlockchainMux.Lock()
			prevHash := MinerInstance.LocalBlockchain.GetLongestChainHash()
			MinerInstance.BlockchainMux.Unlock()
			block := MinerInstance.LocalBlockchain.MineNopBlockWithHash(prevHash)

			MinerInstance.BlockchainMux.Lock()
			MinerInstance.LocalBlockchain.Add(block)
			fmt.Println("Adding to blockchain:", block, " with hash ", block.Hash())
			sendBlock(block)

			MinerInstance.MinerMux.Lock()
			fmt.Println("Number of coins (mined nop) = ", MinerInstance.LocalBlockchain.GetCurrentCoinCount())
			confirmCreates()
			confirmAppends()
			initializeTimeoutIfWeHaveCoinsForOps()
			MinerInstance.BlockchainMux.Unlock()
			MinerInstance.MinerMux.Unlock()
		}
	}
}

func getNumberOfOpsFromMiner() int {
	sum := 0
	for _, elt := range MinerInstance.OpsBundle {
		if elt.OperationFromMiner {
			sum++
		}
	}
	return sum
}

func getFirstOpFromMiner() *OpAndChannels {
	for _, elt := range MinerInstance.OpsBundle {
		if elt.OperationFromMiner {
			return &elt
		}
	}
	return nil
}

func initializeTimeoutIfWeHaveCoinsForOps() {
	if !MinerInstance.TimeoutInitialized {
		if getNumberOfOpsFromMiner() != 0 {
			firstOp := getFirstOpFromMiner()
			if firstOp != nil {
				cost := getCostForOp(MinerInstance.OpsBundle[0].Op)
				if cost <= MinerInstance.LocalBlockchain.GetCurrentCoinCount() {
					MinerInstance.TimeoutInitialized = true
					fmt.Println("INITIALIZING TIMEOUT")
					go initializeTimeout()
				}
			}
		}
	}
}

/* Add operations in bundle waiting to be mined to the queue of operations waiting to be confirmed.
   This function is called right after the bundle that was waiting for a timeout is added to the queue.
   Elements in the queue are made up of an operation, the hash of the block it belongs to, and a channel
   that is used to notify the function that is waiting for a confirmation.
 */
func addOpsToConfirmationQueue(opsAndChannels []OpAndChannels, hash string) {
	for _, elt := range opsAndChannels {
		if elt.OperationFromMiner {
			opWaitingForConf := ConfirmationQueueElement{
				OpAndCh:   elt,
				BlockHash: hash,
				Counter:   getConfirmsForOps(elt.Op) + 5}
			MinerInstance.OpsWaitingForConf = append(MinerInstance.OpsWaitingForConf, opWaitingForConf)
		}
	}
}

func getConfirmsForOps(op bc.Operation) int {
	if op.OperationType() == bc.CREATE {
		return int(MinerInstance.MinerSettings.ConfirmsPerFileCreate)
	} else {
		return int(MinerInstance.MinerSettings.ConfirmsPerFileAppend)
	}
}

func getCostForOp(op bc.Operation) int {
	if op.OperationType() == bc.CREATE {
		return int(MinerInstance.MinerSettings.NumCoinsPerFileCreate)
	} else {
		return 1
	}
}

// Starts counting timeout and notifies timeout channel when timeout is done
func initializeTimeout() {
	time.Sleep(time.Duration(MinerInstance.MinerSettings.GenOpBLockTimeout) * time.Second)
	MinerInstance.TimeoutChannel <- true
}

// Creates an instance of a miner with the appropriate settings and starts the RPC server
func StartMiner(settings Settings) {
	// Register an RPC handler.
	blockchain := createBlockchain(settings)
	MinerInstance = Miner{
		LocalBlockchain:      blockchain,
		MinerSettings:        settings,
		OpsWaitingForConf:    []ConfirmationQueueElement{},
		OpsBundle:            []OpAndChannels{},
		RecapChannel:         make(chan []bc.Block, 100),
		TimeoutChannel:       make(chan bool, 100),
		IncomingMinerOps:     make(chan bc.Operation, 100),
		IncomingMinerBlocks:  make(chan bc.Block, 100),
		StopMining:           make(chan bool),
		ContinueMining:       make(chan bool),
		MinerConnections:     make(map[string]*Peer),
		ReceivedOps:          make(map[string]bool),
		ReceivedBlocks:       make(map[string]bool),
		MinerConnectionsLock: &sync.Mutex{},
		ReceivedBlocksLock:   &sync.Mutex{},
		ReceivedOpLock:       &sync.Mutex{},
		TimeoutInitialized:   false,
		MinerMux:             &sync.Mutex{},
		BlockchainMux:        &sync.Mutex{}}
	logger := govec.InitGoVector(MinerInstance.MinerSettings.MinerID, MinerInstance.MinerSettings.MinerID, govec.GetDefaultConfig())
	fmt.Println()
	client := new(ClientApi)
	minerApi := new(MinerApi)
	server := rpc.NewServer()
	server.Register(client)
	server.Register(minerApi)
	InitializeConnectionToAllPeers()
	go initializeOperationBundling()
	clientListener, errorClient := net.Listen("tcp", MinerInstance.MinerSettings.IncomingClientsAddr)
	minerListener, errorMiner := net.Listen("tcp", MinerInstance.MinerSettings.IncomingMinersAddr)
	if errorClient != nil {
		log.Fatal("listen error:", errorClient)
		fmt.Println(clientListener)
	}
	if errorMiner != nil {
		log.Fatal("listen error:", errorMiner)
	}
	options := govec.GetDefaultLogOptions()
	go vrpc.ServeRPCConn(server, clientListener, logger, options)
	go vrpc.ServeRPCConn(server, minerListener, logger, options)
	go startMiningNops()
	// Sleep for 600 seconds (a hack to make the server survive
	// long-enough for the rfslib to do its thing).
	// TODO: perform proper termination (e.g., on key press).
	time.Sleep(10000 * 1000 * time.Millisecond)
}
