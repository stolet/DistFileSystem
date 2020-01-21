package miner

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/DistributedClocks/GoVector/govec"
	"github.com/DistributedClocks/GoVector/govec/vrpc"
	"net/rpc"
	"strings"
	"time"
)
import bc "../blockchain"

type MinerApi int

type Peer struct {
	addr                string
	blockChannel        chan bc.Block
	opChannel           chan bc.Operation
	recapRequestChannel chan map[string]struct{}
	connected           bool
}

// Connect to this miner
func (m *MinerApi) Connect(args string, reply *bool) error {
	MinerInstance.MinerConnectionsLock.Lock()
	defer MinerInstance.MinerConnectionsLock.Unlock()
	fmt.Println("Got connect request from ", args)
	if _, ok := MinerInstance.MinerConnections[args]; !ok {
		peer := Peer{args, make(chan bc.Block, 1000), make(chan bc.Operation, 1000), make(chan map[string]struct{}, 3), true}
		MinerInstance.MinerConnections[args] = &peer
		go createAndManageConnection(peer)
	}

	*reply = true
	return nil
}

// A create operation that gets sent is received here
func (m *MinerApi) FloodOperation(args bc.CreateOperation, reply *bool) error {
	return recvOperation(args, reply)
}

// An append operation that gets sent is received here
func (m *MinerApi) FloodAppend(args bc.AppendOperation, reply *bool) error {
	return recvOperation(args, reply)
}

// An append operation that gets sent is received here
func (m *MinerApi) GetRecap(args map[string]struct{}, reply *[]byte) error {
	fmt.Println("Got recap request")
	MinerInstance.BlockchainMux.Lock()
	recapBlocks := MinerInstance.LocalBlockchain.BuildRecapList(args)
	MinerInstance.BlockchainMux.Unlock()

	bytes := EncodeBlockList(recapBlocks)
	*reply = bytes

	return nil
}

func EncodeBlockList(recapBlocks []bc.Block) []byte {
	encodedBytes := new(bytes.Buffer)
	gob.Register(bc.CreateOperation{})
	gob.Register(bc.AppendOperation{})
	encoder := gob.NewEncoder(encodedBytes)
	encoder.Encode(recapBlocks)
	bytes := encodedBytes.Bytes()
	return bytes
}

func DecodeBlockList(recapBlocks []byte) []bc.Block {
	gob.Register(bc.CreateOperation{})
	gob.Register(bc.AppendOperation{})
	decoder := gob.NewDecoder(bytes.NewBuffer(recapBlocks[:]))
	var blocks []bc.Block
	err := decoder.Decode(&blocks)
	if err != nil {
		fmt.Println(err)
	}
	return blocks
}

// An append operation that gets sent is received here
func (m *MinerApi) FloodBlock(args []byte, reply *bool) error {
	block := decodeBlock(args)
	MinerInstance.ReceivedBlocksLock.Lock()
	defer MinerInstance.ReceivedBlocksLock.Unlock()
	if _, ok := MinerInstance.ReceivedBlocks[block.Hash()]; !ok {
		MinerInstance.ReceivedBlocks[block.Hash()] = true
		fanOutBlock(block)
		fmt.Println("Received a block from a peer ", block.MinerId)
		MinerInstance.IncomingMinerBlocks <- block
	}

	*reply = true
	return nil
}

func decodeBlock(args []byte) bc.Block {
	gob.Register(bc.CreateOperation{})
	gob.Register(bc.AppendOperation{})
	gobobj := gob.NewDecoder(bytes.NewBuffer(args[:]))
	var block bc.Block
	err := gobobj.Decode(&block)
	if err != nil {
		fmt.Println(err)
	}
	return block
}

func encodeBlock(block bc.Block) []byte {
	encodedBytes := new(bytes.Buffer)
	gob.Register(bc.CreateOperation{})
	gob.Register(bc.AppendOperation{})
	gobobj := gob.NewEncoder(encodedBytes)
	gobobj.Encode(block)
	bytes := encodedBytes.Bytes()
	return bytes
}

func recvOperation(operation bc.Operation, reply *bool) error {
	MinerInstance.ReceivedOpLock.Lock()
	defer MinerInstance.ReceivedOpLock.Unlock()
	if _, ok := MinerInstance.ReceivedOps[bc.GetOperationHash(operation)]; !ok {
		MinerInstance.ReceivedOps[bc.GetOperationHash(operation)] = true
		fanOutOperation(operation)
		fmt.Println("Received a operation from a peer ", operation.GetMinerID())
		MinerInstance.IncomingMinerOps <- operation
	}

	*reply = true
	return nil
}

// Sends an append operation to the miner listening at minerAddr
func sendOperation(operation bc.Operation) {
	MinerInstance.MinerConnectionsLock.Lock()
	defer MinerInstance.MinerConnectionsLock.Unlock()
	for _, peer := range MinerInstance.MinerConnections {
		select {
		case peer.opChannel <- operation:
		default:
		}
	}

	MinerInstance.ReceivedOpLock.Lock()
	defer MinerInstance.ReceivedOpLock.Unlock()
	MinerInstance.ReceivedOps[bc.GetOperationHash(operation)] = true
}

func fanOutOperation(op bc.Operation) {
	MinerInstance.MinerConnectionsLock.Lock()
	defer MinerInstance.MinerConnectionsLock.Unlock()
	for _, peer := range MinerInstance.MinerConnections {
		select {
		case peer.opChannel <- op:
		default:
		}
	}
	MinerInstance.ReceivedOps[bc.GetOperationHash(op)] = true
}

func fanOutBlock(block bc.Block) {
	MinerInstance.MinerConnectionsLock.Lock()
	defer MinerInstance.MinerConnectionsLock.Unlock()
	for _, peer := range MinerInstance.MinerConnections {
		select {
		case peer.blockChannel <- block:
		default:
		}
	}

	MinerInstance.ReceivedBlocks[block.Hash()] = true
}

func sendBlock(block bc.Block) {
	MinerInstance.MinerConnectionsLock.Lock()
	defer MinerInstance.MinerConnectionsLock.Unlock()
	for _, peer := range MinerInstance.MinerConnections {
		select {
		case peer.blockChannel <- block:
		default:
		}
	}

	MinerInstance.ReceivedBlocksLock.Lock()
	defer MinerInstance.ReceivedBlocksLock.Unlock()

	MinerInstance.ReceivedBlocks[block.Hash()] = true
}

func sendRecapRequest(knownBlocks map[string]struct{}) {
	MinerInstance.MinerConnectionsLock.Lock()
	defer MinerInstance.MinerConnectionsLock.Unlock()
	for _, peer := range MinerInstance.MinerConnections {
		select {
		case peer.recapRequestChannel <- knownBlocks:
		default:
		}
	}
}

func createAndManageConnection(peer Peer) {
	logfilename := strings.Replace(peer.addr, ":", "-", 1)
	logger := govec.InitGoVector(peer.addr, logfilename, govec.GetDefaultConfig())
	options := govec.GetDefaultLogOptions()
	connected := false
	var minerConnection *rpc.Client
	lastAttempt := time.Now()
	fmt.Println("staring connection with ", peer.addr)
	for {
		if !connected {
			time.Sleep(time.Duration(time.Second*5) - time.Now().Sub(lastAttempt))
			var err error
			minerConnection, err = vrpc.RPCDial("tcp", peer.addr, logger, options)
			lastAttempt = time.Now()
			if err == nil {
				var reply = false
				err = minerConnection.Call("MinerApi.Connect", getExternalAddress(), &reply)
				if err == nil {
					connected = true
					MinerInstance.MinerConnectionsLock.Lock()
					MinerInstance.MinerConnections[peer.addr].connected = true
					MinerInstance.MinerConnectionsLock.Unlock()
				} else {
					fmt.Println(err)
				}
			} else {
				fmt.Println(err)
				continue
			}
		}
		select {
		case block := <-peer.blockChannel:
			var reply = false
			bytes := encodeBlock(block)

			err := minerConnection.Call("MinerApi.FloodBlock", bytes, &reply)

			if reply {
				fmt.Println(peer.addr, " received block")
				connected = true
				MinerInstance.MinerConnectionsLock.Lock()
				MinerInstance.MinerConnections[peer.addr].connected = true
				MinerInstance.MinerConnectionsLock.Unlock()
			} else {
				fmt.Println(peer.addr, " did not receive operation")
				connected = false
				MinerInstance.MinerConnectionsLock.Lock()
				MinerInstance.MinerConnections[peer.addr].connected = false
				MinerInstance.MinerConnectionsLock.Unlock()
				fmt.Println(err)
			}

		case operation := <-peer.opChannel:
			var reply = false
			var err error
			if operation.OperationType() == bc.CREATE {
				err = minerConnection.Call("MinerApi.FloodOperation", operation, &reply)
			} else {
				err = minerConnection.Call("MinerApi.FloodAppend", operation, &reply)
			}

			if reply {
				fmt.Println(peer.addr, " received operation")
				connected = true
				MinerInstance.MinerConnectionsLock.Lock()
				MinerInstance.MinerConnections[peer.addr].connected = true
				MinerInstance.MinerConnectionsLock.Unlock()
			} else {
				fmt.Println(peer.addr, " did not receive operation")
				connected = false
				MinerInstance.MinerConnectionsLock.Lock()
				MinerInstance.MinerConnections[peer.addr].connected = false
				MinerInstance.MinerConnectionsLock.Unlock()
				fmt.Println(err)
			}
		case knownBlocks := <-peer.recapRequestChannel:
			var reply []byte

			err := minerConnection.Call("MinerApi.GetRecap", knownBlocks, &reply)

			if err == nil {
				decodeBlockList := DecodeBlockList(reply)
				fmt.Println(peer.addr, " Recap received with ", len(decodeBlockList))
				MinerInstance.RecapChannel <- decodeBlockList
				connected = true
				MinerInstance.MinerConnectionsLock.Lock()
				MinerInstance.MinerConnections[peer.addr].connected = true
				MinerInstance.MinerConnectionsLock.Unlock()
			} else {
				fmt.Println(peer.addr, " Recap failed")
				fmt.Println(err)
				connected = false
				MinerInstance.MinerConnectionsLock.Lock()
				MinerInstance.MinerConnections[peer.addr].connected = false
				MinerInstance.MinerConnectionsLock.Unlock()

			}
		}
	}

}

func getExternalAddress() string {
	port := strings.Split(MinerInstance.MinerSettings.IncomingMinersAddr, ":")[1]
	return MinerInstance.MinerSettings.OutgoingMinersIP + ":" + port
}

// itializes a connection to all miners in this miner's list of peer addresses
func InitializeConnectionToAllPeers() {
	MinerInstance.MinerConnectionsLock.Lock()
	defer MinerInstance.MinerConnectionsLock.Unlock()

	for _, addr := range MinerInstance.MinerSettings.PeerMinersAddrs {
		peer := Peer{addr,
			make(chan bc.Block, 1000),
			make(chan bc.Operation, 1000),
			make(chan map[string]struct{}, 5),
			true}
		MinerInstance.MinerConnections[addr] = &peer
		go createAndManageConnection(peer)
	}
}
