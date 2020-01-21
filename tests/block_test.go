package tests

import (
	"testing"
	"time"
)
import b "../blockchain"

func TestFindNonce(t *testing.T) {

	block := b.Block{"asadfaewfaewfawe",
		[]b.Operation{b.CreateOperation{time.Now(), "testFile", "miner1"},
			b.AppendOperation{time.Now(), "test", b.Record{1, 2, 3, 4}, 2, "miner2"}}, "miner1", 0}
	block.FindNonce(4)

	hash := block.Hash()
	if hash[len(hash)-4:] != "0000" {
		t.Error("Hash is does not have correct number of zeros")
	}
}

func TestHashConsistency(t *testing.T) {

	time1, _ := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", "2018-11-02 14:34:26.8121512 -0700 PDT")
	time2, _ := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", "2018-11-02 14:34:27.7965198 -0700 PDT")
	time3, _ := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", "2018-11-02 14:34:28.7828831 -0700 PDT")

	block1 := b.Block{"2b7d5922b3a30b267fb33c661b600000",
		[]b.Operation{b.CreateOperation{time1, "File1", "Stacy"},
			b.CreateOperation{time2, "File2", "Stacy"},
			b.AppendOperation{time3, "File3", b.Record{1,2,3,5},2,"Stacy"}}, "Stacy", 1231}

	time4, _ := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", "2018-11-02 14:34:26.8121512 -0700 PDT")
	time5, _ := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", "2018-11-02 14:34:27.7965198 -0700 PDT")
	time6, _ := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", "2018-11-02 14:34:28.7828831 -0700 PDT")

	block2 := b.Block{"2b7d5922b3a30b267fb33c661b600000",
		[]b.Operation{b.CreateOperation{time4, "File1", "Stacy"},
			b.CreateOperation{time5, "File2", "Stacy"},
			b.AppendOperation{time6, "File3", b.Record{1,2,3,5},2,"Stacy"}}, "Stacy", 1231}

	if block1.Hash() != block2.Hash() {
		t.Error("hashes are not consistent")
	}
}
