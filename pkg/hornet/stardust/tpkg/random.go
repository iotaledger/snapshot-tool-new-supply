package tpkg

import (
	"math/rand"
	"sync"
	"time"

	iotago "github.com/iotaledger/iota.go/v3"
)

var (
	//nolint:gosec // we don't care about weak random numbers here
	seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	randLock   = &sync.Mutex{}
)

func RandomIntn(n int) int {
	// Rand needs to be locked: https://github.com/golang/go/issues/3611
	randLock.Lock()
	defer randLock.Unlock()

	return seededRand.Intn(n)
}

// RandBytes returns length amount random bytes.
func RandBytes(length int) []byte {
	var b []byte
	for i := 0; i < length; i++ {
		b = append(b, byte(RandomIntn(256)))
	}

	return b
}

func RandNFTID() iotago.NFTID {
	nft := iotago.NFTID{}
	copy(nft[:], RandBytes(iotago.NFTIDLength))

	return nft
}

func RandAliasID() iotago.AliasID {
	alias := iotago.AliasID{}
	copy(alias[:], RandBytes(iotago.AliasIDLength))

	return alias
}

func RandAddress(addressType iotago.AddressType) iotago.Address {
	switch addressType {
	case iotago.AddressEd25519:
		address := &iotago.Ed25519Address{}
		addressBytes := RandBytes(32)
		copy(address[:], addressBytes)

		return address

	case iotago.AddressNFT:
		return RandNFTID().ToAddress()

	case iotago.AddressAlias:
		return RandAliasID().ToAddress()

	default:
		panic("unknown address type")
	}
}
