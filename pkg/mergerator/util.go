package mergerator

import (
	"log"
	"strconv"

	"golang.org/x/crypto/blake2b"

	"github.com/iotaledger/hive.go/serializer/v2"
	iotago3 "github.com/iotaledger/iota.go/v3"
)

func parseBech32Address(bech32Address string) (iotago3.Address, error) {
	_, address, err := iotago3.ParseBech32(bech32Address)
	if err != nil {
		log.Panicf("unable to convert bech32 address '%s': %s", bech32Address, err)
	}

	return address, nil
}

func OutputIDsHash(outputIDs []iotago3.OutputID) string {
	h, _ := blake2b.New256(nil)
	for _, outputID := range outputIDs {
		id := outputID.UTXOInput().ID()
		h.Write(id[:])
	}
	return iotago3.EncodeHex(h.Sum(nil))
}

func OutputsHash(outputs []iotago3.Output) string {
	h, _ := blake2b.New256(nil)
	for _, output := range outputs {
		outputBytes, err := output.Serialize(serializer.DeSeriModeNoValidation, nil)
		if err != nil {
			log.Panic(err)
		}
		h.Write(outputBytes)
	}
	return iotago3.EncodeHex(h.Sum(nil))
}

func mustParseUint64(str string) uint64 {
	num, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		log.Panicf("unable to parse uint64: %s", err)
	}
	return num
}
