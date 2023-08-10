package mergerator

import (
	"encoding/hex"
	"encoding/json"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/mr-tron/base58"

	"github.com/iotaledger/hive.go/core/ioutils"
	iotago3 "github.com/iotaledger/iota.go/v3"
)

func readAssemblyRewardFiles(cfg *Config, alloc Allocation) (uint64, map[string]uint64) {
	files, err := os.ReadDir(alloc.Rewards.Dir)
	if err != nil {
		log.Panicf("unable to read rewards directory: %s", err)
	}

	type rewards struct {
		Rewards map[string]uint64 `json:"rewards"`
	}

	var totalRewards uint64
	accumulatedRewardsPerAddress := make(map[string]uint64)
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		r := &rewards{}
		if err := ioutils.ReadJSONFromFile(path.Join(alloc.Rewards.Dir, f.Name()), r); err != nil {
			log.Panicf("unable to open rewards file %s: %s", f.Name(), err)
		}

		for k, v := range r.Rewards {
			var addr iotago3.Ed25519Address
			addrBytes, err := hex.DecodeString(k)
			if err != nil {
				log.Panicf("unable to decode hex encoded address '%s' in assembly reward file: %s", k, err)
			}
			copy(addr[:], addrBytes)

			bech32AddrStr := addr.Bech32(iotago3.NetworkPrefix(cfg.ProtocolParameters.Bech32HRP))
			totalRewards += v
			accumulatedRewardsPerAddress[bech32AddrStr] += v
		}
	}
	log.Printf("total rewards %d on %d addresses", totalRewards, len(accumulatedRewardsPerAddress))

	return totalRewards, accumulatedRewardsPerAddress
}

func readAssemblyDistributionFile(cfg *Config, alloc Allocation) (uint64, map[string]uint64) {
	asmbDistroData, err := os.ReadFile(alloc.Distribution.File)
	if err != nil {
		log.Panicf("unable to read assembly distribution file: %s", err)
	}

	type tuple struct {
		Base58Addr string `json:"address"`
		Balance    uint64 `json:"balance"`
	}

	var tuples []tuple
	if err := json.Unmarshal(asmbDistroData, &tuples); err != nil {
		log.Panicf("unable to parse assembly distribution file: %s", err)
	}

	var totalRewards uint64
	asmbTokensPerAddr := make(map[string]uint64)
	for _, tuple := range tuples {
		var addr iotago3.Ed25519Address

		if _, excluded := alloc.Distribution.Exclude[tuple.Base58Addr]; excluded {
			log.Printf("skipping assembly distribution address '%s' with %d balance", tuple.Base58Addr, tuple.Balance)
			continue
		}

		assemblyAddressBytes, err := base58.Decode(tuple.Base58Addr)
		if err != nil {
			log.Panicf("unable to decode base58 assembly address '%s': %s", tuple.Base58Addr, err)
		}
		// is prefixed with zero byte to indicate address type in distribution file
		copy(addr[:], assemblyAddressBytes[1:])

		bech32AddrStr := addr.Bech32(iotago3.NetworkPrefix(cfg.ProtocolParameters.Bech32HRP))
		totalRewards += tuple.Balance
		asmbTokensPerAddr[bech32AddrStr] += tuple.Balance
	}
	log.Printf("total assembly tokens %d on %d addresses", totalRewards, len(asmbDistroData))

	return totalRewards, asmbTokensPerAddr
}

func convertAssemblyToIOTA(iotaTokensToDistribute uint64, asmbTokensTotal uint64, asmbTokensPerAddr map[string]uint64) []AddrBalanceTuple {
	remainder := iotaTokensToDistribute

	type balancetuple struct {
		address           iotago3.Address
		assemblyRewards   uint64
		iotaRewards       uint64
		divisionRemainder float64
	}

	tuples := make([]balancetuple, 0)
	for addr, assemblyTokens := range asmbTokensPerAddr {
		addr, err := parseBech32Address(addr)
		if err != nil {
			log.Panicf("unable to parse address %s: %s", addr, err)
		}

		iotaRewardsFloat64 := float64(iotaTokensToDistribute) * (float64(assemblyTokens) / float64(asmbTokensTotal))
		iotaRewards := uint64(iotaRewardsFloat64)
		remainder -= iotaRewards
		tuples = append(tuples, balancetuple{
			addr, assemblyTokens,
			iotaRewards, iotaRewardsFloat64 - float64(iotaRewards)},
		)
	}

	log.Printf("iota remainder to distribute: %d", remainder)

	// sort by remainder of balance division
	sort.Slice(tuples, func(i, j int) bool {
		// reverse (highest first)
		return tuples[i].divisionRemainder > tuples[j].divisionRemainder
	})

	// fill up addresses fairly according to remainder (highest first)
	var iotaRewardsControl uint64
	var addresses []AddrBalanceTuple
	for i := 0; i < len(tuples); i++ {
		if remainder > 0 {
			tuples[i].iotaRewards += 1
			remainder--
		}
		iotaRewardsControl += tuples[i].iotaRewards
		addresses = append(addresses, AddrBalanceTuple{Address: tuples[i].address.Bech32("iota"), Tokens: strconv.FormatUint(tuples[i].iotaRewards, 10)})
	}

	if iotaRewardsControl != iotaTokensToDistribute {
		log.Panicf("total rewards distributed to addresses %d != %d", iotaRewardsControl, iotaTokensToDistribute)
	}

	// make address tuples ordering deterministic
	sort.Slice(addresses, func(i, j int) bool {
		return strings.Compare(addresses[i].Address, addresses[j].Address) < 0
	})

	return addresses
}
