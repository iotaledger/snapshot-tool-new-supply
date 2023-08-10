package mergerator

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"golang.org/x/crypto/blake2b"

	iotago3 "github.com/iotaledger/iota.go/v3"
)

var (
	dayDuration     = time.Duration(86400000000000)
	weeklyDuration  = dayDuration * 7
	monthlyDuration = dayDuration * 30
)

func vestingIntervalFromStr(str string) time.Duration {
	switch str {
	case "daily":
		return dayDuration
	case "weekly":
		return weeklyDuration
	case "monthly":
		return monthlyDuration
	default:
		panic(fmt.Sprintf("invalid frequency: %s", str))
	}
}

// generates the times at which funds for the given investor become unlocked.
func vestingTimelocks(alloc Allocation, startDate time.Time) []*iotago3.TimelockUnlockCondition {
	unlocks := make([]*iotago3.TimelockUnlockCondition, 0)
	end := startDate.AddDate(alloc.Unlocks.VestingPeriodYears, 0, 0)
	for date := startDate; date.Before(end); date = date.Add(vestingIntervalFromStr(alloc.Unlocks.Frequency)) {
		unlocks = append(unlocks, &iotago3.TimelockUnlockCondition{UnixTime: uint32(date.Unix())})
	}
	return unlocks
}

func newOutputIDFromMarker(supplyIncreaseMarker []byte, outputIndex *uint32) iotago3.OutputID {
	outputID := iotago3.OutputID{}
	// 30 bytes marker, 4 bytes for index
	copy(outputID[:], supplyIncreaseMarker[:len(supplyIncreaseMarker)-2])
	binary.LittleEndian.PutUint32(outputID[len(outputID)-4:], *outputIndex)
	*outputIndex += 1
	return outputID
}

func generateVestingOutputs(
	cfg *Config, target iotago3.Address, alloc Allocation,
	vestedTokens uint64, supplyIncreaseMarker []byte, outputIndex *uint32,
) ([]iotago3.OutputID, []iotago3.Output, []*iotago3.TimelockUnlockCondition) {
	investorTimelocks := vestingTimelocks(alloc, cfg.Vesting.StartingDate)

	var controlSum uint64
	initialUnlock := uint64(float64(vestedTokens) * alloc.Unlocks.InitialUnlock)
	initialUnlock += (vestedTokens - initialUnlock) % uint64(len(investorTimelocks))
	fundsPerUnlock := (vestedTokens - initialUnlock) / uint64(len(investorTimelocks))

	// initial unlock output
	outputIDs := []iotago3.OutputID{newOutputIDFromMarker(supplyIncreaseMarker, outputIndex)}
	outputs := []iotago3.Output{
		&iotago3.BasicOutput{
			Amount:     initialUnlock,
			Conditions: iotago3.UnlockConditions{&iotago3.AddressUnlockCondition{Address: target}},
		},
	}
	controlSum += initialUnlock

	for i := 0; i < len(investorTimelocks); i++ {
		outputID := newOutputIDFromMarker(supplyIncreaseMarker, outputIndex)
		outputIDs = append(outputIDs, outputID)
		outputs = append(outputs, &iotago3.BasicOutput{
			Amount: fundsPerUnlock,
			Conditions: iotago3.UnlockConditions{
				&iotago3.AddressUnlockCondition{Address: target}, investorTimelocks[i],
			},
		})
		controlSum += fundsPerUnlock
	}

	if controlSum != vestedTokens {
		log.Panicf("control sum for vested outputs is not equal defined token amount: %d vs. %d", controlSum, vestedTokens)
	}

	return outputIDs, outputs, investorTimelocks
}

func generateVestingOutputsForGroup(alloc Allocation, cfg *Config, supplyIncreaseMarker []byte,
	currentOutputIndex *uint32,
) ([]iotago3.OutputID, []iotago3.Output) {
	outputIDs := make([]iotago3.OutputID, 0)
	outputs := make([]iotago3.Output, 0)

	targetDir := path.Join(cfg.CSV.Export.Dir, alloc.Name)
	_ = os.MkdirAll(targetDir, 0777)
	unlockAccumBalance := map[uint32]uint64{}
	for _, addrTuple := range alloc.Addresses {

		targetAddr, err := parseBech32Address(addrTuple.Address)
		if err != nil {
			log.Panicf("unable to parse address %s: %s", addrTuple.Address, err)
		}

		newOutputIDs, newOutputs, timelocks := generateVestingOutputs(
			cfg, targetAddr, alloc, mustParseUint64(addrTuple.Tokens), supplyIncreaseMarker, currentOutputIndex,
		)

		unlockAccumBalance[0] += newOutputs[0].Deposit()
		for i, timelock := range timelocks {
			unlockAccumBalance[timelock.UnixTime] += newOutputs[i+1].Deposit()
		}

		outputIDs = append(outputIDs, newOutputIDs...)
		outputs = append(outputs, newOutputs...)
		writeOutputIDTokenUnlockCSV(cfg, newOutputIDs, newOutputs, path.Join(targetDir, fmt.Sprintf("%s-%s.csv", addrTuple.Name, addrTuple.Address)))
	}
	writeTokenUnlockCSV(cfg, unlockAccumBalance, path.Join(targetDir, "summary.csv"))
	log.Printf("generated %d outputs, placed CSVs in %s, outputs+ids hashes %s/%s", len(outputs), targetDir, OutputsHash(outputs), OutputIDsHash(outputIDs))
	return outputIDs, outputs
}

// generates all the outputs holding the new supply
func GenerateNewSupplyOutputs(cfg *Config) ([]iotago3.OutputID, []iotago3.Output) {
	allOutputIDs := make([]iotago3.OutputID, 0)
	allOutputs := make([]iotago3.Output, 0)

	var currentOutputIndex uint32
	outputMarker := blake2b.Sum256([]byte(cfg.Vesting.OutputMarker))
	log.Printf("using marker '%s' to mark supply increase outputs", iotago3.EncodeHex(outputMarker[:len(outputMarker)-2]))

	for _, alloc := range cfg.Vesting.Allocations {
		if alloc.Rewards != nil {
			asmbTokensTotal, asmbTokensPerAddr := readAssemblyRewardFiles(cfg, alloc)
			alloc.Addresses = convertAssemblyToIOTA(mustParseUint64(alloc.Rewards.Tokens), asmbTokensTotal, asmbTokensPerAddr)
			log.Printf("there are %d addresses for '%s' receiving assembly rewards", len(alloc.Addresses), alloc.Name)
		}
		if alloc.Distribution != nil {
			asmbTokensTotal, asmbTokensPerAddr := readAssemblyDistributionFile(cfg, alloc)
			alloc.Addresses = convertAssemblyToIOTA(mustParseUint64(alloc.Distribution.Tokens), asmbTokensTotal, asmbTokensPerAddr)
			log.Printf("there are %d addresses for '%s' assembly distribution", len(alloc.Addresses), alloc.Name)
		}
		outputIDs, outputs := generateVestingOutputsForGroup(alloc, cfg, outputMarker[:], &currentOutputIndex)
		allOutputIDs = append(allOutputIDs, outputIDs...)
		allOutputs = append(allOutputs, outputs...)
	}

	return allOutputIDs, allOutputs
}
