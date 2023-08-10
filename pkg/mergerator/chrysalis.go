package mergerator

import (
	"log"
	"os"
	"sort"
	"strings"

	iotago2 "github.com/iotaledger/iota.go/v2"
	iotago3 "github.com/iotaledger/iota.go/v3"
	"github.com/iotaledger/the-mergerator/pkg/hornet/chrysalis"
)

type ChrysalisOutputs []*chrysalis.Output

func convertChrysalisToStardust(output *chrysalis.Output) iotago3.Output {
	addr := iotago3.Ed25519Address{}
	copy(addr[:], output.Address.(*iotago2.Ed25519Address)[:])
	addrUnlock := &iotago3.AddressUnlockCondition{Address: &addr}
	return &iotago3.BasicOutput{Amount: output.Amount, Conditions: iotago3.UnlockConditions{addrUnlock}}
}

func (outputs ChrysalisOutputs) ConvertToStardust() ([]iotago3.OutputID, []iotago3.Output) {
	var stardustOutputs []iotago3.Output
	var outputIDs []iotago3.OutputID
	for _, output := range outputs {
		stardustOutputs = append(stardustOutputs, convertChrysalisToStardust(output))
		outputIDs = append(outputIDs, output.OutputID)
	}
	return outputIDs, stardustOutputs
}

type ChrysalisSnapshotStats struct {
	TotalBalance              uint64 `json:"totalBalance"`
	TotalBalanceSumOutputs    uint64 `json:"totalBalanceSumOutputs"`
	TotalOutputsCount         uint64 `json:"totalOutputsCount"`
	DustAllowanceOutputsCount uint64 `json:"dustAllowanceOutputsCount"`
	DustOutputsCount          uint64 `json:"dustOutputsCount"`
	TotalDustBalance          uint64 `json:"totalDustBalance"`
	TreasuryFunds             uint64 `json:"treasuryFunds"`
}

type ChrysalisSnapshot struct {
	// Header of the snapshot
	Header *chrysalis.ReadFileHeader
	// All ledger outputs which are not dust allowance, dust or treasury outputs
	Outputs ChrysalisOutputs
	// All dust allowance dust outputs mapped by their address
	DustAllowanceOutputs map[string]ChrysalisOutputs
	// All dust outputs mapped by their address
	DustOutputs map[string]ChrysalisOutputs
	// Treasury output
	TreasuryOutput *chrysalis.TreasuryOutput
	// Stats of the snapshot
	Stats ChrysalisSnapshotStats
	// Solid Entry Point
	SolidEntryPointMessageID chrysalis.MessageID
}

func NewChrysalisSnapshot() *ChrysalisSnapshot {
	return &ChrysalisSnapshot{
		Outputs:              make(ChrysalisOutputs, 0),
		Header:               nil,
		DustAllowanceOutputs: make(map[string]ChrysalisOutputs),
		DustOutputs:          make(map[string]ChrysalisOutputs),
	}
}

// StardustOutputs converts the Chrysalis outputs into their Stardust representation.
// Dust outputs are auto. merged into the first dust allowance output on that address, where
// "first" means lexicographically sorted by the dust allowance output's ID.
func (s *ChrysalisSnapshot) StardustOutputs() ([]iotago3.OutputID, []iotago3.Output) {

	// convert non dust related outputs
	stardustOutputIDs, stardustOutputs := s.Outputs.ConvertToStardust()

	// consolidate dust outputs
	type dustallowancetuple struct {
		addr                 string
		dustAllowanceOutputs ChrysalisOutputs
	}

	// sort dust allowance outputs away from map for determinism
	dustAllowanceTuples := make([]dustallowancetuple, 0)
	for addr, dustAllowOutputs := range s.DustAllowanceOutputs {
		dustAllowanceTuples = append(dustAllowanceTuples, dustallowancetuple{
			addr:                 addr,
			dustAllowanceOutputs: dustAllowOutputs,
		})
	}

	sort.Slice(dustAllowanceTuples, func(i, j int) bool {
		return strings.Compare(dustAllowanceTuples[i].addr, dustAllowanceTuples[j].addr) < 0
	})

	for _, tuple := range dustAllowanceTuples {
		dustOutputs, has := s.DustOutputs[tuple.addr]
		// add dust outputs to "first" dust allowance output
		if has {
			// makes the target dust allowance output deterministic
			sort.Sort(chrysalis.LexicalOrderedOutputs(tuple.dustAllowanceOutputs))

			// turn on the vacuum
			var dustVacuumed uint64
			for _, dustOutput := range dustOutputs {
				dustVacuumed += dustOutput.Amount
			}
			// add dust bag
			tuple.dustAllowanceOutputs[0].Amount += dustVacuumed
		}

		dustOutputIDs, convDustOutputs := tuple.dustAllowanceOutputs.ConvertToStardust()
		stardustOutputIDs = append(stardustOutputIDs, dustOutputIDs...)
		stardustOutputs = append(stardustOutputs, convDustOutputs...)
	}

	return stardustOutputIDs, stardustOutputs
}

func ReadChrysalisSnapshot(cfg *Config) *ChrysalisSnapshot {
	chrysalisSnapshotFile, err := os.Open(cfg.Snapshot.ChrysalisSnapshotFile)
	if err != nil {
		log.Panicf("unable to open chrysalis snapshot file: %s", err)
	}

	chrysalisSnapshot := NewChrysalisSnapshot()

	if err := chrysalis.StreamSnapshotDataFrom(chrysalisSnapshotFile,
		// header
		func(header *chrysalis.ReadFileHeader) error {
			chrysalisSnapshot.Header = header
			return nil
		},
		// SEPs
		func(id chrysalis.MessageID) error {
			if chrysalisSnapshot.SolidEntryPointMessageID != nil {
				log.Panic("snapshot contains more than one SEP")
			}
			chrysalisSnapshot.SolidEntryPointMessageID = id
			return nil
		},
		// ledger
		func(output *chrysalis.Output) error {
			key := output.Address.(*iotago2.Ed25519Address).String()
			chrysalisSnapshot.Stats.TotalOutputsCount++

			switch {
			case output.OutputType == iotago2.OutputSigLockedDustAllowanceOutput:
				dustAllowanceOutputsSlice, has := chrysalisSnapshot.DustAllowanceOutputs[key]
				if !has {
					dustAllowanceOutputsSlice = append(dustAllowanceOutputsSlice, output)
				}
				chrysalisSnapshot.DustAllowanceOutputs[key] = dustAllowanceOutputsSlice
				chrysalisSnapshot.Stats.DustAllowanceOutputsCount++

			case isDustOutput(output):
				dustOutputsSlice, has := chrysalisSnapshot.DustOutputs[key]
				if !has {
					dustOutputsSlice = append(dustOutputsSlice, output)
				}
				chrysalisSnapshot.DustOutputs[key] = dustOutputsSlice
				chrysalisSnapshot.Stats.DustOutputsCount++
				chrysalisSnapshot.Stats.TotalDustBalance += output.Amount

			default:
				chrysalisSnapshot.Outputs = append(chrysalisSnapshot.Outputs, output)
			}

			chrysalisSnapshot.Stats.TotalBalanceSumOutputs += output.Amount
			return nil
		},
		// treasury
		func(output *chrysalis.TreasuryOutput) error {
			chrysalisSnapshot.Stats.TotalOutputsCount++
			chrysalisSnapshot.TreasuryOutput = output
			chrysalisSnapshot.Stats.TreasuryFunds = output.Amount
			return nil
		},
		// milestone diffs
		func(milestoneDiff *chrysalis.MilestoneDiff) error {
			return nil
		},
	); err != nil {
		log.Panicf("unable to read in chrysalis snapshot data: %s", err)
	}

	chrysalisSnapshot.Stats.TotalBalance = chrysalisSnapshot.Stats.TotalBalanceSumOutputs + chrysalisSnapshot.Stats.TreasuryFunds

	return chrysalisSnapshot
}

func isDustOutput(output *chrysalis.Output) bool {
	return output.Amount < 1000000
}
