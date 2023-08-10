package mergerator

import (
	"fmt"
	"os"
	"time"

	"github.com/iotaledger/hive.go/core/ioutils"
	iotago3 "github.com/iotaledger/iota.go/v3"
	"github.com/iotaledger/the-mergerator/pkg/hornet/stardust/tpkg"
)

// sample output for storage deposit calculation
var refTimelockedBasicOutput = &iotago3.BasicOutput{
	Conditions: iotago3.UnlockConditions{
		&iotago3.AddressUnlockCondition{Address: tpkg.RandAddress(iotago3.AddressEd25519)},
		&iotago3.TimelockUnlockCondition{UnixTime: 1337},
	},
}

var refNonTimelockedbasicOutput = &iotago3.BasicOutput{
	Conditions: iotago3.UnlockConditions{
		&iotago3.AddressUnlockCondition{Address: tpkg.RandAddress(iotago3.AddressEd25519)},
	},
}

type Config struct {
	ProtocolParameters struct {
		Version       byte   `json:"version"`
		NetworkName   string `json:"networkName"`
		Bech32HRP     string `json:"bech32Hrp"`
		MinPoWScore   uint32 `json:"minPowScore"`
		BelowMaxDepth uint8  `json:"belowMaxDepth"`
		RentStructure struct {
			VByteCost    uint32 `json:"vByteCost"`
			VBFactorData int    `json:"vbFactorData"`
			VBFactorKey  int    `json:"vbFactorKEy"`
		} `json:"rentStructure"`
		TokenSupply string `json:"tokenSupply"`
	} `json:"protocolParameters"`
	ValidateSupply bool   `json:"validateSupply"`
	TreasuryTokens string `json:"treasuryTokens"`
	Snapshot       struct {
		ChrysalisSnapshotFile    string `json:"chrysalisSnapshotFile"`
		OutputSnapshotFile       string `json:"outputSnapshotFile"`
		SkipSnapshotGeneration   bool   `json:"skipSnapshotGeneration"`
		GenesisMilestoneIndex    int    `json:"genesisMilestoneIndex"`
		TargetMilestoneIndex     int    `json:"targetMilestoneIndex"`
		TargetMilestoneTimestamp int    `json:"targetMilestoneTimestamp"`
		TargetMilestoneID        string `json:"targetMilestoneID"`
		LedgerMilestoneIndex     int    `json:"ledgerMilestoneIndex"`
	} `json:"snapshot"`
	CSV struct {
		Export struct {
			Active bool   `json:"active"`
			Dir    string `json:"dir"`
		} `json:"export"`
		Import struct {
			Active       bool   `json:"active"`
			OutputMarker string `json:"outputMarker"`
			LedgerFile   string `json:"ledgerFile"`
		} `json:"import"`
	} `json:"csv"`
	Vesting struct {
		StartingDate time.Time    `json:"startingDate"`
		OutputMarker string       `json:"outputMarker"`
		Allocations  []Allocation `json:"allocations"`
	}
	ParsedProtocolParameters           *iotago3.ProtocolParameters
	MinCostPerTimelockedBasicOutput    uint64
	MinCostPerNonTimelockedBasicOutput uint64
}

type Allocation struct {
	Name    string `json:"name"`
	Unlocks struct {
		Frequency          string  `json:"frequency"`
		InitialUnlock      float64 `json:"initialUnlock"`
		VestingPeriodYears int     `json:"vestingPeriodYears"`
	} `json:"unlocks"`
	Rewards *struct {
		Tokens string `json:"tokens"`
		Dir    string `json:"dir"`
	} `json:"rewards"`
	Distribution *struct {
		Exclude map[string]int `json:"exclude"`
		Tokens  string         `json:"tokens"`
		File    string         `json:"file"`
	} `json:"distribution"`
	Addresses []AddrBalanceTuple
}

type AddrBalanceTuple struct {
	Name    string `json:"name"`
	Address string `json:"address"`
	Tokens  string `json:"tokens"`
}

func LoadConfigFile(filePath string) (*Config, error) {
	cfg := &Config{}
	if err := ioutils.ReadJSONFromFile(filePath, cfg); err != nil {
		return nil, fmt.Errorf("unable to read config file: %w", err)
	}

	if _, err := os.Stat(cfg.Snapshot.ChrysalisSnapshotFile); err != nil || os.IsNotExist(err) {
		return nil, fmt.Errorf("chrysalis snapshot file missing: %w", err)
	}

	if _, err := os.Stat(cfg.Snapshot.OutputSnapshotFile); err == nil || !os.IsNotExist(err) {
		return nil, fmt.Errorf("output snapshot file '%s' already exists", cfg.Snapshot.OutputSnapshotFile)
	}

	println("loading protocol parameters ...")

	protoParams := iotago3.ProtocolParameters{
		Version:       cfg.ProtocolParameters.Version,
		NetworkName:   cfg.ProtocolParameters.NetworkName,
		Bech32HRP:     iotago3.NetworkPrefix(cfg.ProtocolParameters.Bech32HRP),
		MinPoWScore:   cfg.ProtocolParameters.MinPoWScore,
		BelowMaxDepth: cfg.ProtocolParameters.BelowMaxDepth,
		RentStructure: iotago3.RentStructure{
			VByteCost:    cfg.ProtocolParameters.RentStructure.VByteCost,
			VBFactorData: iotago3.VByteCostFactor(cfg.ProtocolParameters.RentStructure.VBFactorData),
			VBFactorKey:  iotago3.VByteCostFactor(cfg.ProtocolParameters.RentStructure.VBFactorKey),
		},
		TokenSupply: mustParseUint64(cfg.ProtocolParameters.TokenSupply),
	}

	cfg.ParsedProtocolParameters = &protoParams

	r := cfg.ParsedProtocolParameters.RentStructure
	cfg.MinCostPerTimelockedBasicOutput = uint64(r.VByteCost) * uint64((refTimelockedBasicOutput).VBytes(&r, nil))
	cfg.MinCostPerNonTimelockedBasicOutput = uint64(r.VByteCost) * uint64((refNonTimelockedbasicOutput).VBytes(&r, nil))

	return cfg, nil
}
