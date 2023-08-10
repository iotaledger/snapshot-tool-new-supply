package main

import (
	"time"

	iotago3 "github.com/iotaledger/iota.go/v3"
)

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
