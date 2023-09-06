package main

import (
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mr-tron/base58"
	"golang.org/x/crypto/blake2b"

	"github.com/iotaledger/hive.go/core/ioutils"
	"github.com/iotaledger/hive.go/serializer/v2"
	iotago2 "github.com/iotaledger/iota.go/v2"
	iotago3 "github.com/iotaledger/iota.go/v3"

	"github.com/iotaledger/the-mergerator/pkg/hornet/chrysalis"
	"github.com/iotaledger/the-mergerator/pkg/hornet/stardust"
	"github.com/iotaledger/the-mergerator/pkg/hornet/stardust/tpkg"
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
		StartingDate time.Time    `json:"startDate"`
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

// AddrBalanceTuple represents an allocation to an address.
type AddrBalanceTuple struct {
	Name    string `json:"name"`
	Address string `json:"address"`
	Tokens  string `json:"tokens"`
}

var (
	dayDuration      = time.Hour * 24
	weeklyDuration   = dayDuration * 7
	biWeeklyDuration = weeklyDuration * 2
	monthlyDuration  = dayDuration * 30
)

// parses a frequency duration string to a time.Duration
func vestingIntervalFromStr(str string) time.Duration {
	switch str {
	case "daily":
		return dayDuration
	case "weekly":
		return weeklyDuration
	case "bi-weekly":
		return biWeeklyDuration
	case "monthly":
		return monthlyDuration
	default:
		panic(fmt.Sprintf("invalid frequency: %s", str))
	}
}

// generates timelock unlock conditions given the Allocation's frequency and vesting period.
func vestingTimelocks(alloc Allocation, startDate time.Time) []*iotago3.TimelockUnlockCondition {
	unlocks := make([]*iotago3.TimelockUnlockCondition, 0)
	end := startDate.UTC().AddDate(alloc.Unlocks.VestingPeriodYears, 0, 0)
	interval := vestingIntervalFromStr(alloc.Unlocks.Frequency)
	for date := startDate.UTC().Add(interval); date.Before(end); date = date.Add(interval) {
		unlocks = append(unlocks, &iotago3.TimelockUnlockCondition{UnixTime: uint32(date.Unix())})
	}
	return unlocks
}

func parseBech32Address(bech32Address string) (iotago3.Address, error) {
	_, address, err := iotago3.ParseBech32(bech32Address)
	if err != nil {
		log.Panicf("unable to convert bech32 address '%s': %s", bech32Address, err)
	}

	return address, nil
}

func NewChrysalisSnapshot() *ChrysalisSnapshot {
	return &ChrysalisSnapshot{
		Outputs:              make(ChrysalisOutputs, 0),
		Header:               nil,
		DustAllowanceOutputs: make(map[string]ChrysalisOutputs),
		DustOutputs:          make(map[string]ChrysalisOutputs),
	}
}

type ChrysalisOutputs []*chrysalis.Output

func (outputs ChrysalisOutputs) ConvertToStardust() ([]iotago3.OutputID, []iotago3.Output) {
	var stardustOutputs []iotago3.Output
	var outputIDs []iotago3.OutputID
	for _, output := range outputs {
		stardustOutputs = append(stardustOutputs, convertChrysalisToStardust(output))
		outputIDs = append(outputIDs, output.OutputID)
	}
	return outputIDs, stardustOutputs
}

// ChrysalisSnapshot represents a Chrysalis snapshot.
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
	// Metadata of the snapshot
	Metadata ChrysalisSnapshotMetadata
	// Solid Entry Point
	SolidEntryPointMessageID chrysalis.MessageID
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
		outputs := dustAllowOutputs

		dustAllowanceTuples = append(dustAllowanceTuples, dustallowancetuple{
			addr:                 addr,
			dustAllowanceOutputs: outputs,
		})
	}

	sort.Slice(dustAllowanceTuples, func(i, j int) bool {
		return strings.Compare(dustAllowanceTuples[i].addr, dustAllowanceTuples[j].addr) < 0
	})

	// check if we consumed all dust outputs, therefore create a copy of the map
	dustOutputsMap := make(map[string]ChrysalisOutputs)
	for k, o := range s.DustOutputs {
		outputs := o
		dustOutputsMap[k] = outputs
	}

	for _, tuple := range dustAllowanceTuples {
		dustOutputs, has := dustOutputsMap[tuple.addr]
		// add dust outputs to "first" dust allowance output
		if has {
			// delete it from the map
			delete(dustOutputsMap, tuple.addr)

			// turn on the vacuum
			var dustVacuumed uint64
			for _, dustOutput := range dustOutputs {
				dustVacuumed += dustOutput.Amount
			}

			// makes the target dust allowance output deterministic
			sort.Sort(chrysalis.LexicalOrderedOutputs(tuple.dustAllowanceOutputs))

			// add dust bag to the first dust allowance output of that address (lexically ordered)
			tuple.dustAllowanceOutputs[0].Amount += dustVacuumed
		}

		dustOutputIDs, convDustOutputs := tuple.dustAllowanceOutputs.ConvertToStardust()
		stardustOutputIDs = append(stardustOutputIDs, dustOutputIDs...)
		stardustOutputs = append(stardustOutputs, convDustOutputs...)
	}

	if len(dustOutputsMap) > 0 {
		log.Panic("not all dust outputs could be vaccumed, seems like dust allowance is missing")
	}

	return stardustOutputIDs, stardustOutputs
}

func convertChrysalisToStardust(output *chrysalis.Output) iotago3.Output {
	addr := iotago3.Ed25519Address{}
	copy(addr[:], output.Address.(*iotago2.Ed25519Address)[:])
	addrUnlock := &iotago3.AddressUnlockCondition{Address: &addr}
	return &iotago3.BasicOutput{Amount: output.Amount, Conditions: iotago3.UnlockConditions{addrUnlock}}
}

// ChrysalisSnapshotMetadata represents the metadata around a Chrysalis based snapshot.
type ChrysalisSnapshotMetadata struct {
	// The total balance of all outputs + treasury occurred within the snapshot.
	TotalBalance uint64 `json:"totalBalance"`
	// The total balance of all outputs within the snapshot.
	TotalBalanceSumOutputs uint64 `json:"totalBalanceSumOutputs"`
	// The total amount of outputs within the snapshot.
	TotalOutputsCount uint64 `json:"totalOutputsCount"`
	// The total amount of dust allowance outputs within the snapshot.
	DustAllowanceOutputsCount uint64 `json:"dustAllowanceOutputsCount"`
	// The total amount of dust outputs within the snapshot.
	DustOutputsCount uint64 `json:"dustOutputsCount"`
	// The total amount of dust balance within the snapshot.
	TotalDustBalance uint64 `json:"totalDustBalance"`
	// The balance of tokens within the treasury.
	Treasury uint64 `json:"treasury"`
}

func main() {
	// Define a string flag for the config file, with a default value of "config.json"
	configFile := flag.String("configFile", "./config.json", "Path to the config file")

	// Parse the command-line flags
	flag.Parse()

	// Read the config file
	cfg := &Config{}
	if err := ioutils.ReadJSONFromFile(*configFile, cfg); err != nil {
		log.Panicf("unable to read config file: %s", err)
	}

	treasuryTokens := mustParseUint64(cfg.TreasuryTokens)

	if _, err := os.Stat(cfg.Snapshot.ChrysalisSnapshotFile); err != nil || os.IsNotExist(err) {
		log.Panicf("Chrysalis snapshot file missing: %s", err)
	}

	if _, err := os.Stat(cfg.Snapshot.OutputSnapshotFile); err == nil || !os.IsNotExist(err) {
		log.Panicf("output snapshot file '%s' already exists", cfg.Snapshot.OutputSnapshotFile)
	}

	println("loading protocol parameters ...")

	// parse protocol parameters from config into struct
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
	protoParamsBytes, err := protoParams.Serialize(serializer.DeSeriModeNoValidation, nil)
	if err != nil {
		log.Panicf("failed to serialize protocol parameters: %s", err)
	}
	cfg.ParsedProtocolParameters = &protoParams

	// compute storage deposit for minimal basic outputs (+timelocked)
	r := cfg.ParsedProtocolParameters.RentStructure
	cfg.MinCostPerTimelockedBasicOutput = r.MinRent(refTimelockedBasicOutput)
	cfg.MinCostPerNonTimelockedBasicOutput = r.MinRent(refNonTimelockedbasicOutput)

	// parse Chrysalis snapshot
	chrysalisSnapshot := readChrysalisSnapshot(err, cfg)
	beautifiedChrysalisStats, err := json.MarshalIndent(chrysalisSnapshot.Metadata, "", " ")
	if err != nil {
		log.Panicf("unable to serialize Chrysalis snapshot stats: %s", err)
	}

	log.Printf("read in Chrysalis snapshot %s:", cfg.Snapshot.ChrysalisSnapshotFile)
	log.Printf(string(beautifiedChrysalisStats))

	log.Println("converting Chrysalis outputs to Stardust outputs:")
	stardustOutputIDs, stardustOutputs := chrysalisSnapshot.StardustOutputs()
	log.Printf("outputs count after conversion to Stardust outputs: %d (from %d previously)", len(stardustOutputs), chrysalisSnapshot.Metadata.TotalOutputsCount)
	log.Printf("converted outputs + IDs hash: %s / %s", outputsHash(stardustOutputs), outputIDsHash(stardustOutputIDs))

	log.Printf("generating outputs for supply increase with starting date %s", cfg.Vesting.StartingDate)
	supplyIncreaseOutputIDs, supplyIncreaseOutputs := generateNewSupplyOutputs(cfg)
	log.Printf("supply increase outputs + IDs hash: %s / %s", outputsHash(supplyIncreaseOutputs), outputIDsHash(supplyIncreaseOutputIDs))
	log.Printf("generated %d outputs to accomodate the supply increase", len(supplyIncreaseOutputs))

	var csvImportOutputIDs []iotago3.OutputID
	var csvImportOutputs []iotago3.Output
	if cfg.CSV.Import.Active {
		log.Printf("generating outputs from %s", cfg.CSV.Import.LedgerFile)
		csvImportOutputIDs, csvImportOutputs = generateCSVOutputs(cfg)
		log.Printf("CSV import outputs + IDs hash: %s / %s", outputsHash(csvImportOutputs), outputIDsHash(csvImportOutputIDs))
		log.Printf("generated %d outputs from CSV import file", len(csvImportOutputs))
	}

	if cfg.Snapshot.SkipSnapshotGeneration {
		log.Println("finished (skipped snapshot generation)")
		return
	}

	// check if the infos from the chrysalis snapshot fit the config
	if uint32(cfg.Snapshot.GenesisMilestoneIndex) != chrysalisSnapshot.Header.LedgerMilestoneIndex {
		log.Panicf("the given GenesisMilestoneIndex (%d) in the config does not match the LedgerMilestoneIndex (%d) in the snapshot", cfg.Snapshot.GenesisMilestoneIndex, chrysalisSnapshot.Header.LedgerMilestoneIndex)
	}
	if uint32(cfg.Snapshot.TargetMilestoneIndex) != chrysalisSnapshot.Header.LedgerMilestoneIndex {
		log.Panicf("the given TargetMilestoneIndex (%d) in the config does not match the LedgerMilestoneIndex (%d) in the snapshot", cfg.Snapshot.TargetMilestoneIndex, chrysalisSnapshot.Header.LedgerMilestoneIndex)
	}
	if uint32(cfg.Snapshot.LedgerMilestoneIndex) != chrysalisSnapshot.Header.LedgerMilestoneIndex {
		log.Panicf("the given LedgerMilestoneIndex (%d) in the config does not match the LedgerMilestoneIndex (%d) in the snapshot", cfg.Snapshot.LedgerMilestoneIndex, chrysalisSnapshot.Header.LedgerMilestoneIndex)
	}
	if uint64(cfg.Snapshot.TargetMilestoneTimestamp) != chrysalisSnapshot.Header.Timestamp {
		log.Panicf("the given TargetMilestoneTimestamp (%d) in the config does not match the Timestamp (%d) in the snapshot", cfg.Snapshot.TargetMilestoneTimestamp, chrysalisSnapshot.Header.Timestamp)
	}

	genesisMilestoneIndex := chrysalisSnapshot.Header.LedgerMilestoneIndex
	genesisMilestoneTimestamp := uint32(chrysalisSnapshot.Header.Timestamp)

	// create snapshot file
	fullHeader := &stardust.FullSnapshotHeader{
		Version:                  stardust.SupportedFormatVersion,
		Type:                     stardust.Full,
		GenesisMilestoneIndex:    genesisMilestoneIndex,
		TargetMilestoneIndex:     genesisMilestoneIndex,
		TargetMilestoneTimestamp: genesisMilestoneTimestamp,
		TargetMilestoneID: func() iotago3.MilestoneID {
			// TargetMilestoneID doesn't matter for the new genesis snapshot.
			// It is not used in the consensus, only to check if a delta snapshot can be applied.
			return iotago3.MilestoneID{}
		}(),
		LedgerMilestoneIndex: genesisMilestoneIndex,
		TreasuryOutput: &stardust.TreasuryOutput{
			MilestoneID: chrysalisSnapshot.Header.TreasuryOutput.MilestoneID,
			Amount:      treasuryTokens,
		},
		ProtocolParamsMilestoneOpt: &iotago3.ProtocolParamsMilestoneOpt{
			TargetMilestoneIndex: chrysalisSnapshot.Header.LedgerMilestoneIndex,
			ProtocolVersion:      cfg.ProtocolParameters.Version,
			Params:               protoParamsBytes,
		},
		// will be overwritten in StreamFullSnapshotDataTo
		OutputCount:        0,
		MilestoneDiffCount: 0,
		SEPCount:           0,
	}

	// solid entry points
	// add "solidEntryPointBlockID" as sole entry point
	var solidEntryPointBlockID iotago3.BlockID
	copy(solidEntryPointBlockID[:], chrysalisSnapshot.SolidEntryPointMessageID)

	entryPointAdded := false
	solidEntryPointProducerFunc := func() (iotago3.BlockID, error) {
		if entryPointAdded {
			return solidEntryPointBlockID, stardust.ErrNoMoreSEPToProduce
		}
		entryPointAdded = true

		return solidEntryPointBlockID, nil
	}

	// unspent transaction outputs
	var stardustOutputsIndex, supplyIncreaseOutputsIndex, csvImportOutputsIndex int
	var nonTreasuryOutputsSupplyTotal uint64
	outputProducerFunc := func() (*stardust.Output, error) {

		if stardustOutputsIndex < len(stardustOutputs) {
			output := stardust.CreateOutput(
				stardustOutputIDs[stardustOutputsIndex],
				iotago3.EmptyBlockID(),
				genesisMilestoneIndex,
				genesisMilestoneTimestamp,
				stardustOutputs[stardustOutputsIndex])
			nonTreasuryOutputsSupplyTotal += output.Deposit()
			stardustOutputsIndex++
			return output, nil
		}

		if supplyIncreaseOutputsIndex < len(supplyIncreaseOutputs) {
			output := stardust.CreateOutput(
				supplyIncreaseOutputIDs[supplyIncreaseOutputsIndex],
				iotago3.EmptyBlockID(),
				genesisMilestoneIndex,
				genesisMilestoneTimestamp,
				supplyIncreaseOutputs[supplyIncreaseOutputsIndex])
			nonTreasuryOutputsSupplyTotal += output.Deposit()
			supplyIncreaseOutputsIndex++
			return output, nil
		}

		if csvImportOutputsIndex < len(csvImportOutputs) {
			output := stardust.CreateOutput(
				csvImportOutputIDs[csvImportOutputsIndex],
				iotago3.EmptyBlockID(),
				genesisMilestoneIndex,
				genesisMilestoneTimestamp,
				csvImportOutputs[csvImportOutputsIndex])
			nonTreasuryOutputsSupplyTotal += output.Deposit()
			csvImportOutputsIndex++
			return output, nil
		}

		// all outputs added
		return nil, nil
	}

	// milestone diffs
	milestoneDiffProducerFunc := func() (*stardust.MilestoneDiff, error) {
		// no milestone diffs needed
		return nil, nil
	}

	// build temp file path
	outputFilePathTmp := cfg.Snapshot.OutputSnapshotFile + "_tmp"

	// we don't need to check the error, maybe the file doesn't exist
	_ = os.Remove(outputFilePathTmp)

	fileHandle, err := os.OpenFile(outputFilePathTmp, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Panicf("unable to create snapshot file: %s", err)
	}

	log.Println("writing merge snapshot...")
	if err := stardust.StreamFullSnapshotDataTo(
		fileHandle,
		fullHeader,
		outputProducerFunc,
		milestoneDiffProducerFunc,
		solidEntryPointProducerFunc,
	); err != nil {
		_ = fileHandle.Close()
		log.Panicf("couldn't generate snapshot file: %s", err)
	}

	if err := fileHandle.Close(); err != nil {
		log.Panicf("unable to close snapshot file: %s", err)
	}

	if cfg.ValidateSupply {
		if protoParams.TokenSupply != nonTreasuryOutputsSupplyTotal+treasuryTokens {
			log.Panicf("supply defined in protocol parameters does not match supply within generated snapshot! %d vs. %d", protoParams.TokenSupply, nonTreasuryOutputsSupplyTotal+treasuryTokens)
		}
	}

	// rename tmp file to final file name
	if err := os.Rename(outputFilePathTmp, cfg.Snapshot.OutputSnapshotFile); err != nil {
		log.Panicf("unable to rename temp snapshot file: %s", err)
	}

	log.Println("computing blake2b-256 hash of snapshot file...")
	hash, err := blake2b.New256(nil)
	if err != nil {
		log.Panicf("unable to hash function: %s", err)
	}
	snapshotFile, err := os.Open(cfg.Snapshot.OutputSnapshotFile)
	if err != nil {
		log.Panicf("unable to read snapshot file for hash computation: %s", err)
	}
	defer func(snapshotFile *os.File) {
		if err := snapshotFile.Close(); err != nil {
			log.Panicf("unable to close generated snapshot file: %s", err)
		}
	}(snapshotFile)

	if _, err := io.Copy(hash, snapshotFile); err != nil {
		log.Panicf("unable to read snapshot file into hash function: %s", err)
	}

	log.Printf("snapshot creation successful! blake2b-256 hash %s", hex.EncodeToString(hash.Sum(nil)))
	log.Printf("supply in ledger outputs %d, treasury %d, total %d", nonTreasuryOutputsSupplyTotal, treasuryTokens, nonTreasuryOutputsSupplyTotal+treasuryTokens)
	log.Printf("total outputs written to snapshot: %d", len(stardustOutputs)+len(supplyIncreaseOutputs)+len(csvImportOutputs))
}

func generateCSVOutputs(cfg *Config) ([]iotago3.OutputID, []iotago3.Output) {
	csvImportFile, err := os.Open(cfg.CSV.Import.LedgerFile)
	if err != nil {
		log.Panicf("unable to open CSV import file: %s", err)
	}
	defer func(csvImportFile *os.File) {
		if err := csvImportFile.Close(); err != nil {
			log.Panicf("unable to close CSV import file: %s", err)
		}
	}(csvImportFile)

	var currentOutputIndex uint32
	outputMarker := blake2b.Sum256([]byte(cfg.CSV.Import.OutputMarker))
	log.Printf("using marker '%s' to mark CSV import outputs", iotago3.EncodeHex(outputMarker[:len(outputMarker)-4]))

	rows, err := csv.NewReader(csvImportFile).ReadAll()
	if err != nil {
		log.Panicf("unable to read rows from import CSV file: %s", err)
	}

	outputIDs := make([]iotago3.OutputID, 0)
	outputs := make([]iotago3.Output, 0)

	for i, row := range rows {
		hexAddrStr, balanceStr := row[0], row[1]
		edAddr := iotago3.Ed25519Address{}
		addrBytes, err := iotago3.DecodeHex(hexAddrStr)
		if err != nil {
			log.Panicf("unable to convert CSV address at row %d: %s", i+1, err)
		}
		copy(edAddr[:], addrBytes)
		outputs = append(outputs, &iotago3.BasicOutput{
			Amount: mustParseUint64(balanceStr),
			Conditions: iotago3.UnlockConditions{
				&iotago3.AddressUnlockCondition{Address: &edAddr},
			},
		})
		outputIDs = append(outputIDs, newOutputIDFromMarker(outputMarker[:], &currentOutputIndex))
	}

	return outputIDs, outputs
}

func outputIDsHash(outputIDs []iotago3.OutputID) string {
	h, _ := blake2b.New256(nil)
	for _, outputID := range outputIDs {
		id := outputID.UTXOInput().ID()
		h.Write(id[:])
	}
	return iotago3.EncodeHex(h.Sum(nil))
}

func outputsHash(outputs []iotago3.Output) string {
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

// generates all the outputs holding the new supply
func generateNewSupplyOutputs(cfg *Config) ([]iotago3.OutputID, []iotago3.Output) {
	allOutputIDs := make([]iotago3.OutputID, 0)
	allOutputs := make([]iotago3.Output, 0)

	var currentOutputIndex uint32
	outputMarker := blake2b.Sum256([]byte(cfg.Vesting.OutputMarker))
	log.Printf("using marker '%s' to mark supply increase outputs", iotago3.EncodeHex(outputMarker[:len(outputMarker)-4]))

	for _, alloc := range cfg.Vesting.Allocations {
		switch {
		case alloc.Rewards != nil:
			asmbTokensTotal, asmbTokensPerAddr := readAssemblyRewardFiles(cfg, alloc)
			alloc.Addresses = convertAssemblyToIOTA(mustParseUint64(alloc.Rewards.Tokens), asmbTokensTotal, asmbTokensPerAddr)
			log.Printf("there are %d addresses for '%s' receiving Assembly rewards", len(alloc.Addresses), alloc.Name)

		case alloc.Distribution != nil:
			asmbTokensTotal, asmbTokensPerAddr := readAssemblyDistributionFile(cfg, alloc)
			alloc.Addresses = convertAssemblyToIOTA(mustParseUint64(alloc.Distribution.Tokens), asmbTokensTotal, asmbTokensPerAddr)
			log.Printf("there are %d addresses for '%s' Assembly distribution", len(alloc.Addresses), alloc.Name)
		}
		outputIDs, outputs := generateOutputsForGroup(alloc, cfg, outputMarker[:], &currentOutputIndex)
		allOutputIDs = append(allOutputIDs, outputIDs...)
		allOutputs = append(allOutputs, outputs...)
	}

	return allOutputIDs, allOutputs
}

func mustParseUint64(str string) uint64 {
	num, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		log.Panicf("unable to parse uint64: %s", err)
	}
	return num
}

func generateOutputsForGroup(alloc Allocation, cfg *Config, supplyIncreaseMarker []byte,
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

		fileName := fmt.Sprintf("%s-%s-%s.csv", addrTuple.Tokens, addrTuple.Name, addrTuple.Address)
		if len(addrTuple.Name) == 0 {
			fileName = fmt.Sprintf("%s-%s.csv", addrTuple.Tokens, addrTuple.Address)
		}
		writeOutputsCSV(cfg, newOutputIDs, newOutputs, path.Join(targetDir, fileName))
	}
	writeSummaryCSV(cfg, unlockAccumBalance, path.Join(targetDir, "summary.csv"))
	writeAddrListCSV(cfg, alloc.Addresses, path.Join(targetDir, "address_balances.csv"))
	log.Printf("generated %d outputs, placed CSVs in %s, outputs + IDs hashes %s/%s", len(outputs), targetDir, outputsHash(outputs), outputIDsHash(outputIDs))
	return outputIDs, outputs
}

func writeAddrListCSV(cfg *Config, addrList []AddrBalanceTuple, fileName string) {
	if !cfg.CSV.Export.Active {
		return
	}
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		log.Panicf("unable to write csv %s: %s", fileName, err)
	}
	defer func(f *os.File) {
		if err := f.Close(); err != nil {
			log.Panicf("unable to close file %s", f.Name())
		}
	}(f)

	csvWriter := csv.NewWriter(f)
	defer csvWriter.Flush()

	if err := csvWriter.Write([]string{"Address", "Tokens"}); err != nil {
		log.Panicf("unable to write out CSV header: %s", err)
	}

	for _, addr := range addrList {
		if err := csvWriter.Write([]string{addr.Address, addr.Tokens}); err != nil {
			log.Panicf("unable to write out CSV record: %s", err)
		}
	}
}

func writeSummaryCSV(cfg *Config, timelocksAndFunds map[uint32]uint64, fileName string) {
	if !cfg.CSV.Export.Active {
		return
	}
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		log.Panicf("unable to write csv %s: %s", fileName, err)
	}
	defer func(f *os.File) {
		if err := f.Close(); err != nil {
			log.Panicf("unable to close file %s", f.Name())
		}
	}(f)

	csvWriter := csv.NewWriter(f)
	defer csvWriter.Flush()

	if err := csvWriter.Write([]string{"Tokens", "Unlock Date"}); err != nil {
		log.Panicf("unable to write out CSV header: %s", err)
	}

	type row struct {
		timelock uint32
		tokens   uint64
	}

	var rows []row
	for k, v := range timelocksAndFunds {
		rows = append(rows, row{k, v})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].timelock < rows[j].timelock
	})

	for _, row := range rows {
		unlockDate := time.Unix(int64(row.timelock), 0).UTC().String()
		if err := csvWriter.Write([]string{strconv.FormatUint(row.tokens, 10), unlockDate}); err != nil {
			log.Panicf("unable to write out CSV record: %s", err)
		}
	}
}

func writeOutputsCSV(cfg *Config, outputIDs []iotago3.OutputID, outputs []iotago3.Output, fileName string) {
	if !cfg.CSV.Export.Active {
		return
	}
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		log.Panicf("unable to write csv %s: %s", fileName, err)
	}
	defer func(f *os.File) {
		if err := f.Close(); err != nil {
			log.Panicf("unable to close file %s", f.Name())
		}
	}(f)

	csvWriter := csv.NewWriter(f)
	defer csvWriter.Flush()

	if err := csvWriter.Write([]string{"OutputID Hex", "Tokens", "Unlock Date"}); err != nil {
		log.Panicf("unable to write out CSV header: %s", err)
	}

	for i := 0; i < len(outputs); i++ {
		outputID, output := outputIDs[i], outputs[i].(*iotago3.BasicOutput)
		var unlockDate string
		if timelock := output.UnlockConditionSet().Timelock(); timelock != nil {
			unlockDate = time.Unix(int64(timelock.UnixTime), 0).String()
		}
		if err := csvWriter.Write([]string{iotago3.EncodeHex(outputID[:]), strconv.FormatUint(output.Deposit(), 10), unlockDate}); err != nil {
			log.Panicf("unable to write out CSV record: %s", err)
		}
	}
}

func readAssemblyRewardFiles(cfg *Config, alloc Allocation) (uint64, map[string]uint64) {
	files, err := os.ReadDir(alloc.Rewards.Dir)
	if err != nil {
		log.Panicf("unable to read Assembly rewards directory: %s", err)
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
			log.Panicf("unable to open Assembly rewards file %s: %s", f.Name(), err)
		}

		for k, v := range r.Rewards {
			var addr iotago3.Ed25519Address
			addrBytes, err := hex.DecodeString(k)
			if err != nil {
				log.Panicf("unable to decode hex encoded address '%s' in Assembly reward file: %s", k, err)
			}
			copy(addr[:], addrBytes)

			bech32AddrStr := addr.Bech32(iotago3.NetworkPrefix(cfg.ProtocolParameters.Bech32HRP))
			totalRewards += v
			accumulatedRewardsPerAddress[bech32AddrStr] += v
		}
	}
	log.Printf("total Assembly rewards %d on %d addresses", totalRewards, len(accumulatedRewardsPerAddress))

	return totalRewards, accumulatedRewardsPerAddress
}

func readAssemblyDistributionFile(cfg *Config, alloc Allocation) (uint64, map[string]uint64) {
	asmbDistroData, err := os.ReadFile(alloc.Distribution.File)
	if err != nil {
		log.Panicf("unable to read Assembly distribution file: %s", err)
	}

	type tuple struct {
		Base58Addr string `json:"address"`
		Balance    uint64 `json:"balance"`
	}

	var tuples []tuple
	if err := json.Unmarshal(asmbDistroData, &tuples); err != nil {
		log.Panicf("unable to parse Assembly distribution file: %s", err)
	}

	var totalRewards uint64
	asmbTokensPerAddr := make(map[string]uint64)
	for _, tuple := range tuples {
		var addr iotago3.Ed25519Address

		if _, excluded := alloc.Distribution.Exclude[tuple.Base58Addr]; excluded {
			log.Printf("skipping Assembly distribution address '%s' with %d balance", tuple.Base58Addr, tuple.Balance)
			continue
		}

		assemblyAddressBytes, err := base58.Decode(tuple.Base58Addr)
		if err != nil {
			log.Panicf("unable to decode base58 Assembly address '%s': %s", tuple.Base58Addr, err)
		}
		// is prefixed with zero byte to indicate address type in distribution file
		copy(addr[:], assemblyAddressBytes[1:])

		bech32AddrStr := addr.Bech32(iotago3.NetworkPrefix(cfg.ProtocolParameters.Bech32HRP))
		totalRewards += tuple.Balance
		asmbTokensPerAddr[bech32AddrStr] += tuple.Balance
	}
	log.Printf("total Assembly tokens %d on %d addresses", totalRewards, len(asmbTokensPerAddr))

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
		parsedAddr, err := parseBech32Address(addr)
		if err != nil {
			log.Panicf("unable to parse address %s: %s", addr, err)
		}

		iotaRewardsFloat64 := float64(iotaTokensToDistribute) * (float64(assemblyTokens) / float64(asmbTokensTotal))
		iotaRewards := uint64(iotaRewardsFloat64)
		remainder -= iotaRewards
		tuples = append(tuples, balancetuple{
			address:           parsedAddr,
			assemblyRewards:   assemblyTokens,
			iotaRewards:       iotaRewards,
			divisionRemainder: iotaRewardsFloat64 - float64(iotaRewards),
		})
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
			// the missing remainder per address can only be between 0 and 1 IOTA,
			// so we fill the missing IOTA to the addresses with highest remainder as long as IOTA are left.
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

func generateVestingOutputs(
	cfg *Config, target iotago3.Address, alloc Allocation,
	vestedTokens uint64, supplyIncreaseMarker []byte, outputIndex *uint32,
) ([]iotago3.OutputID, []iotago3.Output, []*iotago3.TimelockUnlockCondition) {

	// no vesting due to 100% initial unlock
	if alloc.Unlocks.InitialUnlock == 1 {
		outputIDs := []iotago3.OutputID{newOutputIDFromMarker(supplyIncreaseMarker, outputIndex)}
		outputs := []iotago3.Output{
			&iotago3.BasicOutput{
				Amount:     vestedTokens,
				Conditions: iotago3.UnlockConditions{&iotago3.AddressUnlockCondition{Address: target}},
			},
		}
		return outputIDs, outputs, make([]*iotago3.TimelockUnlockCondition, 0)
	}

	var controlSum uint64
	investorTimelocks := vestingTimelocks(alloc, cfg.Vesting.StartingDate)
	initialUnlock := uint64(float64(vestedTokens) * alloc.Unlocks.InitialUnlock)

	// add the remainer of the funds that wouldn't fit into the fundsPerUnlock into the initialUnlock (to avoid fractions)
	initialUnlock += (vestedTokens - initialUnlock) % uint64(len(investorTimelocks))
	fundsPerUnlock := (vestedTokens - initialUnlock) / uint64(len(investorTimelocks))

	// if fundsPerUnlock is 0, then we only have the initialUnlock
	if fundsPerUnlock == 0 {
		investorTimelocks = make([]*iotago3.TimelockUnlockCondition, 0)
	}

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

func newOutputIDFromMarker(supplyIncreaseMarker []byte, outputIndex *uint32) iotago3.OutputID {
	outputID := iotago3.OutputID{}
	// 28 bytes marker, 4 bytes for index, 2 bytes for outputIndex
	copy(outputID[:], supplyIncreaseMarker[:len(supplyIncreaseMarker)-4])
	binary.LittleEndian.PutUint32(outputID[len(outputID)-6:], *outputIndex)
	binary.LittleEndian.PutUint16(outputID[len(outputID)-2:], 0)
	*outputIndex += 1
	return outputID
}

func readChrysalisSnapshot(err error, cfg *Config) *ChrysalisSnapshot {
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
			chrysalisSnapshot.Metadata.TotalOutputsCount++

			switch {
			case output.OutputType == iotago2.OutputSigLockedDustAllowanceOutput:
				dustAllowanceOutputsSlice, has := chrysalisSnapshot.DustAllowanceOutputs[key]
				if !has {
					dustAllowanceOutputsSlice = make(ChrysalisOutputs, 0)
				}
				dustAllowanceOutputsSlice = append(dustAllowanceOutputsSlice, output)
				chrysalisSnapshot.DustAllowanceOutputs[key] = dustAllowanceOutputsSlice
				chrysalisSnapshot.Metadata.DustAllowanceOutputsCount++

			case isDustOutput(output):
				dustOutputsSlice, has := chrysalisSnapshot.DustOutputs[key]
				if !has {
					dustOutputsSlice = make(ChrysalisOutputs, 0)
				}
				dustOutputsSlice = append(dustOutputsSlice, output)
				chrysalisSnapshot.DustOutputs[key] = dustOutputsSlice
				chrysalisSnapshot.Metadata.DustOutputsCount++
				chrysalisSnapshot.Metadata.TotalDustBalance += output.Amount

			default:
				chrysalisSnapshot.Outputs = append(chrysalisSnapshot.Outputs, output)
			}

			chrysalisSnapshot.Metadata.TotalBalanceSumOutputs += output.Amount
			return nil
		},
		// treasury
		func(output *chrysalis.TreasuryOutput) error {
			chrysalisSnapshot.Metadata.TotalOutputsCount++
			chrysalisSnapshot.TreasuryOutput = output
			chrysalisSnapshot.Metadata.Treasury = output.Amount
			return nil
		},
		// milestone diffs
		func(milestoneDiff *chrysalis.MilestoneDiff) error {
			log.Panic("the snapshot should not contain a milestone diff")
			return nil
		},
	); err != nil {
		log.Panicf("unable to read in chrysalis snapshot data: %s", err)
	}

	chrysalisSnapshot.Metadata.TotalBalance = chrysalisSnapshot.Metadata.TotalBalanceSumOutputs + chrysalisSnapshot.Metadata.Treasury

	if chrysalisSnapshot.Header.Type != chrysalis.Full {
		log.Panic("the snapshot should be a FULL snapshot")
	}

	if chrysalisSnapshot.SolidEntryPointMessageID == nil {
		log.Panic("the snapshot should contain a SEP")
	}

	if chrysalisSnapshot.Header.LedgerMilestoneIndex != chrysalisSnapshot.Header.SEPMilestoneIndex {
		log.Panic("the snapshot must be a global snapshot")
	}

	if chrysalisSnapshot.Header.TreasuryOutput == nil {
		log.Panic("the snapshot must contain a treasury output")
	}

	return chrysalisSnapshot
}

func isDustOutput(output *chrysalis.Output) bool {
	return output.Amount < 1000000
}
