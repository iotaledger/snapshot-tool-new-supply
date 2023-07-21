package main

import (
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/blake2b"

	"github.com/iotaledger/hive.go/core/ioutils"
	"github.com/iotaledger/hive.go/serializer/v2"
	"github.com/iotaledger/hornet/pkg/model/hornet"
	iotago2 "github.com/iotaledger/iota.go/v2"

	chrysalisutxo "github.com/iotaledger/hornet/pkg/model/utxo"
	chrysalissnapshot "github.com/iotaledger/hornet/pkg/snapshot"

	// don't move this import, it fixes the issue of the multiple hornet versions
	// declaring the same command line flags and thus crashing the program up on startup
	_ "github.com/iotaledger/the-mergerator/pkg"

	"github.com/iotaledger/hornet/v2/pkg/model/utxo"
	"github.com/iotaledger/hornet/v2/pkg/snapshot"
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
	TreasuryTokens             string `json:"treasuryTokens"`
	ChrysalisSnapshotFile      string `json:"chrysalisSnapshotFile"`
	OutputSnapshotFile         string `json:"outputSnapshotFile"`
	NewSupplyOutputMarkerInput string `json:"newSupplyOutputMarkerInput"`
	CSVExportFolder            string `json:"csvExportFolder"`
	DoCSVExport                bool   `json:"doCSVExport"`
	CheckSupplyOutcome         bool   `json:"checkSupplyOutcome"`
	Allocations                struct {
		VestingStartingDate time.Time `json:"vestingStartingDate"`
		Assembly            struct {
			Investors InvestorAllocations `json:"investors"`
			Stakers   struct {
				CSVFolderName string            `json:"csvFolderName"`
				Tokens        string            `json:"tokens"`
				RewardsDir    string            `json:"rewardsDir"`
				Unlocks       FundsUnlockParams `json:"unlocks"`
			} `json:"stakers"`
		} `json:"assembly"`
		NewInvestors  InvestorAllocations `json:"newInvestors"`
		EcosystemFund struct {
			CSVFolderName string            `json:"csvFolderName"`
			Tokens        string            `json:"tokens"`
			Address       string            `json:"address"`
			Unlocks       FundsUnlockParams `json:"unlocks"`
		} `json:"ecosystemFund"`
		IOTAFoundation struct {
			CSVFolderName string            `json:"csvFolderName"`
			Tokens        string            `json:"tokens"`
			Address       string            `json:"address"`
			Unlocks       FundsUnlockParams `json:"unlocks"`
		} `json:"iotaFoundation"`
	} `json:"allocations"`
}

type FundsUnlockParams struct {
	Frequency          string  `json:"frequency"`
	InitialUnlock      float64 `json:"initialUnlock"`
	VestingPeriodYears int     `json:"vestingPeriodYears"`
}

type AddrBalanceTuple struct {
	Name    string `json:"name"`
	Address string `json:"address"`
	Tokens  string `json:"tokens"`
}

var (
	dayDuration     = time.Duration(86400000000000)
	weeklyDuration  = dayDuration * 7
	monthlyDuration = dayDuration * 30
)

type InvestorAllocations struct {
	CSVFolderName string             `json:"csvFolderName"`
	Unlocks       FundsUnlockParams  `json:"unlocks"`
	Addresses     []AddrBalanceTuple `json:"addresses"`
}

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
func vestingTimelocks(interval time.Duration, startDate time.Time, years int) []*iotago3.TimelockUnlockCondition {
	unlocks := make([]*iotago3.TimelockUnlockCondition, 0)
	end := startDate.AddDate(years, 0, 0)
	for date := startDate; date.Before(end); date = date.Add(interval) {
		unlocks = append(unlocks, &iotago3.TimelockUnlockCondition{UnixTime: uint32(date.Unix())})
	}
	return unlocks
}

func parseAddress(bech32Address string) (iotago3.Address, error) {
	_, address, err := iotago3.ParseBech32(bech32Address)
	if err != nil {
		bech32Address = strings.TrimPrefix(bech32Address, "0x")

		if len(bech32Address) != iotago3.Ed25519AddressBytesLength*2 {
			return nil, err
		}

		// try parsing as hex
		address, err = iotago3.ParseEd25519AddressFromHexString("0x" + bech32Address)
		if err != nil {
			return nil, err
		}
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

type ChrysalisOutputs []*chrysalissnapshot.Output

func (outputs ChrysalisOutputs) ConvertToStardust() ([]iotago3.OutputID, []iotago3.Output) {
	var stardustOutputs []iotago3.Output
	var outputIDs []iotago3.OutputID
	for _, output := range outputs {
		stardustOutputs = append(stardustOutputs, convertChrysalisToStardust(output))
		outputIDs = append(outputIDs, output.OutputID)
	}
	return outputIDs, stardustOutputs
}

type ChrysalisSnapshot struct {
	// Header of the snapshot
	Header *chrysalissnapshot.ReadFileHeader
	// All ledger outputs which are not dust allowance, dust or treasury outputs
	Outputs ChrysalisOutputs
	// All dust allowance dust outputs mapped by their address
	DustAllowanceOutputs map[string]ChrysalisOutputs
	// All dust outputs mapped by their address
	DustOutputs map[string]ChrysalisOutputs
	// Treasury output
	TreasuryOutput *chrysalisutxo.TreasuryOutput
	// Stats of the snapshot
	Stats ChrysalisSnapshotStats
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
			sort.Sort(chrysalissnapshot.LexicalOrderedOutputs(tuple.dustAllowanceOutputs))

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

func convertChrysalisToStardust(output *chrysalissnapshot.Output) iotago3.Output {
	addr := iotago3.Ed25519Address{}
	copy(addr[:], output.Address.(*iotago2.Ed25519Address)[:])
	addrUnlock := &iotago3.AddressUnlockCondition{Address: &addr}
	return &iotago3.BasicOutput{Amount: output.Amount, Conditions: iotago3.UnlockConditions{addrUnlock}}
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

func main() {
	args := os.Args[1:]
	if len(args) == 1 {
		os.Exit(1)
	}

	cfg := &Config{}
	if err := ioutils.ReadJSONFromFile("./config.json", cfg); err != nil {
		log.Panicf("unable to read config file: %s", err)
	}

	treasuryTokens := mustParseUint64(cfg.TreasuryTokens)

	if _, err := os.Stat(cfg.ChrysalisSnapshotFile); err != nil || os.IsNotExist(err) {
		log.Panicf("chrysalis snapshot file missing: %s", err)
	}

	if _, err := os.Stat(cfg.OutputSnapshotFile); err == nil || !os.IsNotExist(err) {
		log.Panicf("output snapshot file '%s' already exists", cfg.OutputSnapshotFile)
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
	protoParamsBytes, err := protoParams.Serialize(serializer.DeSeriModeNoValidation, nil)
	if err != nil {
		log.Panicf("failed to serialize protocol parameters: %s", err)
	}

	chrysalisSnapshot := readChrysalisSnapshot(err, cfg)
	beautifiedChrysalisStats, err := json.MarshalIndent(chrysalisSnapshot.Stats, "", " ")
	if err != nil {
		log.Panicf("unable to serialize chrysalis snapshot stats: %s", err)
	}

	log.Printf("read in chrysalis snapshot %s:", cfg.ChrysalisSnapshotFile)
	log.Printf(string(beautifiedChrysalisStats))

	log.Println("converting to stardust ledger outputs:")
	stardustOutputIDs, stardustOutputs := chrysalisSnapshot.StardustOutputs()
	log.Printf("outputs count under Stardust %d (from %d previously)", len(stardustOutputs), chrysalisSnapshot.Stats.TotalOutputsCount)
	log.Printf("converted output + ids hash: %s / %s", outputsHash(stardustOutputs), outputIDsHash(stardustOutputIDs))

	log.Printf("generating outputs for supply increase with starting date %s", cfg.Allocations.VestingStartingDate)
	supplyIncreaseOutputIDs, supplyIncreaseOutputs := generateNewSupplyOutputs(cfg)
	log.Printf("supply increase outputs + ids hash: %s / %s", outputsHash(supplyIncreaseOutputs), outputIDsHash(supplyIncreaseOutputIDs))
	log.Printf("generated %d outputs for supply increase", len(supplyIncreaseOutputs))

	// create snapshot file
	var targetIndex iotago3.MilestoneIndex
	fullHeader := &snapshot.FullSnapshotHeader{
		Version:                  snapshot.SupportedFormatVersion,
		Type:                     snapshot.Full,
		GenesisMilestoneIndex:    0,
		TargetMilestoneIndex:     iotago3.MilestoneIndex(chrysalisSnapshot.Header.SEPMilestoneIndex),
		TargetMilestoneTimestamp: uint32(chrysalisSnapshot.Header.Timestamp),
		TargetMilestoneID:        iotago3.MilestoneID{},
		LedgerMilestoneIndex:     iotago3.MilestoneIndex(chrysalisSnapshot.Header.LedgerMilestoneIndex),
		TreasuryOutput: &utxo.TreasuryOutput{
			MilestoneID: chrysalisSnapshot.Header.TreasuryOutput.MilestoneID,
			Amount:      treasuryTokens,
		},
		ProtocolParamsMilestoneOpt: &iotago3.ProtocolParamsMilestoneOpt{
			TargetMilestoneIndex: targetIndex,
			ProtocolVersion:      cfg.ProtocolParameters.Version,
			Params:               protoParamsBytes,
		},
		OutputCount:        0,
		MilestoneDiffCount: 0,
		SEPCount:           0,
	}

	// solid entry points
	// add "EmptyBlockID" as sole entry point
	nullHashAdded := false
	solidEntryPointProducerFunc := func() (iotago3.BlockID, error) {
		if nullHashAdded {
			return iotago3.EmptyBlockID(), snapshot.ErrNoMoreSEPToProduce
		}
		nullHashAdded = true

		return iotago3.EmptyBlockID(), nil
	}

	// unspent transaction outputs
	var chrysalisLedgerIndex, supplyIncreaseOutputsIndex int
	var nonTreasuryOutputsSupplyTotal uint64
	outputProducerFunc := func() (*utxo.Output, error) {

		if chrysalisLedgerIndex < len(stardustOutputs) {
			output := utxo.CreateOutput(
				stardustOutputIDs[chrysalisLedgerIndex],
				iotago3.EmptyBlockID(),
				0,
				0,
				stardustOutputs[chrysalisLedgerIndex])
			nonTreasuryOutputsSupplyTotal += output.Deposit()
			chrysalisLedgerIndex++
			return output, nil
		}

		if supplyIncreaseOutputsIndex < len(supplyIncreaseOutputs) {
			output := utxo.CreateOutput(
				supplyIncreaseOutputIDs[supplyIncreaseOutputsIndex],
				iotago3.EmptyBlockID(),
				0,
				0,
				supplyIncreaseOutputs[supplyIncreaseOutputsIndex])
			nonTreasuryOutputsSupplyTotal += output.Deposit()
			supplyIncreaseOutputsIndex++
			return output, nil
		}

		// all outputs added
		return nil, nil
	}

	// milestone diffs
	milestoneDiffProducerFunc := func() (*snapshot.MilestoneDiff, error) {
		// no milestone diffs needed
		return nil, nil
	}

	// build temp file path
	outputFilePathTmp := cfg.OutputSnapshotFile + "_tmp"

	// we don't need to check the error, maybe the file doesn't exist
	_ = os.Remove(outputFilePathTmp)

	fileHandle, err := os.OpenFile(outputFilePathTmp, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Panicf("unable to create snapshot file: %s", err)
	}

	log.Println("writing merge snapshot...")
	if _, err := snapshot.StreamFullSnapshotDataTo(
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

	if cfg.CheckSupplyOutcome {
		if protoParams.TokenSupply != nonTreasuryOutputsSupplyTotal+treasuryTokens {
			log.Panicf("supply defined in protocol parameters does not match supply within generated snapshot! %d vs. %d", protoParams.TokenSupply, nonTreasuryOutputsSupplyTotal+treasuryTokens)
		}
	}

	// rename tmp file to final file name
	if err := os.Rename(outputFilePathTmp, cfg.OutputSnapshotFile); err != nil {
		log.Panicf("unable to rename temp snapshot file: %s", err)
	}

	log.Println("computing blake2b-256 hash of snapshot file...")
	hash, err := blake2b.New256(nil)
	if err != nil {
		log.Panicf("unable to hash function: %s", err)
	}
	snapshotFile, err := os.Open(cfg.OutputSnapshotFile)
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
	log.Printf("total outputs written to snapshot: %d", len(stardustOutputs)+len(supplyIncreaseOutputs))
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
	supplyIncreaseMarker := blake2b.Sum256([]byte(cfg.NewSupplyOutputMarkerInput))
	log.Printf("using marker '%s' to mark supply increase outputs", iotago3.EncodeHex(supplyIncreaseMarker[:]))

	// new investors
	outputIDs, outputs := generateOutputsForGroup(cfg.Allocations.NewInvestors, cfg, supplyIncreaseMarker[:], &currentOutputIndex)
	allOutputIDs = append(allOutputIDs, outputIDs...)
	allOutputs = append(allOutputs, outputs...)

	// assembly investors
	outputIDs, outputs = generateOutputsForGroup(cfg.Allocations.Assembly.Investors, cfg, supplyIncreaseMarker[:], &currentOutputIndex)
	allOutputIDs = append(allOutputIDs, outputIDs...)
	allOutputs = append(allOutputs, outputs...)

	// assembly stakers
	assemblyStakers := readAssemblyStakingRewards(cfg)
	log.Printf("there are %d unique assembly staker addresses", len(assemblyStakers))
	outputIDs, outputs = generateOutputsForGroup(InvestorAllocations{
		CSVFolderName: cfg.Allocations.Assembly.Stakers.CSVFolderName,
		Unlocks:       cfg.Allocations.Assembly.Stakers.Unlocks,
		Addresses:     assemblyStakers,
	}, cfg, supplyIncreaseMarker[:], &currentOutputIndex)
	allOutputIDs = append(allOutputIDs, outputIDs...)
	allOutputs = append(allOutputs, outputs...)

	// ecosystem fund
	outputIDs, outputs = generateOutputsForGroup(InvestorAllocations{
		CSVFolderName: cfg.Allocations.EcosystemFund.CSVFolderName,
		Unlocks:       cfg.Allocations.EcosystemFund.Unlocks,
		Addresses: []AddrBalanceTuple{{
			Name:    "",
			Address: cfg.Allocations.EcosystemFund.Address,
			Tokens:  cfg.Allocations.EcosystemFund.Tokens,
		}},
	}, cfg, supplyIncreaseMarker[:], &currentOutputIndex)
	allOutputIDs = append(allOutputIDs, outputIDs...)
	allOutputs = append(allOutputs, outputs...)

	// iota foundation
	outputIDs, outputs = generateOutputsForGroup(InvestorAllocations{
		CSVFolderName: cfg.Allocations.IOTAFoundation.CSVFolderName,
		Unlocks:       cfg.Allocations.IOTAFoundation.Unlocks,
		Addresses: []AddrBalanceTuple{{
			Name:    "",
			Address: cfg.Allocations.IOTAFoundation.Address,
			Tokens:  cfg.Allocations.IOTAFoundation.Tokens,
		}},
	}, cfg, supplyIncreaseMarker[:], &currentOutputIndex)
	allOutputIDs = append(allOutputIDs, outputIDs...)
	allOutputs = append(allOutputs, outputs...)

	return allOutputIDs, allOutputs
}

func mustParseUint64(str string) uint64 {
	num, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		log.Panicf("unable to parse uint64: %s", err)
	}
	return num
}

func generateOutputsForGroup(alloc InvestorAllocations, cfg *Config, supplyIncreaseMarker []byte,
	currentOutputIndex *uint32,
) ([]iotago3.OutputID, []iotago3.Output) {
	outputIDs := make([]iotago3.OutputID, 0)
	outputs := make([]iotago3.Output, 0)

	targetDir := path.Join(cfg.CSVExportFolder, alloc.CSVFolderName)
	_ = os.MkdirAll(targetDir, 0777)
	unlockAccumBalance := map[uint32]uint64{}
	for _, addrTuple := range alloc.Addresses {

		targetAddr, err := parseAddress(addrTuple.Address)
		if err != nil {
			log.Panicf("unable to parse address %s: %s", addrTuple.Address, err)
		}

		newOutputIDs, newOutputs, timelocks := generateVestingOutputs(
			targetAddr, alloc.Unlocks.Frequency, cfg.Allocations.VestingStartingDate,
			alloc.Unlocks.VestingPeriodYears, mustParseUint64(addrTuple.Tokens), alloc.Unlocks.InitialUnlock,
			supplyIncreaseMarker, currentOutputIndex,
		)

		unlockAccumBalance[0] += newOutputs[0].Deposit()
		for i, timelock := range timelocks {
			unlockAccumBalance[timelock.UnixTime] += newOutputs[i+1].Deposit()
		}

		outputIDs = append(outputIDs, newOutputIDs...)
		outputs = append(outputs, newOutputs...)
		writeOutputsCSV(cfg, newOutputIDs, newOutputs, path.Join(targetDir, fmt.Sprintf("%s-%s.csv", addrTuple.Name, addrTuple.Address)))
	}
	writeSummaryCSV(cfg, unlockAccumBalance, path.Join(targetDir, "summary.csv"))
	log.Printf("generated %d outputs, placed CSVs in %s, outputs+ids hashes %s/%s", len(outputs), targetDir, outputsHash(outputs), outputIDsHash(outputIDs))
	return outputIDs, outputs
}

func writeSummaryCSV(cfg *Config, timelocksAndFunds map[uint32]uint64, fileName string) {
	if !cfg.DoCSVExport {
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
		unlockDate := time.Unix(int64(row.timelock), 0).String()
		if err := csvWriter.Write([]string{strconv.FormatUint(row.tokens, 10), unlockDate}); err != nil {
			log.Panicf("unable to write out CSV record: %s", err)
		}
	}
}

func writeOutputsCSV(cfg *Config, outputIDs []iotago3.OutputID, outputs []iotago3.Output, fileName string) {
	if !cfg.DoCSVExport {
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

func readAssemblyStakingRewards(cfg *Config) []AddrBalanceTuple {
	rewardsDir := cfg.Allocations.Assembly.Stakers.RewardsDir
	files, err := os.ReadDir(rewardsDir)
	if err != nil {
		log.Panicf("unable to read assembly stakers directory: %s", err)
	}

	type rewards struct {
		Rewards map[string]uint64 `json:"rewards"`
	}

	var totalAssemblyRewards uint64
	accumulatedRewardsPerAddress := make(map[string]uint64)
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		r := &rewards{}
		if err := ioutils.ReadJSONFromFile(path.Join(rewardsDir, f.Name()), r); err != nil {
			log.Panicf("unable to open assembly rewards file %s: %s", f.Name(), err)
		}

		for k, v := range r.Rewards {
			totalAssemblyRewards += v
			accumulatedRewardsPerAddress[k] += v
		}
	}
	log.Printf("total assembly rewards %d on %d addresses", totalAssemblyRewards, len(accumulatedRewardsPerAddress))

	iotaTokensToDistribute := mustParseUint64(cfg.Allocations.Assembly.Stakers.Tokens)
	remainder := iotaTokensToDistribute

	type balancetuple struct {
		address           iotago3.Address
		assemblyRewards   uint64
		iotaRewards       uint64
		divisionRemainder float64
	}

	tuples := make([]balancetuple, 0)
	for addr, assemblyRewards := range accumulatedRewardsPerAddress {
		addr, err := parseAddress(addr)
		if err != nil {
			log.Panicf("unable to parse assembly staker address %s: %s", addr, err)
		}

		iotaRewardsFloat64 := float64(iotaTokensToDistribute) * (float64(assemblyRewards) / float64(totalAssemblyRewards))
		iotaRewards := uint64(iotaRewardsFloat64)
		remainder -= iotaRewards
		tuples = append(tuples, balancetuple{addr, assemblyRewards, iotaRewards, iotaRewardsFloat64 - float64(iotaRewards)})
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
		log.Panicf("total funds distributed to assembly stakers %d != %d", iotaRewardsControl, cfg.Allocations.Assembly.Stakers.Tokens)
	}

	// make address tuples ordering deterministic
	sort.Slice(addresses, func(i, j int) bool {
		return strings.Compare(addresses[i].Address, addresses[j].Address) < 0
	})

	return addresses
}

func generateVestingOutputs(
	target iotago3.Address, unlockFreq string, startDate time.Time,
	years int, vestedTokens uint64, initialUnlockPerc float64,
	supplyIncreaseMarker []byte, outputIndex *uint32,
) ([]iotago3.OutputID, []iotago3.Output, []*iotago3.TimelockUnlockCondition) {
	investorTimelocks := vestingTimelocks(vestingIntervalFromStr(unlockFreq), startDate, years)

	initialUnlock := uint64(float64(vestedTokens) * initialUnlockPerc)
	initialUnlock += (vestedTokens - initialUnlock) % uint64(len(investorTimelocks))
	fundsPerUnlock := (vestedTokens - initialUnlock) / uint64(len(investorTimelocks))

	// initial unlock output
	outputIDs := []iotago3.OutputID{newVestingOutputID(supplyIncreaseMarker, outputIndex)}
	outputs := []iotago3.Output{
		&iotago3.BasicOutput{
			Amount:     initialUnlock,
			Conditions: iotago3.UnlockConditions{&iotago3.AddressUnlockCondition{Address: target}},
		},
	}

	for i := 0; i < len(investorTimelocks); i++ {
		outputID := newVestingOutputID(supplyIncreaseMarker, outputIndex)
		outputIDs = append(outputIDs, outputID)
		outputs = append(outputs, &iotago3.BasicOutput{
			Amount: fundsPerUnlock,
			Conditions: iotago3.UnlockConditions{
				&iotago3.AddressUnlockCondition{Address: target}, investorTimelocks[i],
			},
		})
	}

	return outputIDs, outputs, investorTimelocks
}

func newVestingOutputID(supplyIncreaseMarker []byte, outputIndex *uint32) iotago3.OutputID {
	outputID := iotago3.OutputID{}
	// 30 bytes marker, 4 bytes for index
	copy(outputID[:], supplyIncreaseMarker[:len(supplyIncreaseMarker)-2])
	binary.LittleEndian.PutUint32(outputID[len(outputID)-4:], *outputIndex)
	*outputIndex += 1
	return outputID
}

func readChrysalisSnapshot(err error, cfg *Config) *ChrysalisSnapshot {
	chrysalisSnapshotFile, err := os.Open(cfg.ChrysalisSnapshotFile)
	if err != nil {
		log.Panicf("unable to open chrysalis snapshot file: %s", err)
	}

	chrysalisSnapshot := NewChrysalisSnapshot()

	if err := chrysalissnapshot.StreamSnapshotDataFrom(chrysalisSnapshotFile,
		// header
		func(header *chrysalissnapshot.ReadFileHeader) error {
			chrysalisSnapshot.Header = header
			return nil
		},
		// SEPs
		func(id hornet.MessageID) error {
			return nil
		},
		// ledger
		func(output *chrysalissnapshot.Output) error {
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
		func(output *chrysalisutxo.TreasuryOutput) error {
			chrysalisSnapshot.Stats.TotalOutputsCount++
			chrysalisSnapshot.TreasuryOutput = output
			chrysalisSnapshot.Stats.TreasuryFunds = output.Amount
			return nil
		},
		// milestone diffs
		func(milestoneDiff *chrysalissnapshot.MilestoneDiff) error {
			return nil
		},
	); err != nil {
		log.Panicf("unable to read in chrysalis snapshot data: %s", err)
	}

	chrysalisSnapshot.Stats.TotalBalance = chrysalisSnapshot.Stats.TotalBalanceSumOutputs + chrysalisSnapshot.Stats.TreasuryFunds

	return chrysalisSnapshot
}

func isDustOutput(output *chrysalissnapshot.Output) bool {
	return output.Amount < 1000000
}
