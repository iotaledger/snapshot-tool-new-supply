package mergerator

import (
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"

	"golang.org/x/crypto/blake2b"

	"github.com/iotaledger/hive.go/serializer/v2"
	iotago3 "github.com/iotaledger/iota.go/v3"
	"github.com/iotaledger/the-mergerator/pkg/hornet/stardust"
)

func GenerateSnapshot(cfg *Config,
	chrysalisSnapshot *ChrysalisSnapshot,
	stardustOutputIDs []iotago3.OutputID,
	stardustOutputs []iotago3.Output,
	supplyIncreaseOutputIDs []iotago3.OutputID,
	supplyIncreaseOutputs []iotago3.Output,
	csvImportOutputIDs []iotago3.OutputID,
	csvImportOutputs []iotago3.Output) error {

	protoParamsBytes, err := cfg.ParsedProtocolParameters.Serialize(serializer.DeSeriModeNoValidation, nil)
	if err != nil {
		log.Panicf("failed to serialize protocol parameters: %s", err)
	}

	treasuryTokens := mustParseUint64(cfg.TreasuryTokens)

	// create snapshot file
	var targetIndex iotago3.MilestoneIndex
	var innerErr error
	fullHeader := &stardust.FullSnapshotHeader{
		Version:                  stardust.SupportedFormatVersion,
		Type:                     stardust.Full,
		GenesisMilestoneIndex:    iotago3.MilestoneIndex(cfg.Snapshot.GenesisMilestoneIndex),
		TargetMilestoneIndex:     iotago3.MilestoneIndex(cfg.Snapshot.TargetMilestoneIndex),
		TargetMilestoneTimestamp: uint32(cfg.Snapshot.TargetMilestoneTimestamp),
		TargetMilestoneID: func() iotago3.MilestoneID {
			if len(cfg.Snapshot.TargetMilestoneID) == 0 {
				return iotago3.MilestoneID{}
			}

			var msID iotago3.MilestoneID
			milestoneIDBytes, err := iotago3.DecodeHex(cfg.Snapshot.TargetMilestoneID)
			if err != nil {
				innerErr = fmt.Errorf("unable to convert target milestone ID %s: %w", cfg.Snapshot.TargetMilestoneID, err)
			} else {
				copy(msID[:], milestoneIDBytes)
			}
			return msID
		}(),
		LedgerMilestoneIndex: iotago3.MilestoneIndex(cfg.Snapshot.LedgerMilestoneIndex),
		TreasuryOutput: &stardust.TreasuryOutput{
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
	if innerErr != nil {
		return innerErr
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
	var chrysalisLedgerIndex, supplyIncreaseOutputsIndex, csvImportOutputsIndex int
	var nonTreasuryOutputsSupplyTotal uint64
	outputProducerFunc := func() (*stardust.Output, error) {

		if chrysalisLedgerIndex < len(stardustOutputs) {
			output := stardust.CreateOutput(
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
			output := stardust.CreateOutput(
				supplyIncreaseOutputIDs[supplyIncreaseOutputsIndex],
				iotago3.EmptyBlockID(),
				0,
				0,
				supplyIncreaseOutputs[supplyIncreaseOutputsIndex])
			nonTreasuryOutputsSupplyTotal += output.Deposit()
			supplyIncreaseOutputsIndex++
			return output, nil
		}

		if len(csvImportOutputIDs) > 0 && csvImportOutputsIndex < len(csvImportOutputs) {
			output := stardust.CreateOutput(
				csvImportOutputIDs[csvImportOutputsIndex],
				iotago3.EmptyBlockID(),
				0,
				0,
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
		return fmt.Errorf("unable to create snapshot file: %w", err)
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
		return fmt.Errorf("couldn't generate snapshot file: %w", err)
	}

	if err := fileHandle.Close(); err != nil {
		return fmt.Errorf("unable to close snapshot file: %w", err)
	}

	if cfg.ValidateSupply {
		if cfg.ParsedProtocolParameters.TokenSupply != nonTreasuryOutputsSupplyTotal+treasuryTokens {
			return fmt.Errorf("supply defined in protocol parameters does not match supply within generated snapshot! %d vs. %d", cfg.ParsedProtocolParameters.TokenSupply, nonTreasuryOutputsSupplyTotal+treasuryTokens)
		}
	}

	// rename tmp file to final file name
	if err := os.Rename(outputFilePathTmp, cfg.Snapshot.OutputSnapshotFile); err != nil {
		return fmt.Errorf("unable to rename temp snapshot file: %w", err)
	}

	log.Println("computing blake2b-256 hash of snapshot file...")
	hash, err := blake2b.New256(nil)
	if err != nil {
		return fmt.Errorf("unable to hash function: %w", err)
	}
	snapshotFile, err := os.Open(cfg.Snapshot.OutputSnapshotFile)
	if err != nil {
		return fmt.Errorf("unable to read snapshot file for hash computation: %w", err)
	}

	var deferErr error
	defer func(snapshotFile *os.File) {
		if err := snapshotFile.Close(); err != nil {
			deferErr = fmt.Errorf("unable to close generated snapshot file: %w", err)
		}
	}(snapshotFile)

	if _, err := io.Copy(hash, snapshotFile); err != nil {
		return fmt.Errorf("unable to read snapshot file into hash function: %w", err)
	}

	log.Printf("snapshot creation successful! blake2b-256 hash %s", hex.EncodeToString(hash.Sum(nil)))
	log.Printf("supply in ledger outputs %d, treasury %d, total %d", nonTreasuryOutputsSupplyTotal, treasuryTokens, nonTreasuryOutputsSupplyTotal+treasuryTokens)
	log.Printf("total outputs written to snapshot: %d", len(stardustOutputs)+len(supplyIncreaseOutputs))

	return deferErr
}
