package stardust

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/iotaledger/hive.go/serializer/v2"
	iotago "github.com/iotaledger/iota.go/v3"
)

const (
	// SupportedFormatVersion defines the supported snapshot file version.
	SupportedFormatVersion byte = 2
)

var (
	// ErrMilestoneDiffProducerNotProvided is returned when a milestone diff producer has not been provided.
	ErrMilestoneDiffProducerNotProvided = errors.New("milestone diff producer is not provided")
	// ErrSolidEntryPointProducerNotProvided is returned when a solid entry point producer has not been provided.
	ErrSolidEntryPointProducerNotProvided = errors.New("solid entry point producer is not provided")
	// ErrOutputProducerNotProvided is returned when an output producer has not been provided.
	ErrOutputProducerNotProvided = errors.New("output producer is not provided")
	// ErrTreasuryOutputNotProvided is returned when the treasury output for a full snapshot has not been provided.
	ErrTreasuryOutputNotProvided = errors.New("treasury output is not provided")
	// ErrWrongSnapshotType is returned if the snapshot type is not supported by this function.
	ErrWrongSnapshotType = errors.New("wrong snapshot type")
	// ErrNoMoreSEPToProduce is returned when there are no more solid entry points to produce.
	ErrNoMoreSEPToProduce = errors.New("no more SEP to produce")
)

// Type defines the type of the snapshot.
type Type byte

const (
	// Full is a snapshot which contains the full ledger entry for a given milestone
	// plus the milestone diffs which subtracted to the ledger milestone reduce to the target milestone ledger.
	// the full snapshot contains additional milestone diffs to calculate the correct protocol parameters (before the target index).
	Full Type = iota
)

// OutputProducerFunc yields an output to be written to a snapshot or nil if no more is available.
type OutputProducerFunc func() (*Output, error)

// MilestoneDiffProducerFunc yields a milestone diff to be written to a snapshot or nil if no more is available.
type MilestoneDiffProducerFunc func() (*MilestoneDiff, error)

// SEPProducerFunc yields a solid entry point to be written to a snapshot or nil if no more is available.
type SEPProducerFunc func() (iotago.BlockID, error)

type FullSnapshotHeader struct {
	// Version denotes the version of this snapshot.
	Version byte
	// Type denotes the type of this snapshot.
	Type Type
	// The index of the genesis milestone of the network.
	GenesisMilestoneIndex iotago.MilestoneIndex
	// The index of the milestone of which the SEPs within the snapshot are from.
	TargetMilestoneIndex iotago.MilestoneIndex
	// The timestamp of the milestone of which the SEPs within the snapshot are from.
	TargetMilestoneTimestamp uint32
	// The ID of the milestone of which the SEPs within the snapshot are from.
	TargetMilestoneID iotago.MilestoneID
	// The index of the milestone of which the UTXOs within the snapshot are from.
	LedgerMilestoneIndex iotago.MilestoneIndex
	// The treasury output existing for the given ledger milestone index.
	// This field must be populated if a Full snapshot is created/read.
	TreasuryOutput *TreasuryOutput
	// Active Protocol Parameter of the ledger milestone index.
	ProtocolParamsMilestoneOpt *iotago.ProtocolParamsMilestoneOpt
	// The amount of UTXOs contained within this snapshot.
	OutputCount uint64
	// The amount of milestone diffs contained within this snapshot.
	MilestoneDiffCount uint32
	// The amount of SEPs contained within this snapshot.
	SEPCount uint16
}

func increaseOffsets(amount int64, offsets ...*int64) {
	for _, offset := range offsets {
		*offset += amount
	}
}

func writeFunc(writeSeeker io.WriteSeeker, variableName string, value any, offsetsToIncrease ...*int64) error {
	length := binary.Size(value)
	if length == -1 {
		return fmt.Errorf("unable to determine length of %s", variableName)
	}

	if err := binary.Write(writeSeeker, binary.LittleEndian, value); err != nil {
		return fmt.Errorf("unable to write LS %s: %w", variableName, err)
	}

	increaseOffsets(int64(length), offsetsToIncrease...)

	return nil
}

func writeFullSnapshotHeader(writeSeeker io.WriteSeeker, header *FullSnapshotHeader) (int64, error) {

	if header.Type != Full {
		return 0, ErrWrongSnapshotType
	}
	if header.ProtocolParamsMilestoneOpt == nil {
		return 0, iotago.ErrMissingProtocolParas
	}
	if header.TreasuryOutput == nil {
		return 0, ErrTreasuryOutputNotProvided
	}

	writeFunc := func(name string, value any, offsetsToIncrease ...*int64) error {
		return writeFunc(writeSeeker, name, value, offsetsToIncrease...)
	}

	// this is the offset of the OutputCount field in the header
	var countersPosition int64

	// Version
	// Denotes the version of this file format.
	if err := writeFunc("version", header.Version, &countersPosition); err != nil {
		return 0, err
	}

	// Type
	// Denotes the type of this file format. Value 0 denotes a full snapshot.
	if err := writeFunc("type", Full, &countersPosition); err != nil {
		return 0, err
	}

	// Genesis Milestone Index
	// The index of the genesis milestone of the network.
	if err := writeFunc("genesis milestone index", header.GenesisMilestoneIndex, &countersPosition); err != nil {
		return 0, err
	}

	// Target Milestone Index
	// The index of the milestone of which the SEPs within the snapshot are from.
	if err := writeFunc("target milestone index", header.TargetMilestoneIndex, &countersPosition); err != nil {
		return 0, err
	}

	// Target Milestone Timestamp
	// The timestamp of the milestone of which the SEPs within the snapshot are from.
	if err := writeFunc("target milestone timestamp", header.TargetMilestoneTimestamp, &countersPosition); err != nil {
		return 0, err
	}

	// Target Milestone ID
	// The ID of the milestone of which the SEPs within the snapshot are from.
	if err := writeFunc("target milestone ID", header.TargetMilestoneID[:], &countersPosition); err != nil {
		return 0, err
	}

	// Ledger Milestone Index
	// The index of the milestone of which the UTXOs within the snapshot are from.
	if err := writeFunc("ledger milestone index", header.LedgerMilestoneIndex, &countersPosition); err != nil {
		return 0, err
	}

	// Treasury Output Milestone ID
	// The milestone ID of the milestone which generated the treasury output.
	if err := writeFunc("treasury output milestone ID", header.TreasuryOutput.MilestoneID[:], &countersPosition); err != nil {
		return 0, err
	}

	// Treasury Output Amount
	// The amount of funds residing on the treasury output.
	if err := writeFunc("treasury output amount", header.TreasuryOutput.Amount, &countersPosition); err != nil {
		return 0, err
	}

	// ProtocolParamsMilestoneOpt Length
	// Denotes the length of the ProtocolParamsMilestoneOpt.
	protoParamsMsOptionBytes, err := header.ProtocolParamsMilestoneOpt.Serialize(serializer.DeSeriModeNoValidation, nil)
	if err != nil {
		return 0, fmt.Errorf("unable to serialize LS protocol parameters milestone option: %w", err)
	}
	if err := writeFunc("protocol parameters milestone option length", uint16(len(protoParamsMsOptionBytes)), &countersPosition); err != nil {
		return 0, err
	}

	// ProtocolParamsMilestoneOpt
	// Active ProtocolParamsMilestoneOpt of the ledger milestone
	if err := writeFunc("protocol parameters milestone option", protoParamsMsOptionBytes, &countersPosition); err != nil {
		return 0, err
	}

	var outputCount uint64
	var msDiffCount uint32
	var sepsCount uint16

	// Outputs Count
	// The amount of UTXOs contained within this snapshot.
	if err := writeFunc("outputs count", outputCount); err != nil {
		return 0, err
	}

	// Milestone Diffs Count
	// The amount of milestone diffs contained within this snapshot.
	if err := writeFunc("milestone diffs count", msDiffCount); err != nil {
		return 0, err
	}

	// SEPs Count
	// The amount of SEPs contained within this snapshot.
	if err := writeFunc("solid entry points count", sepsCount); err != nil {
		return 0, err
	}

	return countersPosition, nil
}

// TreasuryOutput represents the output of a treasury transaction.
type TreasuryOutput struct {
	// The ID of the milestone which generated this output.
	MilestoneID iotago.MilestoneID
	// The amount residing on this output.
	Amount uint64
	// Whether this output was already spent
	Spent bool
}

// MilestoneDiff represents the outputs which were created and consumed for the given milestone
// and the block itself which contains the milestone.
type MilestoneDiff struct {
	// The milestone payload itself.
	Milestone *iotago.Milestone
	// The created outputs with this milestone.
	Created Outputs
	// The consumed spents with this milestone.
	Consumed Spents
	// The consumed treasury output with this milestone.
	SpentTreasuryOutput *TreasuryOutput
}

func (md *MilestoneDiff) MarshalBinary() ([]byte, error) {
	var b bytes.Buffer

	msBytes, err := md.Milestone.Serialize(serializer.DeSeriModePerformValidation, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to serialize milestone for ls-milestone-diff %d: %w", md.Milestone.Index, err)
	}

	if err := binary.Write(&b, binary.LittleEndian, uint32(len(msBytes))); err != nil {
		return nil, fmt.Errorf("unable to write milestone payload length for ls-milestone-diff %d: %w", md.Milestone.Index, err)
	}

	if _, err := b.Write(msBytes); err != nil {
		return nil, fmt.Errorf("unable to write milestone payload for ls-milestone-diff %d: %w", md.Milestone.Index, err)
	}

	// write in spent treasury output
	opts := md.Milestone.Opts.MustSet()
	if opts.Receipt() != nil {
		if md.SpentTreasuryOutput == nil {
			panic("milestone diff includes a receipt but no spent treasury output is set")
		}
		if _, err := b.Write(md.SpentTreasuryOutput.MilestoneID[:]); err != nil {
			return nil, fmt.Errorf("unable to write treasury input milestone ID for ls-milestone-diff %d: %w", md.Milestone.Index, err)
		}

		if err := binary.Write(&b, binary.LittleEndian, md.SpentTreasuryOutput.Amount); err != nil {
			return nil, fmt.Errorf("unable to write treasury input amount for ls-milestone-diff %d: %w", md.Milestone.Index, err)
		}
	}

	if err := binary.Write(&b, binary.LittleEndian, uint32(len(md.Created))); err != nil {
		return nil, fmt.Errorf("unable to write created outputs array length for ls-milestone-diff %d: %w", md.Milestone.Index, err)
	}

	for x, output := range md.Created {
		outputBytes := output.SnapshotBytes()
		if _, err := b.Write(outputBytes); err != nil {
			return nil, fmt.Errorf("unable to write output %d for ls-milestone-diff %d: %w", x, md.Milestone.Index, err)
		}
	}

	if err := binary.Write(&b, binary.LittleEndian, uint32(len(md.Consumed))); err != nil {
		return nil, fmt.Errorf("unable to write consumed outputs array length for ls-milestone-diff %d: %w", md.Milestone.Index, err)
	}

	for x, spent := range md.Consumed {
		spentBytes := spent.SnapshotBytes()
		if _, err := b.Write(spentBytes); err != nil {
			return nil, fmt.Errorf("unable to write spent %d for ls-milestone-diff %d: %w", x, md.Milestone.Index, err)
		}
	}

	// length of the msDiff itself plus the length for the size field.
	msDiffLength := uint32(b.Len() + serializer.UInt32ByteSize)

	var bufMilestoneDiffLength bytes.Buffer
	if err := binary.Write(&bufMilestoneDiffLength, binary.LittleEndian, msDiffLength); err != nil {
		return nil, fmt.Errorf("unable to write length for ls-milestone-diff %d: %w", md.Milestone.Index, err)
	}

	return append(bufMilestoneDiffLength.Bytes(), b.Bytes()...), nil
}

// StreamFullSnapshotDataTo streams a full snapshot data into the given io.WriteSeeker.
// This function modifies the counts in the FullSnapshotHeader.
func StreamFullSnapshotDataTo(
	writeSeeker io.WriteSeeker,
	header *FullSnapshotHeader,
	outputProd OutputProducerFunc,
	msDiffProd MilestoneDiffProducerFunc,
	sepProd SEPProducerFunc) error {

	if outputProd == nil {
		return ErrOutputProducerNotProvided
	}
	if msDiffProd == nil {
		return ErrMilestoneDiffProducerNotProvided
	}
	if sepProd == nil {
		return ErrSolidEntryPointProducerNotProvided
	}

	countersPosition, err := writeFullSnapshotHeader(writeSeeker, header)
	if err != nil {
		return err
	}

	writeFunc := func(name string, value any, offsetsToIncrease ...*int64) error {
		return writeFunc(writeSeeker, name, value, offsetsToIncrease...)
	}

	var outputCount uint64
	var msDiffCount uint32
	var sepsCount uint16

	// Outputs
	for {
		output, err := outputProd()
		if err != nil {
			return fmt.Errorf("unable to get next LS output #%d: %w", outputCount+1, err)
		}

		if output == nil {
			break
		}

		outputCount++
		if err := writeFunc(fmt.Sprintf("output #%d", outputCount), output.SnapshotBytes()); err != nil {
			return err
		}
	}

	// Milestone Diffs
	for {
		msDiff, err := msDiffProd()
		if err != nil {
			return fmt.Errorf("unable to get next LS milestone diff #%d: %w", msDiffCount+1, err)
		}

		if msDiff == nil {
			break
		}

		msDiffCount++
		msDiffBytes, err := msDiff.MarshalBinary()
		if err != nil {
			return fmt.Errorf("unable to serialize LS milestone diff #%d: %w", msDiffCount, err)
		}
		if err := writeFunc(fmt.Sprintf("milestone diff #%d", msDiffCount), msDiffBytes); err != nil {
			return err
		}
	}

	// SEPs
	for {
		sep, err := sepProd()
		if err != nil {
			if errors.Is(err, ErrNoMoreSEPToProduce) {
				break
			}

			return fmt.Errorf("unable to get next LS SEP #%d: %w", sepsCount+1, err)
		}

		sepsCount++
		if err := writeFunc(fmt.Sprintf("SEP #%d", sepsCount), sep[:]); err != nil {
			return err
		}
	}

	// seek back to the file position of the counters
	if _, err := writeSeeker.Seek(countersPosition, io.SeekStart); err != nil {
		return fmt.Errorf("unable to seek to LS counter placeholders: %w", err)
	}

	// Outputs Count
	// The amount of UTXOs contained within this snapshot.
	if err := writeFunc("outputs count", outputCount); err != nil {
		return err
	}

	// Milestone Diffs Count
	// The amount of milestone diffs contained within this snapshot.
	if err := writeFunc("milestone diffs count", msDiffCount); err != nil {
		return err
	}

	// SEPs Count
	// The amount of SEPs contained within this snapshot.
	if err := writeFunc("solid entry points count", sepsCount); err != nil {
		return err
	}

	// update the values in the header
	header.OutputCount = outputCount
	header.MilestoneDiffCount = msDiffCount
	header.SEPCount = sepsCount

	return nil
}
