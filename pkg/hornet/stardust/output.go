package stardust

import (
	"sync"

	"github.com/iotaledger/hive.go/core/marshalutil"
	"github.com/iotaledger/hive.go/serializer/v2"
	iotago "github.com/iotaledger/iota.go/v3"
)

type Output struct {
	outputID          iotago.OutputID
	blockID           iotago.BlockID
	msIndexBooked     iotago.MilestoneIndex
	msTimestampBooked uint32

	outputData []byte
	outputOnce sync.Once
	output     iotago.Output
}

func (o *Output) SnapshotBytes() []byte {
	m := marshalutil.New()
	m.WriteBytes(o.outputID[:])
	m.WriteBytes(o.blockID[:])
	m.WriteUint32(o.msIndexBooked)
	m.WriteUint32(o.msTimestampBooked)
	m.WriteUint32(uint32(len(o.outputData)))
	m.WriteBytes(o.outputData)

	return m.Bytes()
}

func (o *Output) Output() iotago.Output {
	o.outputOnce.Do(func() {
		if o.output == nil {
			var err error
			outputType := o.outputData[0]
			o.output, err = iotago.OutputSelector(uint32(outputType))
			if err != nil {
				panic(err)
			}
			_, err = o.output.Deserialize(o.outputData, serializer.DeSeriModeNoValidation, nil)
			if err != nil {
				panic(err)
			}
		}
	})

	return o.output
}

func (o *Output) Deposit() uint64 {
	return o.Output().Deposit()
}

type Outputs []*Output

func CreateOutput(outputID iotago.OutputID, blockID iotago.BlockID, msIndexBooked iotago.MilestoneIndex, msTimestampBooked uint32, output iotago.Output) *Output {

	outputBytes, err := output.Serialize(serializer.DeSeriModeNoValidation, nil)
	if err != nil {
		panic(err)
	}

	o := &Output{
		outputID:          outputID,
		blockID:           blockID,
		msIndexBooked:     msIndexBooked,
		msTimestampBooked: msTimestampBooked,
		outputData:        outputBytes,
	}

	o.outputOnce.Do(func() {
		o.output = output
	})

	return o
}
