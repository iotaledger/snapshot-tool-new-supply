package stardust

import (
	"github.com/iotaledger/hive.go/core/marshalutil"
	iotago "github.com/iotaledger/iota.go/v3"
)

// Spent are already spent TXOs (transaction outputs).
type Spent struct {
	// the ID of the transaction that spent the output
	transactionIDSpent iotago.TransactionID

	output *Output
}

func (s *Spent) SnapshotBytes() []byte {
	m := marshalutil.New()
	m.WriteBytes(s.Output().SnapshotBytes())
	m.WriteBytes(s.transactionIDSpent[:])
	// we don't need to write msIndexSpent and msTimestampSpent because this info is available in the milestoneDiff that consumes the output
	return m.Bytes()
}

func (s *Spent) Output() *Output {
	return s.output
}

type Spents []*Spent
