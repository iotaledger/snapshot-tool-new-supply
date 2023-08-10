package mergerator

import (
	"encoding/csv"
	"log"
	"os"

	"golang.org/x/crypto/blake2b"

	iotago3 "github.com/iotaledger/iota.go/v3"
)

func GenerateCSVOutputs(cfg *Config) ([]iotago3.OutputID, []iotago3.Output) {
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
	log.Printf("using marker '%s' to mark CSV import outputs", iotago3.EncodeHex(outputMarker[:len(outputMarker)-2]))

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
