package mergerator

import (
	"encoding/csv"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	iotago3 "github.com/iotaledger/iota.go/v3"
)

func writeTokenUnlockCSV(cfg *Config, timelocksAndFunds map[uint32]uint64, fileName string) {
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
		rows = append(rows, row{
			timelock: k,
			tokens:   v,
		})
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

func writeOutputIDTokenUnlockCSV(cfg *Config, outputIDs []iotago3.OutputID, outputs []iotago3.Output, fileName string) {
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
