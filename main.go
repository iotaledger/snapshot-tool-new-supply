package main

import (
	"encoding/json"
	"log"

	flag "github.com/spf13/pflag"

	iotago3 "github.com/iotaledger/iota.go/v3"
	"github.com/iotaledger/the-mergerator/pkg/mergerator"
)

func main() {
	configFilePath := flag.CommandLine.StringP("config", "c", "config.json", "file path of the configuration file")
	flag.Parse()

	cfg, err := mergerator.LoadConfigFile(*configFilePath)
	if err != nil {
		log.Panic(err)
	}

	chrysalisSnapshot := mergerator.ReadChrysalisSnapshot(cfg)
	beautifiedChrysalisStats, err := json.MarshalIndent(chrysalisSnapshot.Stats, "", " ")
	if err != nil {
		log.Panicf("unable to serialize chrysalis snapshot stats: %s", err)
	}

	log.Printf("read in chrysalis snapshot %s:", cfg.Snapshot.ChrysalisSnapshotFile)
	log.Println(string(beautifiedChrysalisStats))

	log.Println("converting to stardust ledger outputs:")
	stardustOutputIDs, stardustOutputs := chrysalisSnapshot.StardustOutputs()
	log.Printf("outputs count under Stardust %d (from %d previously)", len(stardustOutputs), chrysalisSnapshot.Stats.TotalOutputsCount)
	log.Printf("converted output + ids hash: %s / %s", mergerator.OutputsHash(stardustOutputs), mergerator.OutputIDsHash(stardustOutputIDs))

	log.Printf("generating outputs for supply increase with starting date %s", cfg.Vesting.StartingDate)
	supplyIncreaseOutputIDs, supplyIncreaseOutputs := mergerator.GenerateNewSupplyOutputs(cfg)
	log.Printf("supply increase outputs + ids hash: %s / %s", mergerator.OutputsHash(supplyIncreaseOutputs), mergerator.OutputIDsHash(supplyIncreaseOutputIDs))
	log.Printf("generated %d outputs for supply increase", len(supplyIncreaseOutputs))

	var csvImportOutputIDs []iotago3.OutputID
	var csvImportOutputs []iotago3.Output
	if cfg.CSV.Import.Active {
		log.Printf("generating outputs from %s", cfg.CSV.Import.LedgerFile)
		csvImportOutputIDs, csvImportOutputs = mergerator.GenerateCSVOutputs(cfg)
		log.Printf("CSV import outputs + ids hash: %s / %s", mergerator.OutputsHash(csvImportOutputs), mergerator.OutputIDsHash(csvImportOutputIDs))
		log.Printf("generated %d outputs from CSV import file", len(csvImportOutputs))
	}

	if cfg.Snapshot.SkipSnapshotGeneration {
		log.Println("finished")
		return
	}

	if err := mergerator.GenerateSnapshot(cfg,
		chrysalisSnapshot,
		stardustOutputIDs,
		stardustOutputs,
		supplyIncreaseOutputIDs,
		supplyIncreaseOutputs,
		csvImportOutputIDs,
		csvImportOutputs); err != nil {
		log.Panic(err)
	}
}
