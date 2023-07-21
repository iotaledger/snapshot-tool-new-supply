# Snapshot Tool for new IOTA supply

## Run the tool

**Prerequisites:**
- A file containing the Assembly token distribution (default: `assembly_distribution.json`)
- A folder containing Assembly staking reward JSON files (default: `assembly_staking_rewards`)
- A Chrysalis snapshot file which will be used as input (default: `chrysalis_snapshot_file.bin`)

> Note that the tool verifies whether the tokens occurring within the generated snapshot correspond with the
total supply defined within the protocol parameters section within the configuration.

`config.json` specifies the allocations under `vesting.allocations`. 

For example:
```json
{
   "name": "Example",
   "unlocks": {
      "frequency": "bi-weekly",
      "initialUnlock": 0.10,
      "vestingPeriodYears": 4
   },
   "addresses": [
      {
         "name": "Address1",
         "address": "iota1prxvwqvwf7nru5q5xvh5thwg54zsm2y4wfnk6yk56hj3exxkg92mx20wl3s",
         "tokens": "1337"
      }
   ]
}
```
would allocate 1337 micros to the given address, vested over a 4-year period (bi-weekly unlocks) and a ~10% initial unlock.
Note that the initial unlock will not correspond precisely with the defined percentage as the remainder which does not 
fit equally into the subsequent time-locked outputs is added to the initial unlock.

> If `initialUnlock` is `1` then the entire amount becomes available immediately and there is no vesting.

You must have Go (at least 1.20) installed to run the tool:

```
$ go run main.go
```

An output snapshot file will be generated containing the ledger of the Chrysalis snapshot + the defined allocations.

## Description
This tool generates the global snapshot for the supply adjustment on the IOTA mainnet by doing following:

1. Reads in a Chrysalis snapshot and converts the ledger to Stardust outputs, while also compressing dust outputs (outputs with less than 1,000,000 micros)
   into the first (byte lexical ordered) `SigLockedDustAllowanceOutput`. All converted outputs are `BasicOutput`s except the treasury output.
2. Generates outputs for the new supply by reading in the config where one can define the vesting periods, initial
   unlock percentage and the unlock frequency (daily, weekly, bi-weekly, monthly). This step also uses Assembly staking round
   results added in the corresponding folder, where the rewards are distributed pro rata of the defined allocated IOTA
   tokens for the stakers.
3. Writes out the outputs into a Stardust snapshot while retaining the same output IDs for the outputs stemming from the
   Chrysalis snapshot and using a special marker for the supply increase outputs which is the blake2b-256 hash (only using 28 bytes of it)
   of the in the config defined input text (default "themerge", resulting
   in `0xb191c4bc825ac6983789e50545d5ef07a1d293a98ad974fc9498cb1807f0`) to mark the 28 first bytes of the output ID and
   with the next 4 bytes acting as a counter. Note that therefore the output index for all these new output IDs is 0.
4. Exports all generated outputs as CSV files in the configured folders (if enabled in the config).
5. Writes out a blake2b-256 hash of the generated snapshot file.

Optionally, the snapshot can be augmented with a CSV import file for testing with following format: "address,balance" where
`address` is a hex encoded address with 0x prefix. For example "0xd4b43b68f8184eaf9007a48debfd5e28f634626ac64c36c9e58540f4a2f64ce2,1337"