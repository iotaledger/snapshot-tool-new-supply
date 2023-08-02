# The Mergerator

> This tool is for internal use only, you must not share it with anyone outside the foundation!

This tool generates the global snapshot for "The Merge" on the IOTA mainnet by doing following:

1. Reads in a Chrysalis snapshot and converts the ledger to Stardust outputs, while also compressing dust outputs (
   outputs with less than 1Mi) into the first (byte lexical ordered) `SigLockedDustAllowanceOutput`. All converted
   outputs are `BasicOutput`s except the treasury output.
2. Generates outputs for the new supply by reading in the config where one can define the vesting periods, initial
   unlock percentage and the unlock frequency (daily, weekly, monthly). This step also uses Assembly staking round
   results added in the corresponding folder, where the rewards are distributed pro rata of the defined allocated IOTA
   tokens for the stakers. Same applies to IF member rewards.
3. Writes out the outputs into a Stardust snapshot while retaining the same output IDs for the outputs stemming from the
   Chrysalis snapshot and using a special marker which is the blake2b-256 hash (only using 30 bytes of it)
   of the in the config defined input text (default "themerge", resulting
   in `0xb191c4bc825ac6983789e50545d5ef07a1d293a98ad974fc9498cb1807f0`) to mark the 30 first bytes of the output ID and
   with the last 4 bytes being the index of the output in little endian (note that this
   means that the "output index" of these outputs can exceed the 128 index limit).
4. Exports all generated outputs as CSV files in the configured folders (if enabled in the config).
5. Writes out a blake2b-256 hash of the generated snapshot file.

Optionally, the snapshot can be augmented with a CSV import file with following format: "address,balance" where
`address` is a hex encoded address with 0x prefix. For example "
0xd4b43b68f8184eaf9007a48debfd5e28f634626ac64c36c9e58540f4a2f64ce2,1337"

Todo:

- [x] Chrysalis snapshot conversion
- [x] ASMB staking import
- [x] Vesting output generation
- [x] Snapshot generation
- [x] CSV exports
    - [x] Schedule
    - [x] Summaries (per group)
- [x] Determinism
- [x] Add CSV import for balance of testing scenarios
- [x] IF ASMB rewards
- [ ] Tested
