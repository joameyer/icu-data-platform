# ASIC Mechanical Ventilation >=24h QC

## Purpose
This derivation is an upstream cohort-contract verification for Chapter 1.

It is intended to answer a narrow QC question:
"Does a stay have at least one observed ventilation episode lasting at least 24 hours
when we look directly at harmonized ASIC dynamic ventilation-support markers?"

It is not intended to be a perfect reconstruction of true mechanical ventilation timing.

## Input
The derivation uses harmonized ASIC dynamic data directly, not blocked 8-hour data.

Required harmonized columns:
- `stay_id_global`
- `hospital_id`
- `time`
- `fio2`
- `peep`
- `vt`
- `vt_per_kg_ibw`

## Observed ventilation-support rule
A harmonized dynamic timestamp is treated as ventilation-supported if at least one of the
following variables is non-missing at that timestamp:
- `fio2`
- `peep`
- `vt`
- `vt_per_kg_ibw`

## Episode rule
Within each stay:
- sort ventilation-supported timestamps by harmonized dynamic time
- connect adjacent timestamps into the same observed ventilation episode when the gap is
  less than or equal to 8 hours
- start a new episode when the gap is greater than 8 hours

Each observed episode records:
- episode start time
- episode end time
- episode duration in hours, defined as `episode_end_time - episode_start_time`

## QC criterion
A stay satisfies `mech_vent_ge_24h_qc` if any observed ventilation episode has derived
duration greater than or equal to 24 hours.

## Artifacts
The upstream harmonized pipeline writes:
- stay-level QC output
- episode-level QC output
- hospital-level summary
- explicit failed-stay output
- derivation documentation notes
