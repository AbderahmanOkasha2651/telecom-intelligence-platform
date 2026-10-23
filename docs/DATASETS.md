# Telecom Intelligence Platform — Data Catalog

This document describes the datasets used by the platform,
their original sources, licenses, local paths, and reproduction steps.

## Dataset Overview

| ID | Dataset | Type | Business Domain |
|---|---|---|---|
| network | Commercial RAN Performance Counters | Real, anonymized | 4G/5G Network Monitoring |
| fraudzen | FraudZen CDR Dataset | Synthetic | SIMBox Fraud Detection |

The two datasets come from independent environments.
There is no verified common subscriber, cell, or operator identity.
Cross-dataset record-level joins are not supported.

---

## 1. Network Performance Dataset

**Official name:** Performance Management Counters from Live 5G,
4G and 2G Radio Access Network

**Authors:** Peter Lehoczký, Matúš Turcsány,
Laura Krajčovičová, Filip Zatroch, Marcel Kajan,
and Marek Galinski.

- Repository: https://zenodo.org/records/17815388
- DOI: https://doi.org/10.5281/zenodo.17815388
- Selected archive: Dataset_01.zip
- License: CC BY 4.0 (per source README)
- Archive MD5: cba4e8c2887c0d9382c6a63736fe5ad5

### Files Used

| File | Technology | Rows observed |
|---|---|---:|
| Dataset_01_LTE_2100.csv | 4G LTE / 2100 MHz | 151,026 |
| Dataset_01_NR_1800.csv | 5G NR / 1800 MHz | 151,026 |

The row counts were observed during initial profiling
and should be independently validated by the pipeline.

### Data Characteristics

- Source measurements are aggregated at 15-minute intervals.
- Timestamps are documented as UTC.
- Pseudonymized site and sector identifiers are provided.
- Measurements include user activity, traffic volumes,
  resource utilization, radio-quality indicators, and energy counters.
- Missing and zero values must be interpreted by counter definition.

### Business Use Cases

- Network performance monitoring
- Traffic and utilization trends
- Sector-level KPI analysis
- Performance degradation indicators
- Data freshness and completeness monitoring

No confirmed outage labels are supplied.
An anomaly is not automatically a verified network outage.

### Local Files

data/raw/network/Dataset_01_LTE_2100.csv

data/raw/network/Dataset_01_NR_1800.csv

The initial scope uses Dataset_01 only.
Dataset_02 and Dataset_03 are not required for initial setup.

---

## 2. FraudZen Dataset

**Official name:** FraudZen Dataset: Realistic Ground Truth
CDRs of Bypass Fraud Techniques in Mobile Networks

**Authors:** Anne Josiane Kouam, Aline Carneiro Viana,
and Alain Tchana.

- Repository: https://zenodo.org/records/15706356
- DOI: https://doi.org/10.5281/zenodo.15706356
- Simulator: https://gitlab.inria.fr/simbox-fraud-mitigation/fraudzen
- Archive: Fraudzen-dataset.zip
- License: CC BY 4.0
- Archive MD5: 395fb1e8c21c0c752ecfbbab8bfe8d54

### Data Characteristics

FraudZen generates synthetic mobile Call Detail Records
under different SIMBox bypass-fraud strategies.

### Scenario Groups

- advanced_mobility
- advanced_social
- advanced_traffic
- all_advanced
- all_naive

Each scenario may include its own simulation configurations
and event traces. Scenario and run boundaries must be
validated before training or evaluating detectors.

### Important Source Files

| File | Purpose |
|---|---|
| Op_1_CDRTrace.txt | Local network activity events |
| International_CDRTrace.txt | International call events |
| fraudulentUsers.txt | Known simulated fraudulent subscriber IDs |
| softswitch_canceled_calls.txt | Canceled international requests |

The `fraudulentUsers.txt` ground truth is reserved for
OFFLINE EVALUATION ONLY.

It must never be included in online scoring features.

### Business Use Cases

- Subscriber behavioral profiling
- SIMBox fraud candidate detection
- Call-pattern anomaly analysis
- Explainable fraud-risk scoring
- Offline Precision, Recall, and F1 evaluation

Synthetic benchmark performance must not be presented
as verified accuracy on real mobile networks.

### Local Structure

data/raw/fraudzen/
    scenarios/
        advanced_mobility/
        advanced_social/
        advanced_traffic/
        all_advanced/
        all_naive/
    Inputs/
    Readme.md

---

## 3. Reproducible Dataset Setup

Requirements:

- Python 3.10+
- Internet access
- Sufficient available disk space

Run from the repository root:

```bash
python3 scripts/setup_datasets.py
```

To prepare one dataset:

```bash
python3 scripts/setup_datasets.py --dataset network
python3 scripts/setup_datasets.py --dataset fraudzen
```

The setup script:

1. Reads data/manifests/datasets.json.
2. Downloads original archives from Zenodo when missing.
3. Verifies the published MD5 checksums.
4. Extracts files into the documented local structure.
5. Skips extracted files whose sizes match the archive.
6. Rejects conflicting local files unless --force is supplied.
7. Excludes irrelevant macOS metadata from extraction.

To restore conflicting extracted files from the verified archives:

```bash
python3 scripts/setup_datasets.py --force
```

Use --force deliberately because it overwrites extracted files.

Original archives remain in data/archives/.
Both raw files and archives are normally excluded from Git.

## 4. Data Governance

- Preserve source attribution and license references.
- Preserve original records without silent modification.
- Track source record IDs and archive checksums.
- Do not join unrelated source identity namespaces.
- Separate original event time from replay/ingestion time.
- Keep ground-truth fraud labels isolated from scoring.
- Do not commit secrets, credentials, or private customer data.

## 5. Manifest

Machine-readable dataset configuration:

`data/manifests/datasets.json`

The manifest defines the source URLs, archive integrity
checksums, archive locations, and extraction mappings.
