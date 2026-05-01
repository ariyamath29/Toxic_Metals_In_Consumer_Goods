# Consumer Products and Toxic Metals Analysis
**Ariya Mathrawala**

## Project Summary
This project analyzes the NYC Department of Health and Mental Hygiene dataset on toxic metal content in consumer products. The goal is to identify which product categories, manufacturers, and countries of origin are associated with the highest contamination risk. All analytics were developed using Spark Scala on NYU Dataproc, with HDFS used for storage and Hive used for structured access to cleaned output.

## Read published story [here](https://medium.com/p/6155cb9d2b4e?postPublishedType=in)

## Directory Structure
* `/ana_code`: Contains Spark Scala analytics code used to compute mean, median, mode, standard deviation, hazard rates, and rankings by product type, manufacturer, and country.
* `/data_ingest`: Contains commands used to upload the raw CSV dataset to HDFS.
* `/etl_code/ariya_mathrawala`: Contains Spark Scala code for cleaning the raw dataset and writing the cleaned output to HDFS.
* `/profiling_code/ariya_mathrawala`: Contains Spark Scala code for counting records, checking distinct values, and profiling columns before analysis.
* `/test_code`: Contains code to understand bias that can affect intended results
* `/screenshots`: Contains screenshots that show the code running for every step, fully documented and organized into subdirectories (`/profiling`, `/etl`, `/analytics`, `/hive`, etc.).


## Input Data Location
**Raw input data:**
HDFS path: `/user/aam9811_nyu_edu/input/consumer_metals.csv`

**Cleaned data:**
HDFS path: `/user/aam9811_nyu_edu/output/clean_consumer_metals`

*Note: Read/Execute access to these HDFS directories has been shared with the required grader NetIDs.*

---

## How to Run the Code

**How to Build:**
This project utilizes Scala scripts executed interactively via the Spark shell

**Data Ingest:**
```bash
hdfs dfs -mkdir -p /user/aam9811_nyu_edu/input
hdfs dfs -put consumer_metals.csv /user/aam9811_nyu_edu/input/
```

**Profiling:**
```bash
spark-shell --deploy-mode client -i profiling_code/ariya_mathrawala/CountRecs.scala
```

**ETL:**
```bash
spark-shell --deploy-mode client -i etl_code/ariya_mathrawala/Clean.scala
```

**Analytics:**
```bash
spark-shell --deploy-mode client -i ana_code/FirstCode.scala
```

*OR alternatively, once in spark shell use :load <file path> *

**Hive:**
```sql
USE bd_project;
SHOW TABLES;
DESCRIBE clean_consumer_metals;
SELECT * FROM clean_consumer_metals LIMIT 10;
```

## Where to Find Results
HDFS Data Locations:
- Raw input data:
  /user/aam9811_nyu_edu/input/consumer_metals.csv

- Cleaned and transformed dataset (output of ETL):
  /user/aam9811_nyu_edu/output/clean_consumer_metals

- Hive table (external table on cleaned data):
  consumer_metals_cleaned
  (points to /user/aam9811_nyu_edu/output/clean_consumer_metals)

Analytic Results:
- Analytic results (mean, median, hazard rates, rankings, etc.) are generated dynamically by the Spark analytics script and printed to the console.
- These outputs are documented in:
  /screenshots/analytics

Profiling Results:
- Record counts and distinct value outputs are printed to the console and documented in:
  /screenshots/profiling

Cleaning Results:
- The cleaned dataset written to HDFS is documented in:
  /screenshots/etl

Hive Results:
- Hive table creation and schema are documented in:
  /screenshots/hive



## Ethical Guardrails and Governance

This project includes several data processing decisions that may introduce bias or affect interpretation of results. These decisions were analyzed and justified as part of the data pipeline.

1. Handling of Missing and Unknown Values:
Rows containing missing or non-numeric concentration values were standardized to "Unknown" and excluded from certain statistical analyses. This may bias results toward products and manufacturers underrepresenting risk in poorly documented categories.

2. Hazard Threshold Assumptions:
The hazard classification column is based on approximate thresholds derived from multiple regulatory contexts. Since safety standards vary by country, product type, and regulator, these thresholds represent a simplified model of risk rather than universally accepted limits. Thus, hazard classifications should be interpreted as relative indicators rather than absolute regulatory violations.

3. Product Normalization Bias:
The normalization of product names reduces fragmentation but may group distinct products into a single category. This can slightly distort category-level stats, especially for niche products.

4. Exclusion of Ambiguous Hazard Values:
Rows where hazard classification could not be determined (NA) were excluded from hazard-rate calculations. This may affect comparisons between groups, as some categories may have more ambiguous or incomplete data than others.

5. Alternative Approaches:
Alternative methods could include:
- Using probabilistic estimations instead of excluding unknown values
- Applying multiple thresholds from different regulatory agencies
- Dual analysis with original product labels alongside normalized ones
- Weighting results based on data completeness


## Notes on Parsing and Cleaning
The raw CSV contained inconsistent splitting when parsed with comma delimiter because some text fields contained commas inside quoted strings. To address this, I used regex parsing. Rows with missing or invalid values in key analytic columns were intentionally excluded from the cleaned dataset.

