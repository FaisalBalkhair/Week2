# Week2 - Analytical Summary Report

## Key Findings:

- **Total Orders:** 5
- **Valid Revenue Orders (Non Missing amounts):** 4
- **Total Revenue (SA):** 45.5
- **Total Revenue (AE):** 0.0
- **Average Order Values (SA):** 15.17
- **Refund Rate:** 1/5 Orders (20%)
- **Detected Outliers in Amount:** 1

## Definitions:

- **Revenue**
  Sum of "amount" for non missing values

- **Refund**
  Orders where status == "refund"

- **Average Order Value**
  Mean of "amount" column

- **Valid Orders**
  Orders where "amount" is not missing

- **Time Features**
  Extracted from created_at: day/ year/ month/ dow/ hour

## Data Quality Caveats:

- **Missing Values**

  - "amount": 1 missing value
  - "quantity": 1 missing value
  - "created_at": 1 missing timestamp

- **Duplicates**

  - Users can appear multiple time in orders

- **Joins**

  - Users joined orders Validated as: Many_to_One

- **Outliers**

  - One amount detected as Outlier (100)
  - Winsorization applied to reduce skew

- **Dataset Size**
  - Dataset size to very small, 5 Rows only

## **Next Questions:** Recommended follow-up analyses

- Why did the AE transaction result in missing amount?
- What is the true refund rate with more data?
- Does the revenue trend rise over time?
- How sensitive are results to outliers?
