# Project GridWatch: High-Performance Time-Series Data Engineering on AWS

![Architecture Diagram](link_to_your_diagram.png)

## Overview

This project is an end-to-end demonstration of data engineering best practices for time-series data. It ingests historical UK National Grid energy demand data, stores it in AWS Timestream, and provides a quantitative benchmark showing the performance impact of proper data modeling and querying in a time-series database.

The core objective is to prove that **how you model and query your data is just as important as the technology you use.**

## Core Skills Demonstrated
- **Cloud Architecture:** AWS Lambda, S3, Timestream, IAM
- **Data Pipeline (ETL):** Python-based serverless ingestion script.
- **Time-Series Database:** Schema design (Dimensions vs. Measures) in Amazon Timestream.
- **Performance Benchmarking:** Systematic measurement of query latency to validate optimisation techniques.
- **Data Processing:** Use of Pandas for data cleaning and preparation.

## The Experiment: Optimised vs. Un-optimised Queries

### Hypothesis
In Amazon Timestream, queries that filter on an indexed **Dimension** will be significantly faster and more cost-effective than queries that perform complex operations or filter on **Measure** values to achieve the same analytical goal.

### Methodology
I ingested one year of UK energy demand data into a Timestream table with `region` as a dimension. I then ran two queries 10 times each and measured the average execution time:

1.  **Optimised Query:** A simple `SELECT AVG(...)` query with a `WHERE` clause filtering on the `region` dimension.
2.  **Un-optimised Query:** A query that calculates the same average but includes a more complex operation inside the aggregation, forcing the query engine to do more work per row.

```python
# Insert your benchmarking Python code snippet here
```

### Results
The results clearly support the hypothesis. The optimised query that leverages the dimension was, on average, **X.X% faster** than the un-optimised one.

![Benchmark Results Chart](link_to_your_chart.png)

*(A bar chart showing "Optimised Query" with a low execution time and "Un-optimised Query" with a higher one)*

### Conclusion
This experiment quantitatively demonstrates the critical importance of schema design in time-series databases. By modeling `region` as a dimension, we allow Timestream's query engine to rapidly prune irrelevant data partitions, leading to a dramatic improvement in performance. This is a foundational concept in designing scalable and cost-effective time-series systems.

## How to Run This Project
1. Clone the repository: `git clone ...`
2. Set up AWS credentials.
3. (Add your specific setup steps here...)

## Cost Management
This entire project was designed to be completed for under £5.
- **AWS Lambda & Timestream** both have generous free tiers.
- The dataset is small (~50MB), so S3 and data transfer costs are negligible.
- **IMPORTANT:** All resources (Lambda function, Timestream DB, S3 bucket) were torn down immediately after the project was completed to prevent incurring further costs.