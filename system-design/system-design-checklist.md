# Checklist for System Design

- Step 1 — Understand the data requirements and establish scope
- Step 2 — Propose high-level data flow and get feedback
- Step 3 — Dive deep into the data pipeline design
- Step 4 — Dive into Database Design
- Step 5 — Wrap up and Consider Scalability

### Source of Data

- Source of data
    - Data format
    - Data struct
- Average daily data size or row count
- Spikes in Traffic (due to sales etc)

### Compute

- Frequency of processing - adhoc/periodically - serverless option
- Final users - Read speed, available, frequency, range of data queried
- Transformations - stateful or stateless
- Streaming or Batch processing
    - Streaming: What is the max latency
    - Batch: Processing period
- Compute for the jobs - On-prem, cloud

### Storage

- Data size
- Frequency of access of data
- Read speeds required
- Cost
- Data storage - How long to keep data backup

### Monitoring and Data Quality

- Logging/Monitoring needed
- Data Quality checks
- Scheduler
