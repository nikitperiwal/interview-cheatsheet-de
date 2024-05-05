# Checklist for System Design

- Do I need to use any tech in particular?

### Source of Data

- Average daily data size or row count
- Spikes in Traffic (due to sales etc)
- Source of data
    - Type of data depending on source - structured or semi-structured

### Compute

- Frequency of processing - adhoc/periodically - serverless option
- Final users - Read speed, available, frequency, range of data queried
- Transformations - stateful or stateless
- Streaming or Batch processing
    - Streaming: What is the max latency
    - Batch: Processing period
- On-prem or Cloud - Any preference on which cloud

### Storage

- Data size
- Data storage - How long to keep data backup, cost

### Monitoring and Data Quality

- Logging/Monitoring needed
- Data Quality checks
- Scheduler
