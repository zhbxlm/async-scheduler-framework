# Data Flow

## Task Submission Flow

```
Caller → Task API (8001) → Redis Queue → Worker Node → Callback URL
                                ↓
                           MySQL (task state)
```

1. Caller submits task to **Task API** with `capability`, `input_data`, optional `callback_url`
2. Task API writes task record to **MySQL** (status = `pending`)
3. Task API pushes task ID to **Redis** capability queue
4. **Worker Node** (via Agent) BLPOPs from the queue
5. Worker executes the task, updates status in MySQL (`running` → `success`/`failed`)
6. If `callback_url` is set, Worker POSTs result back to caller

## DAG Execution Flow

For multi-step DAGs, the **DAG Engine** coordinates step sequencing:

1. Root tasks are enqueued immediately
2. On each step completion, DAG Engine checks if downstream tasks are ready
3. Ready tasks are enqueued to their respective capability queues
4. DAG is complete when all leaf tasks succeed (or any task fails, depending on policy)

## State Storage

| Data | Storage | TTL |
|------|---------|-----|
| Task records | MySQL | permanent |
| Pending queues | Redis List | — |
| Running task locks | Redis String | 2× timeout |
| Result cache | Redis Hash | configurable |
| Metrics | Prometheus | scrape interval |
