# async-scheduler-sdk

Python client SDK for [async-scheduler](https://github.com/zhbxlm/async-scheduler-framework).

**No framework internals required.** Just `httpx` and `pydantic`.

## Install

```bash
pip install async-scheduler-sdk
```

## Usage

```python
from scheduler_sdk import SchedulerClient

async with SchedulerClient("http://scheduler:8000", api_key="secret") as client:
    # Submit and wait
    result = await client.submit_and_wait("my_dag", {"x": 1})
    print(result["output_data"])

    # Or submit then poll manually
    task = await client.submit_task("my_dag", {"x": 1})
    result = await client.wait_for_task(task["task_id"])
```

## CLI

```bash
# Submit a task
scheduler --url http://scheduler:8000 submit my_dag --input '{"x":1}' --wait

# Get task status
scheduler get <task_id>

# Cancel
scheduler cancel <task_id>
```
