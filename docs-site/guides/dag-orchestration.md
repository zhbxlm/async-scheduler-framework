# DAG Orchestration

DAGs define multi-step workflows with explicit dependencies.

See the [Architecture Overview](../architecture/overview.md) for the execution model.

## Register a DAG

```bash
scheduler dag register --file my-dag.yaml
```

## Example DAG Definition

```yaml
id: train_pipeline
steps:
  - id: preprocess
    capability: data_pipeline
  - id: train
    capability: gpu_training
    depends_on: [preprocess]
  - id: evaluate
    capability: model_eval
    depends_on: [train]
```
