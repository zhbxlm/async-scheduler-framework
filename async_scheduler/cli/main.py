"""CLI main entry point."""

import asyncio
import logging
import os
import sys

import click
from uvicorn import Config, Server

from async_scheduler.api import app
from async_scheduler.core.consumer import TaskConsumer
from async_scheduler.core.models import TaskCreate, TaskPriority
from async_scheduler.dag import DAGEngine
from async_scheduler.executor import TaskExecutor, default_task_handler
from async_scheduler.persistence import init_db
from async_scheduler.queue import QueueManager
from async_scheduler.observability import configure_logging
from async_scheduler.scheduler import CronScheduler
from async_scheduler.worker import WorkerPool, create_default_workers

# Configure structured JSON logging (respects LOG_LEVEL / LOG_FORMAT env vars)
configure_logging(
    service=os.environ.get("SERVICE_NAME", "async-scheduler"),
    node_id=os.environ.get("NODE_ID"),
    version=os.environ.get("SERVICE_VERSION"),
)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    """Async Scheduler CLI - Manage tasks, schedules, and DAGs."""
    pass


@cli.command()
@click.option("--host", default="127.0.0.1", help="Host to bind to")
@click.option("--port", default=8000, type=int, help="Port to bind to")
@click.option("--reload", is_flag=True, help="Enable auto-reload")
@click.option("--init-db", is_flag=True, help="Initialize database on startup")
def api(host, port, reload, init_db):
    """Start the API server."""
    if init_db:
        asyncio.run(_init_database())

    click.echo(f"Starting API server on {host}:{port}")
    config = Config(app=app, host=host, port=port, reload=reload)
    server = Server(config)

    try:
        asyncio.run(server.serve())
    except KeyboardInterrupt:
        click.echo("\nShutting down API server")


@cli.command()
@click.option("--workers", default=2, type=int, help="Number of workers per type")
@click.option("--max-concurrent", default=10, type=int, help="Max concurrent tasks")
@click.option("--init-db", is_flag=True, help="Initialize database on startup")
def worker(workers, max_concurrent, init_db):
    """Start a worker process."""
    if init_db:
        asyncio.run(_init_database())

    click.echo(f"Starting worker with {workers} workers per type, max {max_concurrent} concurrent tasks")

    async def run_worker():
        # Initialize components
        queue_manager = QueueManager()
        executor = TaskExecutor()
        dag_engine = DAGEngine()

        # Create workers
        worker_list = create_default_workers(queue_manager, executor, num_workers=workers)

        # Create worker pool
        pool = WorkerPool(worker_list)

        # Create task consumer
        consumer = TaskConsumer(
            queue_manager=queue_manager,
            executor=executor,
            handler=default_task_handler,
            max_concurrent_tasks=max_concurrent,
            poll_interval=1.0,
        )

        # Create cron scheduler
        scheduler = CronScheduler(
            queue_manager=queue_manager,
            poll_interval=60.0,
        )

        # Start everything
        await pool.start()
        await consumer.start()
        await scheduler.start()

        logger.info("Worker started successfully")

        # Keep running
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("Shutting down worker")
        finally:
            await consumer.stop()
            await scheduler.stop()
            await pool.stop()

    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        click.echo("\nShutting down worker")


@cli.command()
@click.option("--init-db", is_flag=True, help="Initialize database on startup")
def dev(init_db):
    """Start development environment (API + Worker)."""
    if init_db:
        asyncio.run(_init_database())

    click.echo("Starting development environment...")

    async def run_dev():
        # Initialize components
        queue_manager = QueueManager()
        executor = TaskExecutor()
        dag_engine = DAGEngine()

        # Create workers
        worker_list = create_default_workers(queue_manager, executor, num_workers=1)

        # Create worker pool
        pool = WorkerPool(worker_list)

        # Create task consumer
        consumer = TaskConsumer(
            queue_manager=queue_manager,
            executor=executor,
            handler=default_task_handler,
            max_concurrent_tasks=5,
            poll_interval=1.0,
        )

        # Create cron scheduler
        scheduler = CronScheduler(
            queue_manager=queue_manager,
            poll_interval=60.0,
        )

        # Start worker components
        await pool.start()
        await consumer.start()
        await scheduler.start()

        # Start API server
        config = Config(app=app, host="127.0.0.1", port=8000)
        server = Server(config)

        logger.info("Development environment started successfully")
        click.echo("Development environment running at http://127.0.0.1:8000")
        click.echo("API docs available at http://127.0.0.1:8000/docs")

        # Run server in background
        api_task = asyncio.create_task(server.serve())

        try:
            await api_task
        except asyncio.CancelledError:
            logger.info("Shutting down development environment")
        finally:
            await consumer.stop()
            await scheduler.stop()
            await pool.stop()

    try:
        asyncio.run(run_dev())
    except KeyboardInterrupt:
        click.echo("\nShutting down development environment")


@cli.command()
@click.argument("name")
@click.option("--priority", default="normal", type=click.Choice(["low", "normal", "high", "critical"]))
@click.option("--payload", default="{}", help="JSON payload for the task")
def task(name, priority, payload):
    """Create a task directly from CLI."""
    import json

    try:
        payload_dict = json.loads(payload)
    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON payload")
        sys.exit(1)

    async def create_task():
        from async_scheduler.persistence import get_session, TaskRepository
        from async_scheduler.queue import QueueManager

        queue_manager = QueueManager()

        task_create = TaskCreate(
            name=name,
            payload=payload_dict,
            priority=TaskPriority[priority.upper()],
        )

        async with get_session() as session:
            task = await TaskRepository.create(session, task_create)

        await queue_manager.enqueue(task)

        click.echo(f"Task created: {task.id}")
        click.echo(f"Name: {task.name}")
        click.echo(f"Priority: {task.priority.value}")
        click.echo(f"Status: {task.status.value}")

    try:
        asyncio.run(create_task())
    except Exception as e:
        click.echo(f"Error creating task: {e}")
        sys.exit(1)


@cli.command()
@click.argument("name")
@click.argument("cron_expression")
@click.option("--payload", default="{}", help="JSON template payload for the schedule")
def schedule(name, cron_expression, payload):
    """Create a cron schedule directly from CLI."""
    import json

    try:
        payload_dict = json.loads(payload)
    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON payload")
        sys.exit(1)

    async def create_schedule():
        from async_scheduler.persistence import get_session, ScheduleRepository

        from async_scheduler.core.models import ScheduleCreate

        schedule_create = ScheduleCreate(
            name=name,
            cron_expression=cron_expression,
            task_template=payload_dict,
        )

        async with get_session() as session:
            schedule = await ScheduleRepository.create(session, schedule_create)

        click.echo(f"Schedule created: {schedule.id}")
        click.echo(f"Name: {schedule.name}")
        click.echo(f"Cron: {schedule.cron_expression}")
        click.echo(f"Status: {schedule.status.value}")

    try:
        asyncio.run(create_schedule())
    except Exception as e:
        click.echo(f"Error creating schedule: {e}")
        sys.exit(1)


@cli.command()
@click.option("--force", is_flag=True, help="Drop existing tables first")
def init_db_cmd(force):
    """Initialize the database."""
    if force:
        click.echo("Dropping existing database...")
        asyncio.run(_drop_database())

    click.echo("Initializing database...")
    asyncio.run(_init_database())
    click.echo("Database initialized successfully")


async def _init_database():
    """Initialize database."""
    # Ensure data directory exists
    data_dir = os.path.dirname("/home/gem/.openclaw/workspace/projects/async-scheduler-framework/data/scheduler.db")
    os.makedirs(data_dir, exist_ok=True)

    await init_db()


async def _drop_database():
    """Drop database tables."""
    from async_scheduler.persistence import drop_db

    await drop_db()


@cli.command()
def status():
    """Show system status."""
    from async_scheduler.persistence import get_session, TaskRepository
    from async_scheduler.core.models import TaskStatus

    async def show_status():
        async with get_session() as session:
            # Get task counts by status
            total = len(await TaskRepository.list_all(session, limit=10000))
            pending = len(await TaskRepository.list_all(session, status=TaskStatus.PENDING, limit=10000))
            running = len(await TaskRepository.list_all(session, status=TaskStatus.RUNNING, limit=10000))
            success = len(await TaskRepository.list_all(session, status=TaskStatus.SUCCESS, limit=10000))
            failed = len(await TaskRepository.list_all(session, status=TaskStatus.FAILED, limit=10000))

        click.echo("=== Async Scheduler Status ===")
        click.echo(f"Total Tasks: {total}")
        click.echo(f"  Pending: {pending}")
        click.echo(f"  Running: {running}")
        click.echo(f"  Success: {success}")
        click.echo(f"  Failed: {failed}")

    try:
        asyncio.run(show_status())
    except Exception as e:
        click.echo(f"Error getting status: {e}")


@cli.command("reconcile")
def reconcile_cmd():
    """Run the local task reconciler once."""

    async def _run():
        from async_scheduler.platform import TaskReconciler

        reconciler = TaskReconciler()
        repaired = await reconciler.reconcile()
        click.echo(f"Reconciler repaired: {repaired}")

    try:
        asyncio.run(_run())
    except Exception as e:
        click.echo(f"Error running reconciler: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
