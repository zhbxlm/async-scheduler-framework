from async_scheduler.scheduler.cron import CronScheduler


class DummyQueueManager:
    async def enqueue(self, task):
        return None


def test_cron_scheduler_initial_metrics():
    scheduler = CronScheduler(queue_manager=DummyQueueManager(), poll_interval=1.0)
    assert scheduler._metrics["poll_iterations"] == 0
    assert scheduler._metrics["leader_acquired_count"] == 0
    assert scheduler._metrics["schedule_processed_count"] == 0
