#!/usr/bin/env python3
"""
Load test script for Async Scheduler.
Simulates task creation at specified rate to test system performance.
"""
import asyncio
import httpx
import time
import random
import sys
from typing import List
import statistics
import argparse


class LoadTester:
    def __init__(self, base_url: str, rps: float = 10, duration: int = 300):
        self.base_url = base_url.rstrip('/')
        self.rps = rps
        self.duration = duration
        self.client = httpx.AsyncClient(timeout=30.0)
        self.latencies: List[float] = []
        self.errors = 0
        self.total_requests = 0
        self.start_time = 0
        
    async def create_task(self, task_id: str) -> bool:
        """Create a test task."""
        payload = {
            "task_id": task_id,
            "capability": random.choice(["image_generate", "data_process", "report_generate"]),
            "payload": {"test": True, "data": f"test_data_{random.randint(1, 1000)}"},
            "priority": random.choice(["normal", "high"]),
            "callback_url": "http://test-receiver:8080/callback"
        }
        
        start = time.time()
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/tasks",
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            latency = time.time() - start
            
            if response.status_code == 200:
                self.latencies.append(latency)
                return True
            else:
                print(f"  Request failed: {response.status_code} - {response.text}")
                self.errors += 1
                return False
                
        except Exception as e:
            print(f"  Request error: {e}")
            self.errors += 1
            return False
    
    async def worker(self, worker_id: int):
        """Worker coroutine that generates requests at specified rate."""
        interval = 1.0 / self.rps
        task_count = 0
        
        while time.time() - self.start_time < self.duration:
            task_id = f"loadtest_{worker_id}_{task_count}_{int(time.time())}"
            success = await self.create_task(task_id)
            self.total_requests += 1
            task_count += 1
            
            if success and task_count % 50 == 0:
                print(f"[Worker {worker_id}] Created {task_count} tasks...")
            
            # Sleep to maintain target RPS
            await asyncio.sleep(interval)
    
    async def monitor_health(self):
        """Monitor system health during load test."""
        while time.time() - self.start_time < self.duration:
            try:
                response = await self.client.get(f"{self.base_url}/api/v1/health")
                if response.status_code == 200:
                    health = response.json()
                    print(f"[Health] Status: {health.get('status', 'unknown')}, "
                          f"Redis: {health.get('redis', {}).get('status', 'unknown')}, "
                          f"MySQL: {health.get('mysql', {}).get('status', 'unknown')}")
            except Exception as e:
                print(f"[Health] Error: {e}")
            
            await asyncio.sleep(10)
    
    async def run(self, workers: int = 1):
        """Run the load test."""
        print(f"🚀 Starting load test...")
        print(f"  Target: {self.base_url}")
        print(f"  RPS: {self.rps} requests per second")
        print(f"  Duration: {self.duration} seconds")
        print(f"  Workers: {workers}")
        
        self.start_time = time.time()
        
        # Create worker tasks
        worker_tasks = [asyncio.create_task(self.worker(i)) for i in range(workers)]
        monitor_task = asyncio.create_task(self.monitor_health())
        
        # Wait for duration
        await asyncio.sleep(self.duration)
        
        # Cancel workers
        for task in worker_tasks:
            task.cancel()
        monitor_task.cancel()
        
        # Wait for cancellation
        try:
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            await monitor_task
        except asyncio.CancelledError:
            pass
        
        # Print results
        self.print_results()
        
        await self.client.aclose()
    
    def print_results(self):
        """Print load test results."""
        elapsed = time.time() - self.start_time
        actual_rps = self.total_requests / elapsed
        
        print("\n" + "="*60)
        print("📊 LOAD TEST RESULTS")
        print("="*60)
        print(f"Duration:          {elapsed:.1f}s")
        print(f"Total requests:    {self.total_requests}")
        print(f"Successful:        {self.total_requests - self.errors}")
        print(f"Errors:            {self.errors}")
        print(f"Target RPS:        {self.rps}")
        print(f"Actual RPS:        {actual_rps:.2f}")
        print(f"Error rate:        {(self.errors / self.total_requests * 100):.2f}%")
        
        if self.latencies:
            print(f"\nLatency (seconds):")
            print(f"  Min:            {min(self.latencies):.4f}")
            print(f"  Max:            {max(self.latencies):.4f}")
            print(f"  Average:        {statistics.mean(self.latencies):.4f}")
            print(f"  Median:         {statistics.median(self.latencies):.4f}")
            print(f"  P90:            {sorted(self.latencies)[int(len(self.latencies) * 0.9)]:.4f}")
            print(f"  P95:            {sorted(self.latencies)[int(len(self.latencies) * 0.95)]:.4f}")
            print(f"  P99:            {sorted(self.latencies)[int(len(self.latencies) * 0.99)]:.4f}")
        
        print("="*60)


async def main():
    parser = argparse.ArgumentParser(description="Load test for Async Scheduler")
    parser.add_argument("--url", default="http://localhost:8001", help="Task API URL")
    parser.add_argument("--rps", type=float, default=10.0, help="Requests per second")
    parser.add_argument("--duration", type=int, default=300, help="Duration in seconds")
    parser.add_argument("--workers", type=int, default=1, help="Number of worker threads")
    
    args = parser.parse_args()
    
    tester = LoadTester(args.url, args.rps, args.duration)
    
    try:
        await tester.run(args.workers)
    except KeyboardInterrupt:
        print("\n⚠️  Load test interrupted by user")
    except Exception as e:
        print(f"❌ Load test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())