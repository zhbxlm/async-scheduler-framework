"""scheduler_agent.cli — command-line entry point."""
from __future__ import annotations

import asyncio
import logging

import click


@click.group()
@click.option("--log-level", default="INFO", envvar="LOG_LEVEL",
              type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]))
def cli(log_level: str) -> None:
    """async-scheduler Node Agent."""
    logging.basicConfig(level=getattr(logging, log_level),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")


@cli.command()
@click.option("--scheduler-url", default="http://localhost:8000",
              envvar="SCHEDULER_URL", help="Scheduler API URL")
@click.option("--api-key", default="", envvar="SCHEDULER_API_KEY")
@click.option("--capabilities", default="", envvar="AGENT_CAPABILITIES",
              help="Comma-separated capability names this node handles")
@click.option("--node-id", default="", envvar="AGENT_NODE_ID")
@click.option("--max-concurrent", default=8, type=int, envvar="AGENT_MAX_CONCURRENT")
@click.option("--heartbeat-interval", default=30.0, type=float, envvar="AGENT_HEARTBEAT_INTERVAL")
def start(
    scheduler_url: str,
    api_key: str,
    capabilities: str,
    node_id: str,
    max_concurrent: int,
    heartbeat_interval: float,
) -> None:
    """Start the node agent."""
    from scheduler_agent.node import NodeAgent

    cap_list = [c.strip() for c in capabilities.split(",") if c.strip()]
    agent = NodeAgent(
        scheduler_url=scheduler_url,
        api_key=api_key,
        capabilities=cap_list,
        node_id=node_id,
        max_concurrent_tasks=max_concurrent,
        heartbeat_interval=heartbeat_interval,
    )
    click.echo(f"Starting agent → {scheduler_url}  capabilities={cap_list}")
    asyncio.run(agent.start())


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
