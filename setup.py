"""Package setup for ray-async — aligned with src/ layout.

Note: User-facing components (agent, proxy, worker) are now available as
independent packages in the packages/ directory:
  - async-agent: Node Agent for cluster node management
  - async-proxy: Async Proxy for long-running services
  - async-worker: Worker SDK for building Ray workers
"""
from setuptools import setup, find_packages

setup(
    name="ray-async",
    version="0.1.0",
    packages=find_packages(include=["src*", "config*"]),
    install_requires=[
        "fastapi>=0.104.0",
        "uvicorn[standard]>=0.24.0",
        "sqlalchemy>=2.0.0",
        "pymysql>=1.1.0",
        "pydantic>=2.5.0",
        "redis>=5.0.0",
        "httpx>=0.25.0",
        "click>=8.1.0",
        "PyYAML>=6.0.0",
        "croniter>=2.0.0",
        "python-dotenv>=1.0.0",
    ],
    extras_require={
        "ray": ["ray[default]>=2.9.0"],
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "fakeredis>=2.23.0",
        ],
        # User-facing packages are now available as independent packages:
        #   pip install ../packages/node-agent    # or async-agent
        #   pip install ../packages/async-proxy   # or async-proxy
        #   pip install ../packages/worker-sdk    # or async-worker
    },
    entry_points={
        "console_scripts": [
            "ray-async=src.cli.main:cli",
        ],
    },
    python_requires=">=3.10",
)
