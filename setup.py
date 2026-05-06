"""Package setup for ray-async — aligned with src/ layout.

All components (agent, proxy, worker) are now integrated into the main package.
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
        "orjson>=3.9.0",
        "psutil>=5.9.0",
        "requests>=2.28.0",
    ],
    extras_require={
        "ray": ["ray[default]>=2.9.0"],
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "fakeredis>=2.23.0",
            "ruff>=0.1.0",
            "mypy>=1.6.0",
        ],
        # All components are now integrated into the main package
    },
    entry_points={
        "console_scripts": [
            "ray-async=src.cli.main:cli",
        ],
    },
    python_requires=">=3.10",
)
