"""Package setup for ray-amu."""
from setuptools import setup, find_packages

setup(
    name="ray-amu",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "fastapi>=0.104.0",
        "uvicorn[standard]>=0.24.0",
        "sqlalchemy>=2.0.0",
        "redis>=5.0.0",
        "httpx>=0.25.0",
        "pydantic>=2.5.0",
        "click>=8.1.0",
        "pyyaml>=6.0",
        "croniter>=2.0.0",
    ],
    entry_points={
        "console_scripts": [
            "ray-amu=src.cli.main:cli",
        ],
    },
    python_requires=">=3.10",
)
