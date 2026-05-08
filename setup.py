"""Compatibility shim for setuptools-based tooling.

Canonical project metadata lives in ``pyproject.toml``.
Keep this file minimal to avoid drift between duplicated dependency/version
sources while preserving compatibility with older tooling.
"""

from setuptools import setup


setup()
