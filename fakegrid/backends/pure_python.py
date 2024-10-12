"""A pure python implementation of a shotgrid compatible query engine.

This will be slower than sql_alchemy, but useful if you can't install that package!
"""

from .base import BaseBackend


class PurePythonBackend(BaseBackend):
    """An in-memory pure python implementation of a shotgun query backend."""
