"""Helpers for optional imports.

These utilities allow scripts to run both as part of the ``restaurants``
package and as standalone modules without adjusting ``PYTHONPATH``.
"""

from importlib import import_module
from types import ModuleType
from typing import Any, Tuple


def optional_import(module_name: str) -> ModuleType:
    """Import ``restaurants.module_name`` if available, else ``module_name``.

    This lets scripts work whether executed within the package or from the
    repository root.
    """

    try:
        return import_module(f"restaurants.{module_name}")
    except ImportError:
        return import_module(module_name)


def optional_from(module_name: str, *names: str) -> Tuple[Any, ...]:
    """Return attributes from a module imported via :func:`optional_import`."""

    module = optional_import(module_name)
    return tuple(getattr(module, name) for name in names)
