"""Arbitrum One configuration package.

Provides the Arbitrum-native Settings dataclass and loader, replacing the
legacy Polygon-specific ``bot/config_legacy_polygon.py`` for all new development.
"""
from __future__ import annotations

from .settings import (
    ArbitrumAddresses,
    ConfigError,
    Settings,
    TokenConfig,
    load_arbitrum_config,
    load_settings,
)

__all__ = [
    "ArbitrumAddresses",
    "ConfigError",
    "Settings",
    "TokenConfig",
    "load_arbitrum_config",
    "load_settings",
]
