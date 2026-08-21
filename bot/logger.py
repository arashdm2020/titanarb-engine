"""Rich terminal logging setup for the arbitrage bot.

Uses the `rich` library for colored, human-readable console output instead of
JSON lines. Sensitive material (private keys, seed phrases) is still redacted.
"""
from __future__ import annotations

import logging
import re
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

# Operational rows carry a few compact key/value fields.  A fixed wide console
# preserves the intended one-event-per-row layout when the process is run in a
# narrow redirected terminal.
_console = Console(stderr=False, width=200)

_SECRET_PATTERNS = [
    re.compile(r"0x[a-fA-F0-9]{64}"),
    re.compile(r"(?:https?|wss)://[^\s'\"]+", re.IGNORECASE),
]

_SECRET_KEYS = {
    "private_key", "privatekey", "seed", "mnemonic", "raw_signed_tx", "signed_tx",
    "rpc_url", "ws_rpc_url", "api_key", "authorization",
}
_SECRET_FIELD_FRAGMENTS = ("private", "secret", "password", "rpc", "url", "api_key")
_DETAILED_GAS_FIELDS = {
    "gas_asset", "l1_data_fee", "l2_execution_fee", "gas_components", "calldata_fee",
}


def redact_secrets(value: Any) -> Any:
    if isinstance(value, str):
        redacted = value
        for pattern in _SECRET_PATTERNS:
            redacted = pattern.sub("[REDACTED]", redacted)
        return redacted
    if isinstance(value, dict):
        return {key: redact_secrets(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(redact_secrets(item) for item in value)
    return value


def _format_value(key: str, value: Any) -> str:
    lowered = key.lower()
    if lowered in _SECRET_KEYS or any(fragment in lowered for fragment in _SECRET_FIELD_FRAGMENTS):
        return "[REDACTED]"
    return str(redact_secrets(value))


def _format_amount(key: str, value: Any) -> str:
    """Format large integer token amounts into human-readable form."""
    if not isinstance(value, (int, float)):
        return _format_value(key, value)
    if isinstance(value, float):
        return f"{value:,.4f}"
    if abs(value) >= 1_000_000_000_000_000:
        return f"{value:,.0f}"
    if abs(value) >= 1_000_000_000:
        return f"{value:,.0f}"
    return f"{value:,}"


class RichLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return record.getMessage()


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = RichHandler(
            console=_console,
            show_time=True,
            show_level=True,
            show_path=False,
            markup=True,
            rich_tracebacks=True,
        )
        handler.setFormatter(RichLogFormatter())
        logger.addHandler(handler)
    logger.setLevel(level.upper())
    logger.propagate = False
    return logger


def log_fields(logger: logging.Logger, level: int, message: str, **fields: Any) -> None:
    """Render one compact, credential-safe Rich log line per operational event."""
    level_style = {
        logging.DEBUG: "dim",
        logging.INFO: "cyan",
        logging.WARNING: "yellow",
        logging.ERROR: "bold red",
        logging.CRITICAL: "bold red on white",
    }

    # Color the event marker, retaining one horizontal row for high-volume loops.
    style = level_style.get(level, "white")
    upper_message = message.upper()
    if "TX SENT" in upper_message or "LIVE TRADE" in upper_message:
        style = "bold magenta"
    elif "PROFIT" in upper_message:
        style = "bold green"
    elif "ERROR" in upper_message or level >= logging.ERROR:
        style = "bold red"

    # Check for profit-related fields for special coloring
    net_profit = fields.get("net_profit")
    if net_profit is not None and isinstance(net_profit, (int, float)):
        if net_profit > 0:
            style = "bold green"
        elif net_profit < 0:
            style = "bold red"

    rendered_fields = []
    for key, value in fields.items():
        if level > logging.DEBUG and key in _DETAILED_GAS_FIELDS:
            continue
        formatted = _format_amount(key, value)
        if key in {"net_profit", "expected_net_profit", "realized_profit"} and isinstance(value, (int, float)):
            if value > 0:
                formatted = f"[bold green]{formatted}[/bold green]"
            elif value < 0:
                formatted = f"[bold red]{formatted}[/bold red]"
        rendered_fields.append(f"{key}={formatted}")

    suffix = f" [dim]| {' | '.join(rendered_fields)}[/dim]" if rendered_fields else ""
    logger.log(level, f"[{style}]{message}[/{style}]{suffix}", extra={"rich": True})
