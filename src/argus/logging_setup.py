"""structlog configuration: JSON lines to the data root + readable console."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import structlog

from argus.core.clocks import utc_now
from argus.settings import Settings

_CONFIGURED = False


def configure(settings: Settings) -> structlog.stdlib.BoundLogger:
    """Idempotent logging setup; returns the root ARGUS logger."""
    global _CONFIGURED
    if not _CONFIGURED:
        settings.ensure_dirs()
        logfile: Path = settings.log_dir / f"argus-{utc_now():%Y%m%d}.jsonl"

        shared_processors: list[structlog.typing.Processor] = [
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
        ]
        structlog.configure(
            processors=[
                *shared_processors,
                structlog.processors.format_exc_info,
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

        file_handler = logging.FileHandler(logfile, encoding="utf-8")
        file_handler.setFormatter(
            structlog.stdlib.ProcessorFormatter(
                processor=structlog.processors.JSONRenderer(),
                foreign_pre_chain=shared_processors,
            )
        )
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(
            structlog.stdlib.ProcessorFormatter(
                processor=structlog.dev.ConsoleRenderer(colors=False),
                foreign_pre_chain=shared_processors,
            )
        )

        root = logging.getLogger("argus")
        root.handlers.clear()
        root.addHandler(file_handler)
        root.addHandler(console_handler)
        root.setLevel(logging.INFO)
        root.propagate = False
        _CONFIGURED = True

    return structlog.stdlib.get_logger("argus")
