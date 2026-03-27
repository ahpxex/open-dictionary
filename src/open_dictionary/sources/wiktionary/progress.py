"""Progress helpers for byte-oriented Wiktionary snapshot operations."""

from __future__ import annotations

import sys
import time


class ByteProgressPrinter:
    """Emit coarse progress updates for large streaming transfers."""

    def __init__(
        self,
        label: str,
        total_bytes: int,
        *,
        min_bytes_step: int = 64 * 1024 * 1024,
        min_time_step: float = 5.0,
    ) -> None:
        self.label = label
        self.total_bytes = max(total_bytes, 0)
        self.min_bytes_step = max(min_bytes_step, 1)
        self.min_time_step = max(min_time_step, 0.0)
        self._last_report_time = time.monotonic()
        self._last_report_bytes = 0

    def report(self, processed_bytes: int, *, force: bool = False) -> None:
        if processed_bytes < 0:
            return

        now = time.monotonic()
        bytes_increment = processed_bytes - self._last_report_bytes

        if not force and processed_bytes < self.total_bytes:
            if (
                bytes_increment < self.min_bytes_step
                and (now - self._last_report_time) < self.min_time_step
            ):
                return
        elif not force and bytes_increment <= 0:
            return

        percent_text = ""
        if self.total_bytes:
            percent = min(100.0, (processed_bytes / self.total_bytes) * 100)
            percent_text = f"{percent:5.1f}% | "

        gib_processed = processed_bytes / (1024**3)
        print(
            f"{self.label}: {percent_text}{gib_processed:.2f} GiB",
            file=sys.stderr,
            flush=True,
        )

        self._last_report_time = now
        self._last_report_bytes = processed_bytes

    def finalize(self, processed_bytes: int) -> None:
        if processed_bytes == 0:
            return
        self.report(processed_bytes, force=True)


__all__ = ["ByteProgressPrinter"]
