from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable


ProgressCallback = Callable[[dict[str, Any]], None]


def emit_progress(
    progress_callback: ProgressCallback | None,
    *,
    stage: str,
    event: str,
    **payload: Any,
) -> None:
    if progress_callback is None:
        return
    progress_callback(
        {
            "stage": stage,
            "event": event,
            **payload,
        }
    )


@dataclass
class ThrottledProgressReporter:
    progress_callback: ProgressCallback | None
    stage: str
    min_interval_seconds: float = 2.0
    _last_report_time: float = field(default_factory=lambda: 0.0)

    def report(self, *, event: str, force: bool = False, **payload: Any) -> None:
        if self.progress_callback is None:
            return

        now = time.monotonic()
        if not force and self._last_report_time and (now - self._last_report_time) < self.min_interval_seconds:
            return

        emit_progress(
            self.progress_callback,
            stage=self.stage,
            event=event,
            **payload,
        )
        self._last_report_time = now
