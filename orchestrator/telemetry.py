from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Protocol

from .models import MethodRegistration


class OrchestratorObserver(Protocol):
    def on_method_registered(self, reg: MethodRegistration, *, source: str) -> None:
        ...

    def on_registry_refreshed(self, *, mode: str, method_count: int, source: str) -> None:
        ...

    def on_method_executed(
        self,
        reg: MethodRegistration,
        *,
        duration_ms: float,
        success: bool,
        error: Optional[str],
        source: str,
    ) -> None:
        ...


@dataclass(frozen=True)
class NullObserver:
    def on_method_registered(self, reg: MethodRegistration, *, source: str) -> None:
        return

    def on_registry_refreshed(self, *, mode: str, method_count: int, source: str) -> None:
        return

    def on_method_executed(
        self,
        reg: MethodRegistration,
        *,
        duration_ms: float,
        success: bool,
        error: Optional[str],
        source: str,
    ) -> None:
        return


@dataclass(frozen=True)
class CompositeObserver:
    observers: tuple[OrchestratorObserver, ...]

    @classmethod
    def from_iterable(cls, observers: Iterable[OrchestratorObserver]) -> "CompositeObserver":
        return cls(tuple(observers))

    def on_method_registered(self, reg: MethodRegistration, *, source: str) -> None:
        for o in self.observers:
            o.on_method_registered(reg, source=source)

    def on_registry_refreshed(self, *, mode: str, method_count: int, source: str) -> None:
        for o in self.observers:
            o.on_registry_refreshed(mode=mode, method_count=method_count, source=source)

    def on_method_executed(
        self,
        reg: MethodRegistration,
        *,
        duration_ms: float,
        success: bool,
        error: Optional[str],
        source: str,
    ) -> None:
        for o in self.observers:
            o.on_method_executed(
                reg,
                duration_ms=duration_ms,
                success=success,
                error=error,
                source=source,
            )


def default_observer() -> OrchestratorObserver:
    """Default observer for the orchestrator.

    Goal:
    - Keep backward compatible behavior in this repo (emit to shared.EventBus)
    - Avoid hard dependency inside orchestrator core modules

    If shared EventBus is unavailable for any reason, falls back to NullObserver.
    """
    try:
        from .telemetry_shared import SharedEventBusObserver

        return SharedEventBusObserver.default()
    except Exception:
        return NullObserver()
