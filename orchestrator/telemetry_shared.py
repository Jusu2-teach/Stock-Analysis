from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .models import MethodRegistration
from .telemetry import OrchestratorObserver


@dataclass(frozen=True)
class SharedEventBusObserver(OrchestratorObserver):
    bus: object

    @classmethod
    def default(cls) -> "SharedEventBusObserver":
        from shared import EventBus

        return cls(bus=EventBus.get())

    def on_method_registered(self, reg: MethodRegistration, *, source: str) -> None:
        from shared import MethodRegisteredEvent

        self.bus.emit(
            MethodRegisteredEvent(
                component=reg.component_type,
                method=reg.engine_name,
                engine_type=reg.engine_type,
                engine_name=reg.engine_name,
                version=reg.version,
                priority=reg.priority,
                full_key=reg.full_key,
                source=source,
            )
        )

    def on_registry_refreshed(self, *, mode: str, method_count: int, source: str) -> None:
        from shared import RegistryRefreshedEvent

        self.bus.emit(
            RegistryRefreshedEvent(
                mode=mode,
                method_count=method_count,
                source=source,
            )
        )

    def on_method_executed(
        self,
        reg: MethodRegistration,
        *,
        duration_ms: float,
        success: bool,
        error: Optional[str],
        source: str,
    ) -> None:
        from shared import MethodExecutedEvent

        self.bus.emit(
            MethodExecutedEvent(
                component=reg.component_type,
                method=reg.engine_name,
                engine=reg.engine_type,
                duration_ms=duration_ms,
                success=success,
                error=error,
                source=source,
            )
        )
