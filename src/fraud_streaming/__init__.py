"""Synthetic event-time fraud-streaming reference implementation."""

from .config import PipelineConfig
from .models import FraudAlert, TransactionEvent
from .pipeline import FraudPipeline
from .simulator import SimulationConfig, TransactionSimulator

__all__ = [
    "FraudAlert",
    "FraudPipeline",
    "PipelineConfig",
    "SimulationConfig",
    "TransactionEvent",
    "TransactionSimulator",
]

__version__ = "0.1.0"

