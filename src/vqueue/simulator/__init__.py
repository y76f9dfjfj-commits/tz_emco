"""Генератор телеметрии: детерминированная симуляция площадки и сбои доставки."""

from vqueue.simulator.engine import Simulation
from vqueue.simulator.faults import FaultInjector

__all__ = ["FaultInjector", "Simulation"]
