"""Synthetic fallback generators used when real datasets are unavailable."""

from .generators import (
    generate_synthetic_berkeley,
    generate_synthetic_criteo,
    generate_synthetic_fraud,
    generate_synthetic_unsw,
)

__all__ = [
    "generate_synthetic_fraud",
    "generate_synthetic_criteo",
    "generate_synthetic_unsw",
    "generate_synthetic_berkeley",
]
