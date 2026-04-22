"""Dataset-specific record loaders with schema normalization."""

from .berkeley_loader import BerkeleyLoader
from .criteo_loader import CriteoLoader
from .fraud_loader import FraudLoader
from .unsw_loader import UNSWLoader

__all__ = ["FraudLoader", "CriteoLoader", "UNSWLoader", "BerkeleyLoader"]
