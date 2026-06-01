"""
farm_universe.py

Central source of truth for stable pipeline status enums,
deterministic wind farm contracts, and cluster mappings.
"""

from enum import Enum

class RawSourceStatus(str, Enum):
    MISSING = "missing"
    EXISTS = "exists"
    EMPTY = "empty"

class FilteredAisStatus(str, Enum):
    MISSING = "missing"
    EXISTS = "exists"
    EMPTY = "empty"
    INTERRUPTED = "interrupted"

class VisitsStatus(str, Enum):
    MISSING = "missing"
    EXISTS = "exists"
    EMPTY = "empty"

class DwellsStatus(str, Enum):
    MISSING = "missing"
    EXISTS = "exists"
    EMPTY = "empty"

class DuplicateStatus(str, Enum):
    MISSING = "missing"
    CALCULATED = "calculated"

class WeatherJoinStatus(str, Enum):
    MISSING = "missing"
    JOINED = "joined"

class QaMapStatus(str, Enum):
    MISSING = "missing"
    EXISTS = "exists"

class ValidationStatus(str, Enum):
    MISSING = "missing"
    EXISTS = "exists"

# Deterministic universe of wind farms of interest
FARM_UNIVERSE = {
    "Wikinger": "Wikinger",
    "Arkona-Becken_Südost": "Arkona-Becken Südost",
    "Baltic_Eagle": "Baltic Eagle",
    "EnBW_Windpark_Baltic_1": "EnBW Windpark Baltic 1",
    "EnBW_Windpark_Baltic_2": "EnBW Windpark Baltic 2",
    "Alpha_Ventus": "Alpha Ventus",
    "Amrumbank_West": "Amrumbank West",
    "Nordsee_Ost": "Nordsee Ost",
    "Borkum_Riffgrund_1": "Borkum Riffgrund 1"
}

# The target Baltic Sea cluster wind farms
BALTIC_CLUSTER_FARMS = {
    "Wikinger", "Arkona-Becken_Südost", "Baltic_Eagle",
    "EnBW_Windpark_Baltic_1", "EnBW_Windpark_Baltic_2"
}
