"""
metric_specs.py

Defines which minimal facts we extract from SEC companyfacts for Silver.
"""

METRIC_SPECS = {
    "CFO": {
        "namespace": "us-gaap",
        "tags": ["NetCashProvidedByUsedInOperatingActivities"],
        "unit": "USD",
        "is_ytd": True,
        "abs": False,
    },
    "CAPEX": {
        "namespace": "us-gaap",
        "tags": [
            "PaymentsToAcquirePropertyPlantAndEquipment",
            "CapitalExpenditures",
        ],
        "unit": "USD",
        "is_ytd": True,
        "abs": True,
    },
    "SHARES": {
        "namespace": "us-gaap",
        "tags": [
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            "WeightedAverageNumberOfSharesOutstandingDiluted",
        ],
        "unit": "shares",
        "is_ytd": False,
        "abs": False,
    },
}
