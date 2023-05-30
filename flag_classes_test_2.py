from dataclasses import dataclass
from typing import Any


class SoilMoistureFlag:
    """Small class around soil moisture flags contained in the ISMN database"""

    def __init__(self, kind):
        self.__meaning_dict = {
            "G": "Good",
            "M": "Parameter value missing",
            "C01": "soil moisture < 0.0 m^3/m^3",
            "C02": "soil moisture > 0.6 m^3/m^3",
            "C03": "soil moisture > saturation point (derived from HWSD parameter values)",
            "D01": "in situ soil temperature (at corresponding depth layer) < 0°C",
            "D02": "in situ air temperature < 0°C",
            "D03": "GLDAS soil temperature (at corresponding depth layer) < 0°C",
            "D04": "soil moisture shows peaks without precipitation event (in situ) in the preceding 24 hours",
            "D05": "soil moisture shows peaks without precipitation event (GLDAS) in the preceding 24 hours",
            "D06": "a spike is detected in soil moisture spectrum",
            "D07": "a negative jump is detected in soil moisture spectrum",
            "D08": "a positive jump is detected in soil moisture spectrum",
            "D09": "low constant values (for a minimum time of 12 hours) occur in soil moisture spectrum",
            "D10": "saturated plateau (for a minimum time length of 12 hours) occurs in soil moisture spectrum",
        }

        self.__category_dict = {
            "dynamic variable": ["G", "M"],
            "reported value exceeds output format field size": ["C01", "C02", "C03"],
            "questionable/dubious - geophysical based": [
                "D01",
                "D02",
                "D03",
                "D04",
                "D05",
            ],
            "questionable/dubious - spectrum based": [
                "D06",
                "D07",
                "D08",
                "D09",
                "D10",
            ],
        }

        if kind in self.__meaning_dict.keys():
            self.kind = kind
            self.category = [
                key for key, item in self.__category_dict.items() if self.kind in item
            ][0]

        else:
            raise AttributeError(
                f"There is no soil moisture flag '{kind}' contained in the ISMN database"
            )

    def __str__(self):
        return (
            f'Soil Moisture Flag "{self.kind}" means "{self.__meaning_dict[self.kind]}"'
        )

    def __repr__(self):
        return f"SoilMoistureFlag('{self.kind}')"


flags = [
    "C01",
    "C02",
    "C03",
    "D01",
    "D02",
    "D03",
    "D04",
    "D05",
    "D06",
    "D07",
    "D08",
    "D09",
    "D10",
    "G",
    "M",
]

for ff in flags:
    flag = SoilMoistureFlag(ff)
    print(f"\nkind: {flag.kind}, category: {flag.category}")
    print(f"{flag}")
    print(repr(flag))

flag = SoilMoistureFlag("C04")
