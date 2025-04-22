from dataclasses import dataclass

from models.environment import Environment


@dataclass
class Water(Environment):
    pH: float
    DO: float
    temperature: float
    salinity: float

    @classmethod
    def from_dict(cls, data) -> "Water":
        """Create a Water instance from a dictionary"""

        return cls(
            time=cls.parse_time(data["Time"]),
            station=data["Station"],
            pH=cls.safe_float(data["pH"]),
            DO=cls.safe_float(data["DO"]),
            temperature=cls.safe_float(data["Temperature"]),
            salinity=cls.safe_float(data["Salinity"]),
        )
