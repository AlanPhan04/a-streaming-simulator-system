from dataclasses import dataclass

from models.environment import Environment


@dataclass
class Earth(Environment):
    moisture: float
    temperature: float
    pH: float
    waterRoot: float
    waterLeaf: float
    waterLevel: float
    voltage: float

    @classmethod
    def from_dict(cls, data) -> "Earth":
        """Create an Earth instance from a dictionary and verify data"""

        return cls(
            time=cls.parse_time(data["Time"]),
            station=data["Station"],
            moisture=cls.safe_float(data["Moisture"]),
            temperature=cls.safe_float(data["Temperature"]),
            pH=cls.safe_float(data["pH"]),
            waterRoot=cls.safe_float(data["Water_Root"]),
            waterLeaf=cls.safe_float(data["Water_Leaf"]),
            waterLevel=cls.safe_float(data["Water_Level"]),
            voltage=cls.safe_float(data["Voltage"]),
        )
