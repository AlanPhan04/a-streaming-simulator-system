from dataclasses import dataclass

from models.environment import Environment


@dataclass
class Air(Environment):
    temperature: float
    moisture: float
    light: float
    totalRainfall: float
    rainfall: float
    windDirection: float
    pm2dot5: float
    pm10: float
    CO: float
    NOx: float
    SO2: float

    @classmethod
    def from_dict(cls, data) -> "Air":
        """Create an Air instance from a dictionary and verify data"""

        return cls(
            time=cls.parse_time(data["Time"]),
            station=data["Station"],
            temperature=cls.safe_float(data["Temperature"]),
            moisture=cls.safe_float(data["Moisture"]),
            light=cls.safe_float(data["Light"]),
            totalRainfall=cls.safe_float(data["Total_Rainfall"]),
            rainfall=cls.safe_float(data["Rainfall"]),
            windDirection=cls.safe_float(data["Wind_Direction"]),
            pm2dot5=cls.safe_float(data["PM2.5"]),
            pm10=cls.safe_float(data["PM10"]),
            CO=cls.safe_float(data["CO"]),
            NOx=cls.safe_float(data["NOx"]),
            SO2=cls.safe_float(data["SO2"]),
        )
