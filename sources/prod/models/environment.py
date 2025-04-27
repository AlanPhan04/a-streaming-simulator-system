from dataclasses import dataclass
from datetime import datetime


@dataclass
class Environment:
    time: datetime
    station: str

    @staticmethod
    def safe_float(value) -> float:
        """Convert to float, return 0.0 if invalid"""
        try:
            return (
                float(value)
                if str(value).strip() not in ["", "---", "NA", "None"]
                else 0.0
            )
        except ValueError:
            return 0.0

    @classmethod
    def parse_time(cls, time_str) -> datetime:
        """Parse time string into datetime object"""
        return datetime.strptime(time_str, "%d/%m/%Y %H:%M:%S")