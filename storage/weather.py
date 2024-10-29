from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.sql.functions import now
from base import Base
import datetime

class Weather(Base):
    """ Weather """
    __tablename__ = "weather"

    id = Column(Integer, primary_key=True)
    trace_id = Column(String(36), nullable=False)
    reading_id = Column(String(36), nullable=False)
    sensor_id = Column(String(250), nullable=False)
    timestamp = Column(String(100), nullable=False)
    temperature = Column(Float, nullable=False)
    humidity = Column(Float, nullable=False)
    wind_speed = Column(Float, nullable=False)
    noise_level = Column(Float, nullable=False)
    date_created = Column(String(100), nullable=False)

    def __init__(self, trace_id, reading_id, sensor_id, timestamp, temperature, humidity, wind_speed, noise_level):
        """ Initializes a weather reading """
        self.trace_id = trace_id
        self.reading_id = reading_id
        self.sensor_id = sensor_id
        self.timestamp = timestamp
        self.temperature = temperature
        self.humidity = humidity
        self.wind_speed = wind_speed
        self.noise_level = noise_level
        self.date_created = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    def to_dict(self):
        """ Dictionary Representation of a weather reading """
        return {
            'id': self.id,
            'trace_id': self.trace_id,
            'reading_id': self.reading_id,
            'sensor_id': self.sensor_id,
            'timestamp': self.timestamp,
            'date_created': self.date_created,
            'temperature': self.temperature,
            'humidity': self.humidity,
            'wind_speed': self.wind_speed,
            'noise_level': self.noise_level
        }