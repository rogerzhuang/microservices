from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.sql.functions import now
from base import Base
import datetime

class AirQuality(Base):
    """ Air Quality """
    __tablename__ = "air_quality"

    id = Column(Integer, primary_key=True)
    trace_id = Column(String(36), nullable=False)
    reading_id = Column(String(36), nullable=False)
    sensor_id = Column(String(250), nullable=False)
    timestamp = Column(String(100), nullable=False)
    pm2_5_concentration = Column(Float, nullable=False)
    pm10_concentration = Column(Float, nullable=False)
    co2_level = Column(Float, nullable=False)
    o3_level = Column(Float, nullable=False)
    date_created = Column(String(100), nullable=False)

    def __init__(self, trace_id, reading_id, sensor_id, timestamp, pm2_5_concentration, pm10_concentration, co2_level, o3_level):
        """ Initializes an air quality reading """
        self.trace_id = trace_id
        self.reading_id = reading_id
        self.sensor_id = sensor_id
        self.timestamp = timestamp
        self.pm2_5_concentration = pm2_5_concentration
        self.pm10_concentration = pm10_concentration
        self.co2_level = co2_level
        self.o3_level = o3_level
        self.date_created = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    def to_dict(self):
        """ Dictionary Representation of an air quality reading """
        return {
            'id': self.id,
            'trace_id': self.trace_id,
            'reading_id': self.reading_id,
            'sensor_id': self.sensor_id,
            'timestamp': self.timestamp,
            'date_created': self.date_created,
            'pm2_5_concentration': self.pm2_5_concentration,
            'pm10_concentration': self.pm10_concentration,
            'co2_level': self.co2_level,
            'o3_level': self.o3_level
        }