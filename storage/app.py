import connexion
from connexion import NoContent
import yaml
import logging
import logging.config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
from base import Base
from air_quality import AirQuality
from weather import Weather
import datetime
import pymysql
import json
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import QueuePool
import time

with open('log_config.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

with open('app_config.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

datastore_config = app_config['datastore']

logger.info(f"Connecting to DB. Hostname:{datastore_config['hostname']}, Port:{datastore_config['port']}")

# DB_ENGINE = create_engine("sqlite:///smart_city.sqlite")
DB_ENGINE = create_engine(
    f"mysql+pymysql://{datastore_config['user']}:{datastore_config['password']}@{datastore_config['hostname']}:{datastore_config['port']}/{datastore_config['db']}",
    pool_size=5,
    max_overflow=10,
    pool_recycle=3600,  # Recycle connections after 1 hour
    pool_pre_ping=True  # Enable connection health checks
)
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

pymysql.install_as_MySQLdb()

def submit_air_quality_data(body):
    """ Receives air quality sensor data """
    logger.debug(f"Received event air quality data with a trace id of {body['trace_id']}")

    session = DB_SESSION()

    aq = AirQuality(body['trace_id'],
                    body['reading_id'],
                    body['sensor_id'],
                    body['timestamp'],
                    body['pm2_5_concentration'],
                    body['pm10_concentration'],
                    body['co2_level'],
                    body['o3_level'])

    session.add(aq)

    session.commit()
    session.close()

    logger.debug(f"Stored event air quality request with a trace id of {body['trace_id']}")

    return NoContent, 201

def submit_weather_data(body):
    """ Receives weather sensor data """
    logger.debug(f"Received event weather data with a trace id of {body['trace_id']}")

    session = DB_SESSION()

    weather = Weather(body['trace_id'],
                      body['reading_id'],
                      body['sensor_id'],
                      body['timestamp'],
                      body['temperature'],
                      body['humidity'],
                      body['wind_speed'],
                      body['noise_level'])

    session.add(weather)

    session.commit()
    session.close()

    logger.debug(f"Stored event weather request with a trace id of {body['trace_id']}")

    return NoContent, 201

def get_air_quality_readings(start_timestamp, end_timestamp):
    """ Gets new air quality readings between the start and end timestamps """
    logger.info(f"Received request for air quality readings between {start_timestamp} and {end_timestamp}")

    session = DB_SESSION()
    
    start_timestamp_datetime = datetime.datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    end_timestamp_datetime = datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    readings = session.query(AirQuality).filter(
        and_(AirQuality.date_created >= start_timestamp_datetime,
             AirQuality.date_created < end_timestamp_datetime))
    results_list = [reading.to_dict() for reading in readings]

    session.close()

    logger.info("Query for Air Quality readings between %s and %s returns %d results" %
                (start_timestamp, end_timestamp, len(results_list)))
    
    return results_list, 200

def get_weather_readings(start_timestamp, end_timestamp):
    """ Gets new weather readings between the start and end timestamps """
    logger.info(f"Received request for weather readings between {start_timestamp} and {end_timestamp}")

    session = DB_SESSION()
    
    start_timestamp_datetime = datetime.datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    end_timestamp_datetime = datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    readings = session.query(Weather).filter(
        and_(Weather.date_created >= start_timestamp_datetime,
             Weather.date_created < end_timestamp_datetime))
    results_list = [reading.to_dict() for reading in readings]
    
    session.close()

    logger.info("Query for Weather readings between %s and %s returns %d results" %
                (start_timestamp, end_timestamp, len(results_list)))
    
    return results_list, 200

def process_messages():
    """ Process event messages """
    while True:
        try:
            hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
            client = KafkaClient(hosts=hostname)
            topic = client.topics[str.encode(app_config["events"]["topic"])]
            
            consumer = topic.get_simple_consumer(consumer_group=b'event_group',
                                               reset_offset_on_start=False,
                                               auto_offset_reset=OffsetType.LATEST)
            
            logger.info("Connected to Kafka, starting message processing")
            
            last_message_time = time.time()
            while True:
                message = consumer.consume(timeout=10)  # 10 seconds timeout
                current_time = time.time()
                
                if message is None:
                    # No message received within timeout
                    if current_time - last_message_time > 300:  # 5 minutes without messages
                        logger.warning("No messages received for 5 minutes, reconnecting...")
                        break  # Break inner loop to reconnect
                    continue
                
                last_message_time = current_time
                session = DB_SESSION()
                try:
                    msg_str = message.value.decode('utf-8')
                    msg = json.loads(msg_str)
                    logger.info(f"Message: {msg}")
                    payload = msg["payload"]
                    
                    if msg["type"] == "air_quality":
                        aq = AirQuality(payload['trace_id'],
                                      payload['reading_id'],
                                      payload['sensor_id'],
                                      payload['timestamp'],
                                      payload['pm2_5_concentration'],
                                      payload['pm10_concentration'],
                                      payload['co2_level'],
                                      payload['o3_level'])
                        session.add(aq)
                        logger.info(f"Stored air quality event with trace ID: {payload['trace_id']}")
                    elif msg["type"] == "weather":
                        weather = Weather(payload['trace_id'],
                                        payload['reading_id'],
                                        payload['sensor_id'],
                                        payload['timestamp'],
                                        payload['temperature'],
                                        payload['humidity'],
                                        payload['wind_speed'],
                                        payload['noise_level'])
                        session.add(weather)
                        logger.info(f"Stored weather event with trace ID: {payload['trace_id']}")
                    
                    session.commit()
                    consumer.commit_offsets()
                    
                except OperationalError as e:
                    logger.error(f"Database operational error: {str(e)}")
                    session.rollback()
                except Exception as e:
                    logger.error(f"Processing error: {str(e)}")
                    session.rollback()
                finally:
                    session.close()
                    
        except Exception as e:
            logger.error(f"Kafka connection error: {str(e)}")
            
        logger.info("Attempting to reconnect in 10 seconds...")
        time.sleep(10)  # Wait before reconnecting

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    app.run(port=8090, host="0.0.0.0")
