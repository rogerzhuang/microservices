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
        consumer = None
        try:
            hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
            logger.info(f"Attempting to connect to Kafka at {hostname}")
            client = KafkaClient(hosts=hostname)
            topic = client.topics[str.encode(app_config["events"]["topic"])]
            
            # Configure consumer to only read new messages
            consumer = topic.get_simple_consumer(
                consumer_group=b'event_group',
                reset_offset_on_start=False,  # Don't reset offset on start
                auto_offset_reset=OffsetType.LATEST,  # Use latest when no offset is found
                consumer_timeout_ms=2000,  # 2 second timeout
                fetch_message_max_bytes=52428800,  # 50MB to handle large message batches
                auto_commit_enable=True,
                auto_commit_interval_ms=1000
            )
            
            # Wait for consumer to be ready
            while not consumer.partitions:
                logger.info("Waiting for partitions to be assigned...")
                time.sleep(1)
            
            # Seek to end of each partition using consumer's partition list
            for partition_id in consumer.partitions.keys():
                partition = consumer.partitions[partition_id]
                last_offset = partition.latest_available_offsets()[0]  # Get latest offset
                consumer.reset_offsets([(partition_id, last_offset)])
                logger.info(f"Set partition {partition_id} to latest offset {last_offset}")
            
            logger.info("Successfully connected to Kafka, starting message processing")
            
            last_message_time = time.time()
            message_count = 0
            
            while True:
                try:
                    message = consumer.consume()
                    current_time = time.time()
                    
                    if message is None:
                        time_since_last = current_time - last_message_time
                        logger.debug(f"No message received. Time since last message: {time_since_last:.1f}s")
                        if time_since_last > 300:  # 5 minutes
                            logger.warning("No messages received for 5 minutes, forcing reconnection...")
                            break
                        continue

                    # Process message
                    last_message_time = current_time
                    msg_str = message.value.decode('utf-8')
                    msg = json.loads(msg_str)
                    
                    session = DB_SESSION()
                    try:
                        payload = msg["payload"]
                        
                        if msg["type"] == "air_quality":
                            aq = AirQuality(
                                payload['trace_id'],
                                payload['reading_id'],
                                payload['sensor_id'],
                                payload['timestamp'],
                                payload['pm2_5_concentration'],
                                payload['pm10_concentration'],
                                payload['co2_level'],
                                payload['o3_level']
                            )
                            session.add(aq)
                            logger.info(f"Stored air quality event with trace ID: {payload['trace_id']}")
                        elif msg["type"] == "weather":
                            weather = Weather(
                                payload['trace_id'],
                                payload['reading_id'],
                                payload['sensor_id'],
                                payload['timestamp'],
                                payload['temperature'],
                                payload['humidity'],
                                payload['wind_speed'],
                                payload['noise_level']
                            )
                            session.add(weather)
                            logger.info(f"Stored weather event with trace ID: {payload['trace_id']}")
                        
                        session.commit()
                        message_count += 1
                        logger.debug(f"Successfully processed message {message_count}")
                            
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")
                        session.rollback()
                    finally:
                        session.close()
                        
                except Exception as e:
                    logger.error(f"Error consuming message: {str(e)}")
                    break
                    
        except Exception as e:
            logger.error(f"Kafka connection error: {str(e)}")
        
        finally:
            if consumer:
                try:
                    consumer.stop()
                except Exception as e:
                    logger.error(f"Error stopping consumer: {str(e)}")
                    
        logger.info("Attempting to reconnect in 10 seconds...")
        time.sleep(10)

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    app.run(port=8090, host="0.0.0.0")
