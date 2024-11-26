import connexion
from connexion import NoContent
import yaml
import logging
import logging.config
import json
from pykafka import KafkaClient
from flask_cors import CORS
import os

if "TARGET_ENV" in os.environ and os.environ["TARGET_ENV"] == "test":
    print("In Test Environment")
    app_conf_file = "/config/app_config.yml"
    log_conf_file = "/config/log_config.yml"
else:
    print("In Dev Environment")
    app_conf_file = "app_config.yml"
    log_conf_file = "log_config.yml"

with open(app_conf_file, 'r') as f:
    app_config = yaml.safe_load(f.read())

# External Logging Configuration
with open(log_conf_file, 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

logger.info("App Conf File: %s" % app_conf_file)
logger.info("Log Conf File: %s" % log_conf_file)

def get_air_quality_reading(index):
    """ Get Air Quality Reading in History """
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    
    consumer = topic.get_simple_consumer(reset_offset_on_start=True,
                                         consumer_timeout_ms=1000)
    logger.info(f"Retrieving air quality reading at index {index}")
    
    try:
        air_quality_events = []
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)
            if msg['type'] == 'air_quality':
                air_quality_events.append(msg)
            
            if len(air_quality_events) > index:
                return air_quality_events[index], 200
        
        logger.error(f"Could not find air quality reading at index {index}")
        return {"message": "Not Found"}, 404
    except:
        logger.error("No more messages found")
        logger.error(f"Could not find air quality reading at index {index}")
        return {"message": "Not Found"}, 404

def get_weather_reading(index):
    """ Get Weather Reading in History """
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    
    consumer = topic.get_simple_consumer(reset_offset_on_start=True,
                                         consumer_timeout_ms=1000)
    logger.info(f"Retrieving weather reading at index {index}")
    
    try:
        weather_events = []
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)
            if msg['type'] == 'weather':
                weather_events.append(msg)
            
            if len(weather_events) > index:
                return weather_events[index], 200
        
        logger.error(f"Could not find weather reading at index {index}")
        return {"message": "Not Found"}, 404
    except:
        logger.error("No more messages found")
        logger.error(f"Could not find weather reading at index {index}")
        return {"message": "Not Found"}, 404

def get_event_stats():
    """ Get event stats """
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    
    consumer = topic.get_simple_consumer(reset_offset_on_start=True,
                                         consumer_timeout_ms=1000)
    logger.info("Retrieving event stats")
    
    try:
        num_air_quality = 0
        num_weather = 0
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)
            if msg['type'] == 'air_quality':
                num_air_quality += 1
            elif msg['type'] == 'weather':
                num_weather += 1
        
        stats = {
            "num_air_quality": num_air_quality,
            "num_weather": num_weather
        }
        logger.info(f"Retrieved event stats: {stats}")
        return stats, 200
    except:
        logger.error("Error retrieving event stats")
        return {"message": "Error retrieving stats"}, 500

app = connexion.FlaskApp(__name__, specification_dir='')
if "TARGET_ENV" not in os.environ or os.environ["TARGET_ENV"] != "test":
    CORS(app.app)
app.add_api("openapi.yml", base_path="/analyzer", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    app.run(port=8110, host="0.0.0.0")
