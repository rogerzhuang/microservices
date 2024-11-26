import connexion
from connexion import NoContent
import logging
import logging.config
import yaml
import uuid
import datetime
import json
from kafka import KafkaProducer
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

producer = KafkaProducer(
    bootstrap_servers=f"{app_config['events']['hostname']}:{app_config['events']['port']}",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def submit_air_quality_data(body):
    """ Forwards air quality data to Kafka """
    trace_id = str(uuid.uuid4())
    logger.info(f"Received event air quality request with a trace id of {trace_id}")
    
    body['trace_id'] = trace_id
    
    msg = {
        "type": "air_quality",
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": body
    }
    producer.send(app_config['events']['topic'], value=msg)
    
    logger.info(f"Returned event air quality response (Id: {trace_id}) with status 201")
    return NoContent, 201

def submit_weather_data(body):
    """ Forwards weather data to Kafka """
    trace_id = str(uuid.uuid4())
    logger.info(f"Received event weather request with a trace id of {trace_id}")
    
    body['trace_id'] = trace_id
    
    msg = {
        "type": "weather",
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": body
    }
    producer.send(app_config['events']['topic'], value=msg)
    
    logger.info(f"Returned event weather response (Id: {trace_id}) with status 201")
    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", base_path="/receiver", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")
