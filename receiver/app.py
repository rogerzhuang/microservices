import connexion
from connexion import NoContent
import logging
import logging.config
import yaml
import uuid
import datetime
import json
from pykafka import KafkaClient
from pykafka.exceptions import SocketDisconnectedError, LeaderNotAvailable

with open('app_config.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_config.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

def get_producer():
    """Get a new producer instance"""
    client = KafkaClient(hosts=f"{app_config['events']['hostname']}:{app_config['events']['port']}")
    topic = client.topics[str.encode(app_config['events']['topic'])]
    return topic.get_sync_producer()

# Initial producer setup
producer = get_producer()

def produce_message(msg_str):
    """Produce a message with error handling"""
    global producer
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            producer.produce(msg_str.encode('utf-8'))
            return
        except (SocketDisconnectedError, LeaderNotAvailable) as e:
            logger.warning(f"Connection issue (attempt {attempt + 1}): {str(e)}")
            try:
                producer.stop()
                producer = get_producer()
                producer.start()
            except Exception as e:
                logger.error(f"Failed to reconnect: {str(e)}")
                if attempt == max_retries - 1:
                    raise

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
    msg_str = json.dumps(msg)
    produce_message(msg_str)
    
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
    msg_str = json.dumps(msg)
    produce_message(msg_str)
    
    logger.info(f"Returned event weather response (Id: {trace_id}) with status 201")
    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")
