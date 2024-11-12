import connexion
from connexion import NoContent
import logging
import logging.config
import yaml
import uuid
import datetime
import json
import time
from pykafka import KafkaClient
import threading

with open('app_config.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_config.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

class KafkaProducerWrapper:
    """Wrapper class for Kafka producer with reconnection logic"""
    def __init__(self, host, port, topic):
        self.host = host
        self.port = port
        self.topic_name = topic
        self.client = None
        self.producer = None
        self.last_checked = None
        self.check_interval = 15
        self.running = False
        self.heartbeat_thread = None

    def connect(self):
        """Establish connection to Kafka"""
        try:
            self.client = KafkaClient(hosts=f"{self.host}:{self.port}")
            topic = self.client.topics[str.encode(self.topic_name)]
            self.producer = topic.get_sync_producer(
                min_queued_messages=1,
                max_queued_messages=1000,
                linger_ms=500,
                retry_backoff_ms=100
            )
            self.last_checked = time.time()
            if not self.heartbeat_thread or not self.heartbeat_thread.is_alive():
                self.start_heartbeat()
            logger.info("Successfully connected to Kafka")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            raise

    def start_heartbeat(self):
        """Start the heartbeat background thread"""
        self.running = True
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop)
        self.heartbeat_thread.daemon = True  # Thread will exit when main program exits
        self.heartbeat_thread.start()
        logger.info("Kafka heartbeat thread started")

    def ensure_connection(self):
        """Ensure the connection is alive and reconnect if necessary"""
        current_time = time.time()
        if (self.producer is None or 
            self.last_checked is None or 
            current_time - self.last_checked > self.check_interval):
            try:
                # Test connection by getting topic list
                self.client.topics
                self.last_checked = current_time
            except:
                logger.warning("Kafka connection lost, attempting to reconnect...")
                self.connect()

    def _heartbeat_loop(self):
        """Background thread that maintains connection"""
        while self.running:
            try:
                # Heartbeat: actively check topics to keep connection alive
                self.client.topics
                # Update metadata to ensure fresh connection
                self.client.update_cluster()
                logger.debug("Kafka connection heartbeat successful")
            except Exception as e:
                logger.warning(f"Heartbeat failed, attempting to reconnect... Error: {str(e)}")
                try:
                    self.connect()
                except Exception as conn_err:
                    logger.error(f"Reconnection failed: {str(conn_err)}")
            time.sleep(self.check_interval)

    def produce_message(self, message):
        """Produce message with connection retry"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.ensure_connection()
                self.producer.produce(message.encode('utf-8'))
                logger.debug("Message produced successfully")
                return
            except Exception as e:
                logger.error(f"Failed to produce message (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2)

    def stop(self):
        """Stop the heartbeat thread and clean up"""
        self.running = False
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=5)
        if self.producer:
            self.producer.stop()

# Create a single producer instance
kafka_producer = KafkaProducerWrapper(
    host=app_config['events']['hostname'],
    port=app_config['events']['port'],
    topic=app_config['events']['topic']
)
kafka_producer.connect()

# Add cleanup on application shutdown
def cleanup():
    kafka_producer.stop()

import atexit
atexit.register(cleanup)

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
    kafka_producer.produce_message(msg_str)
    
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
    kafka_producer.produce_message(msg_str)
    
    logger.info(f"Returned event weather response (Id: {trace_id}) with status 201")
    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")
