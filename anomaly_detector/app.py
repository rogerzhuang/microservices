import connexion
from connexion import NoContent
import yaml
import logging
import logging.config
import json
from pykafka import KafkaClient
import datetime
import os
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
import pykafka.common

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

# Log threshold values on startup
logger.info(f"Air Quality PM2.5 threshold: {app_config['thresholds']['pm25_max']}")
logger.info(f"Weather temperature threshold: {app_config['thresholds']['temperature_max']}")

def get_anomalies(anomaly_type=None):
    """ Get anomalies from the data store """
    logger.info(f"Request for anomalies received with type: {anomaly_type}")
    
    if not os.path.exists(app_config['datastore']['filename']):
        logger.error("Anomalies do not exist")
        return {"message": "Anomalies do not exist"}, 404
    
    with open(app_config['datastore']['filename'], 'r') as f:
        anomalies = json.load(f)
    
    # Filter by anomaly type if provided
    if anomaly_type:
        valid_types = ['AirQualityTooHigh', 'WeatherTooHigh']
        if anomaly_type not in valid_types:
            logger.error(f"Invalid anomaly type: {anomaly_type}")
            return {"message": f"Invalid anomaly type. Must be one of: {valid_types}"}, 400
            
        anomalies = [a for a in anomalies if a['anomaly_type'] == anomaly_type]
    
    # Sort anomalies from newest to oldest
    anomalies.sort(key=lambda x: x['timestamp'], reverse=True)
    
    logger.info(f"Returning {len(anomalies)} anomalies")
    return anomalies, 200
def process_messages():
    """ Process messages from Kafka and detect anomalies """
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    
    # Load existing anomalies
    anomalies = []
    if os.path.exists(app_config['datastore']['filename']):
        with open(app_config['datastore']['filename'], 'r') as f:
            anomalies = json.load(f)
    
    # Create consumer with correct offset configuration
    consumer = topic.get_simple_consumer(
        consumer_group=b'anomaly_detector_group',
        auto_offset_reset=pykafka.common.OffsetType.EARLIEST,
        reset_offset_on_start=False,
        auto_commit_enable=True,
        consumer_timeout_ms=1000
    )
    
    logger.info("Processing messages from last committed offset or beginning if no offset found")
    
    # Process messages
    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info(f"Consumed message: {msg}")
        
        # Check for anomalies based on event type
        if msg['type'] == 'air_quality':
            if msg['payload']['pm2_5_concentration'] > app_config['thresholds']['pm25_max']:
                anomaly = {
                    'event_id': msg['payload']['reading_id'],
                    'trace_id': msg['payload']['trace_id'],
                    'event_type': 'air_quality',
                    'anomaly_type': 'AirQualityTooHigh',
                    'description': f"PM2.5 concentration of {msg['payload']['pm2_5_concentration']} exceeds threshold of {app_config['thresholds']['pm25_max']}",
                    'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                anomalies.append(anomaly)
                logger.info(f"Added PM2.5 anomaly: {anomaly}")
        
        elif msg['type'] == 'weather':
            if msg['payload']['temperature'] > app_config['thresholds']['temperature_max']:
                anomaly = {
                    'event_id': msg['payload']['reading_id'],
                    'trace_id': msg['payload']['trace_id'],
                    'event_type': 'weather',
                    'anomaly_type': 'WeatherTooHigh',
                    'description': f"Temperature of {msg['payload']['temperature']} exceeds threshold of {app_config['thresholds']['temperature_max']}",
                    'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                anomalies.append(anomaly)
                logger.info(f"Added temperature anomaly: {anomaly}")
    
    # Save anomalies to file
    with open(app_config['datastore']['filename'], 'w') as f:
        json.dump(anomalies, f, indent=2)

def init_scheduler():
    """Initialize the scheduler to periodically process messages"""
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(process_messages, 
                  'interval', 
                  seconds=app_config['scheduler']['period_sec'])
    sched.start()
    logger.info(f"Scheduler initialized with {app_config['scheduler']['period_sec']} seconds interval")

app = connexion.FlaskApp(__name__, specification_dir='')
CORS(app.app)
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8120, host="0.0.0.0")