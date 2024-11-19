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
import sqlite3

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

def init_database():
    """Initialize SQLite database if it doesn't exist"""
    try:
        conn = sqlite3.connect(app_config['datastore']['filename'])
        c = conn.cursor()
        
        # Create anomalies table if it doesn't exist
        c.execute('''
            CREATE TABLE IF NOT EXISTS anomalies
            (id INTEGER PRIMARY KEY AUTOINCREMENT,
             event_id VARCHAR(250) NOT NULL,
             trace_id VARCHAR(250) NOT NULL,
             event_type VARCHAR(100) NOT NULL,
             anomaly_type VARCHAR(100) NOT NULL,
             description TEXT NOT NULL,
             timestamp DATETIME NOT NULL)
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {app_config['datastore']['filename']}")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise e

def get_anomalies(anomaly_type=None):
    """ Get anomalies from the data store """
    logger.info(f"Request for anomalies received with type: {anomaly_type}")
    
    try:
        conn = sqlite3.connect(app_config['datastore']['filename'])
        c = conn.cursor()
        
        if anomaly_type:
            c.execute('''
                SELECT * FROM anomalies 
                WHERE anomaly_type = ? 
                ORDER BY timestamp DESC
            ''', (anomaly_type,))
        else:
            c.execute('SELECT * FROM anomalies ORDER BY timestamp DESC')
        
        # Convert to list of dictionaries
        columns = ['id', 'event_id', 'trace_id', 'event_type', 'anomaly_type', 'description', 'timestamp']
        results = []
        for row in c.fetchall():
            results.append(dict(zip(columns, row)))
        
        conn.close()
        logger.info(f"Returning {len(results)} anomalies")
        return results, 200
        
    except Exception as e:
        logger.error(f"Error retrieving anomalies: {e}")
        return {"message": "Error retrieving anomalies"}, 500

def process_messages():
    """ Process messages from Kafka and detect anomalies """
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    
    # Create consumer with correct offset configuration
    consumer = topic.get_simple_consumer(
        consumer_group=b'anomaly_detector_group',
        auto_offset_reset=pykafka.common.OffsetType.EARLIEST,
        reset_offset_on_start=False,
        auto_commit_enable=True,
        consumer_timeout_ms=1000
    )
    
    logger.info("Processing messages from last committed offset or beginning if no offset found")
    
    try:
        conn = sqlite3.connect(app_config['datastore']['filename'])
        c = conn.cursor()
        
        # Process messages
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)
            logger.info(f"Consumed message: {msg}")
            
            if msg['type'] == 'air_quality':
                if msg['payload']['pm2_5_concentration'] > app_config['thresholds']['pm25_max']:
                    c.execute('''
                        INSERT INTO anomalies 
                        (event_id, trace_id, event_type, anomaly_type, description, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        msg['payload']['reading_id'],
                        msg['payload']['trace_id'],
                        'air_quality',
                        'TooHigh',
                        f"PM2.5 concentration of {msg['payload']['pm2_5_concentration']} exceeds threshold of {app_config['thresholds']['pm25_max']}",
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ))
                    conn.commit()
                    logger.info("Added PM2.5 anomaly to database")
            
            elif msg['type'] == 'weather':
                if msg['payload']['temperature'] > app_config['thresholds']['temperature_max']:
                    c.execute('''
                        INSERT INTO anomalies 
                        (event_id, trace_id, event_type, anomaly_type, description, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        msg['payload']['reading_id'],
                        msg['payload']['trace_id'],
                        'weather',
                        'TooHigh',
                        f"Temperature of {msg['payload']['temperature']} exceeds threshold of {app_config['thresholds']['temperature_max']}",
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ))
                    conn.commit()
                    logger.info("Added temperature anomaly to database")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error processing messages: {e}")
        raise e

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
    init_database()  # Initialize database before starting the app
    init_scheduler()
    app.run(port=8120, host="0.0.0.0")
