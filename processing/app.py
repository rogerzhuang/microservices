import connexion
from connexion import NoContent
import requests
import yaml
import logging
import logging.config
import datetime
import json
from apscheduler.schedulers.background import BackgroundScheduler
import os
from flask_cors import CORS
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

def init_database():
    """Create the database and stats table if they don't exist"""
    logger.info("Checking if database exists")
    try:
        conn = sqlite3.connect(app_config['datastore']['filename'])
        c = conn.cursor()
        
        # Create stats table if it doesn't exist
        c.execute('''
            CREATE TABLE IF NOT EXISTS stats
            (id INTEGER PRIMARY KEY,
             num_air_quality_readings INTEGER,
             num_weather_readings INTEGER,
             max_pm25_concentration REAL,
             avg_temperature REAL,
             last_updated TEXT)
        ''')
        
        # Check if table is empty and insert initial record if needed
        c.execute('SELECT COUNT(*) FROM stats')
        if c.fetchone()[0] == 0:
            c.execute('''
                INSERT INTO stats
                (num_air_quality_readings, num_weather_readings, 
                 max_pm25_concentration, avg_temperature, last_updated)
                VALUES (0, 0, 0, 0, '2024-01-01T00:00:00Z')
            ''')
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
    except sqlite3.Error as e:
        logger.error(f"Error initializing database: {e}")
        raise

def get_stats():
    logger.info("Request for statistics received")
    
    try:
        conn = sqlite3.connect(app_config['datastore']['filename'])
        c = conn.cursor()
        c.execute('SELECT * FROM stats ORDER BY id DESC LIMIT 1')
        result = c.fetchone()
        conn.close()
        
        if result is None:
            logger.error("Statistics do not exist")
            return {"message": "Statistics do not exist"}, 404
        
        response_data = {
            "num_air_quality_readings": result[1],
            "num_weather_readings": result[2],
            "max_pm25_concentration": result[3],
            "avg_temperature": result[4],
            "last_updated": result[5]
        }
        
        logger.debug(f"Statistics: {response_data}")
        logger.info("Request for statistics completed")
        
        return response_data, 200
    
    except sqlite3.Error as e:
        logger.error(f"Error retrieving statistics: {e}")
        return {"message": "Error retrieving statistics"}, 500

def populate_stats():
    logger.info("Start Periodic Processing")
    
    try:
        conn = sqlite3.connect(app_config['datastore']['filename'])
        c = conn.cursor()
        
        # Get current stats
        c.execute('SELECT * FROM stats ORDER BY id DESC LIMIT 1')
        result = c.fetchone()
        
        stats = {
            "num_air_quality_readings": result[1],
            "num_weather_readings": result[2],
            "max_pm25_concentration": result[3],
            "avg_temperature": result[4],
            "last_updated": result[5]
        }
    
        current_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        last_updated = stats['last_updated']
        
        air_quality_response = requests.get(
            f"{app_config['eventstore']['url']}/air-quality?start_timestamp={last_updated}&end_timestamp={current_timestamp}")
        weather_response = requests.get(
            f"{app_config['eventstore']['url']}/weather?start_timestamp={last_updated}&end_timestamp={current_timestamp}")
        
        if air_quality_response.status_code == 200:
            air_quality_events = air_quality_response.json()
            logger.info(f"Received {len(air_quality_events)} air quality events")
            stats['num_air_quality_readings'] += len(air_quality_events)
            for event in air_quality_events:
                if event['pm2_5_concentration'] > stats['max_pm25_concentration']:
                    stats['max_pm25_concentration'] = event['pm2_5_concentration']
        else:
            logger.error(f"Failed to get air quality events with status {air_quality_response.status_code}")
        
        if weather_response.status_code == 200:
            weather_events = weather_response.json()
            logger.info(f"Received {len(weather_events)} weather events")
            stats['num_weather_readings'] += len(weather_events)
            if weather_events:
                total_temp = sum(event['temperature'] for event in weather_events)
                avg_temp = total_temp / len(weather_events)
                stats['avg_temperature'] = (stats['avg_temperature'] * (stats['num_weather_readings'] - len(weather_events)) + avg_temp * len(weather_events)) / stats['num_weather_readings']
        else:
            logger.error(f"Failed to get weather events with status {weather_response.status_code}")
        
        stats['last_updated'] = current_timestamp
        
        # Update database with new stats
        c.execute('''
            INSERT INTO stats
            (num_air_quality_readings, num_weather_readings, 
             max_pm25_concentration, avg_temperature, last_updated)
            VALUES (?, ?, ?, ?, ?)
        ''', (stats['num_air_quality_readings'], 
              stats['num_weather_readings'],
              stats['max_pm25_concentration'],
              stats['avg_temperature'],
              current_timestamp))
        
        conn.commit()
        conn.close()
        
        logger.debug(f"Updated statistics: {stats}")
        logger.info("Periodic Processing Ended")
        
    except sqlite3.Error as e:
        logger.error(f"Error updating statistics: {e}")

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 
                  'interval', 
                  seconds=app_config['scheduler']['period_sec'])
    sched.start()

app = connexion.FlaskApp(__name__, specification_dir='')
CORS(app.app)
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    init_database()
    init_scheduler()
    app.run(port=8100, host="0.0.0.0")
