import connexion
import json
import logging
import logging.config
import os
import requests
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from requests.exceptions import Timeout, ConnectionError

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

def check_services():
    """ Called periodically to check service health """
    status = {}
    
    # Check Receiver
    receiver_status = "Unavailable"
    try:
        response = requests.get(app_config['urls']['receiver'], 
                              timeout=app_config['timeout'])
        if response.status_code == 200:
            receiver_status = "Healthy"
            logger.info("Receiver is Healthy")
        else:
            logger.info("Receiver returning non-200 response")
    except (Timeout, ConnectionError):
        logger.info("Receiver is Not Available")
    status['receiver'] = receiver_status

    # Check Storage
    storage_status = "Unavailable"
    try:
        response = requests.get(app_config['urls']['storage'], 
                              timeout=app_config['timeout'])
        if response.status_code == 200:
            storage_json = response.json()
            storage_status = f"Storage has {storage_json['num_air_quality_readings']} air quality and {storage_json['num_weather_readings']} weather events"
            logger.info("Storage is Healthy")
        else:
            logger.info("Storage returning non-200 response")
    except (Timeout, ConnectionError):
        logger.info("Storage is Not Available")
    status['storage'] = storage_status

    # Check Processing
    processing_status = "Unavailable"
    try:
        response = requests.get(app_config['urls']['processing'], 
                              timeout=app_config['timeout'])
        if response.status_code == 200:
            processing_json = response.json()
            processing_status = f"Processing has {processing_json['num_air_quality_readings']} air quality and {processing_json['num_weather_readings']} weather events"
            logger.info("Processing is Healthy")
        else:
            logger.info("Processing returning non-200 response")
    except (Timeout, ConnectionError):
        logger.info("Processing is Not Available")
    status['processing'] = processing_status

    # Check Analyzer
    analyzer_status = "Unavailable"
    try:
        response = requests.get(app_config['urls']['analyzer'], 
                              timeout=app_config['timeout'])
        if response.status_code == 200:
            analyzer_json = response.json()
            analyzer_status = f"Analyzer has {analyzer_json['num_air_quality_readings']} air quality and {analyzer_json['num_weather_readings']} weather events"
            logger.info("Analyzer is Healthy")
        else:
            logger.info("Analyzer returning non-200 response")
    except (Timeout, ConnectionError):
        logger.info("Analyzer is Not Available")
    status['analyzer'] = analyzer_status

    # Write status to file
    with open(app_config['datastore']['status_file'], 'w') as f:
        json.dump(status, f, indent=4)
    
    logger.info("Service check completed")

def get_checks():
    """ Returns the service status """
    try:
        with open(app_config['datastore']['status_file'], 'r') as f:
            status = json.load(f)
        return status, 200
    except FileNotFoundError:
        logger.error("Status file not found")
        return {"message": "Status file not found"}, 404

def init_scheduler():
    """ Initialize the scheduler """
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(check_services, 
                  'interval', 
                  seconds=app_config['scheduler']['period_sec'])
    sched.start()

def init_status_file():
    """ Initialize the status file if it doesn't exist """
    if not os.path.exists(app_config['datastore']['status_file']):
        initial_status = {
            "receiver": "Unavailable",
            "storage": "Unavailable",
            "processing": "Unavailable",
            "analyzer": "Unavailable"
        }
        with open(app_config['datastore']['status_file'], 'w') as f:
            json.dump(initial_status, f, indent=4)
        logger.info("Initialized status file")

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", base_path="/check", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    init_status_file()
    init_scheduler()
    app.run(port=8130, host="0.0.0.0")
