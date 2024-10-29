import mysql.connector

def create_tables():
    conn = mysql.connector.connect(
        user='roger',
        password='axl320n24h',
        host='acit3855-kafka.westus.cloudapp.azure.com',
        port=3306,
        database='events'
    )
    c = conn.cursor()

    # Create air_quality table
    c.execute('''
        CREATE TABLE IF NOT EXISTS air_quality
        (id INT AUTO_INCREMENT PRIMARY KEY,
         reading_id VARCHAR(36) NOT NULL,
         sensor_id VARCHAR(250) NOT NULL,
         timestamp VARCHAR(100) NOT NULL,
         pm2_5_concentration FLOAT NOT NULL,
         pm10_concentration FLOAT NOT NULL,
         co2_level FLOAT NOT NULL,
         o3_level FLOAT NOT NULL,
         date_created VARCHAR(100) NOT NULL,
         trace_id VARCHAR(36) NOT NULL)
    ''')

    # Create weather table
    c.execute('''
        CREATE TABLE IF NOT EXISTS weather
        (id INT AUTO_INCREMENT PRIMARY KEY,
         reading_id VARCHAR(36) NOT NULL,
         sensor_id VARCHAR(250) NOT NULL,
         timestamp VARCHAR(100) NOT NULL,
         temperature FLOAT NOT NULL,
         humidity FLOAT NOT NULL,
         wind_speed FLOAT NOT NULL,
         noise_level FLOAT NOT NULL,
         date_created VARCHAR(100) NOT NULL,
         trace_id VARCHAR(36) NOT NULL)
    ''')

    conn.commit()
    conn.close()

if __name__ == '__main__':
    create_tables()
    print("Database tables created successfully.")
