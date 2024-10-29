import mysql.connector

def drop_tables():
    conn = mysql.connector.connect(
        user='roger',
        password='axl320n24h',
        host='acit3855-kafka.westus.cloudapp.azure.com',
        port=3306,
        database='events'
    )
    c = conn.cursor()

    # Drop air_quality table
    c.execute('''
        DROP TABLE IF EXISTS air_quality
    ''')

    # Drop weather table
    c.execute('''
        DROP TABLE IF EXISTS weather
    ''')

    conn.commit()
    conn.close()

if __name__ == '__main__':
    drop_tables()
    print("Database tables dropped successfully.")
