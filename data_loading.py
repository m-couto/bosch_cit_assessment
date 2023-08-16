import pandas as pd
import pyodbc


if __name__=='__main__':

    # load the data
    example_dataset_path = 'datasets/epa/processed_alt_fuel_stations.csv'
    df = pd.read_csv(example_dataset_path)

    # For simplicity I'll only show how to load the first 5 columns
    # into a database in a SQL server
    df = df.iloc[:, :5]


    # Connect to Cloud SQL Server
    driver = 'Cloud SQL Server'
    server = 'local_host'
    database_name = 'bosch_db'

    connection = pyodbc.connect(
        f'Driver={driver};'
        f'Server={server};'
        f'Database={database_name};'
        'Trusted_Connection=yes;'
    )
    cursor = connection.cursor()


    # create table
    create_tb_query = '''
    CREATE TABLE processed_alt_fuel_stations (
    id INT PRIMARY KEY,
    EV_Level2_EVSE_Num FLOAT,
    Latitude FLOAT,
    Longitude FLOAT,
    Open_Date INT
    )
    '''
    cursor.execute(create_tb_query)


    # insert values into the table
    insert_data_query = '''
    INSERT INTO processed_alt_fuel_stations (id, EV_Level2_EVSE_Num, Latitude, Longitude, Open_Date)
    VALUES (?,?,?,?,?)
    '''
    for row in df.itertuples():
        cursor.execute(
            insert_data_query,
            row[0], row[1], row[2], row[3], row[4]
        )
    
    
    connection.commit()
