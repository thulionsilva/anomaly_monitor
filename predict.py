from pycaret.anomaly import load_model, predict_model
import os
import pandas as pd
from sqlalchemy.engine import cursor
import pre_processing as prep
import psycopg2 as pg
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

ENV = os.environ.get("ENV")  # prod or debug
POSTGRES_DB = os.environ.get("POSTGRES_DB")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
DETECTOR_SCHEMA = os.environ.get("DETECTOR_SCHEMA")
MENDIX_SCHEMA = os.environ.get("MENDIX_SCHEMA")
VARIABLES_SCHEMA = os.environ.get("VARIABLES_SCHEMA")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")


""" ******************* PostgreSQL Connection and Querying ******************* """

def get_data_with_in_clause(conn, cursor, tag_name):
    """
    Args:

    Returns:
        pandas.DataFrame: A DataFrame containing the query results,
                          or None if an error occurs or no data is found.
                          Returns an empty DataFrame if filter_values is empty.
    """
    df = None

    # Construct the SQL query using a placeholder for the IN clause
    # Note: We use f-string for table/column names (use with caution)
    #       but %s placeholder for the actual values to prevent SQL injection.
    # sql_query = f"SELECT * FROM public.chlorum_dump WHERE \"Timestamp\" BETWEEN NOW() AND (NOW() - INTERVAL '6 HOUR');"
    sql_query = f"""
        select dt as "Timestamp"
    ,p.description as "Variable"
    ,AVG(value) as "Value"
    from {VARIABLES_SCHEMA}.inspections i 
    inner join {VARIABLES_SCHEMA}.params p on i.id_param_fk = p.id 
    WHERE (p.description like '%{tag_name}%' or p.description = 'II113RC001\\U')
    and dt between (select max(dt) from {VARIABLES_SCHEMA}.inspections) - interval '12 HOUR' and (select max(dt) from {VARIABLES_SCHEMA}.inspections)
    GROUP BY "Timestamp", "Variable"
    ORDER BY "Timestamp" asc;
    """

    try:
        # print(f"Executing query: {cursor.mogrify(sql_query, (values_tuple,)).decode('utf-8')}") # Show rendered query
        # cursor.execute(sql_query, (values_tuple,)) # Crucial: pass values tuple inside another tuple
        cursor.execute(sql_query,) # Crucial: pass values tuple inside another tuple

        # Fetch all the results
        results = cursor.fetchall()

        if results:
            # Get column names from the cursor description
            colnames = [desc[0] for desc in cursor.description]
            # Create the Pandas DataFrame
            df = pd.DataFrame(results, columns=colnames)
            print(f"Successfully fetched {len(df)} rows.")
            print(f"Column name: {df.columns}")
        else:
            print("No data found for the given filter values.")
            # Return empty dataframe with columns if possible
            if cursor.description:
                 colnames = [desc[0] for desc in cursor.description]
                 df = pd.DataFrame(columns=colnames)
            else:
                 df = pd.DataFrame()


    except (Exception, pg.Error) as error:
        print(f"Error while connecting to or querying PostgreSQL: {error}")
        df = None # Indicate error


    return df




"""******************* Get Tag Id *******************"""
def get_anomaly_tags(cursor):
    query = f""" SELECT id, description FROM {VARIABLES_SCHEMA}.params_calc WHERE description like '%\\AS'; """
    cursor.execute(query)
    return cursor.fetchall()




if __name__ == "__main__":

    db_config = {
        'database': POSTGRES_DB ,
        'user': POSTGRES_USER,
        'password': POSTGRES_PASSWORD,
        'host': POSTGRES_HOST,  # e.g., 'localhost' or an IP address
        'port': POSTGRES_PORT       # Default PostgreSQL port
    }

    # Parameters for the query
    # tags = [(1, 'PIC11151A'), (2, 'FIC11121'), (3, 'FIC11120'), (4, 'TIC12102'), (5, 'PIC05101'), (6, 'PIC07814')] 

    # Creating postgres connection
    conn = None
    cursor = None
    try:
        print("Connecting to the PostgreSQL database...")
        conn = pg.connect(**db_config)

        # Create a cursor
        cursor = conn.cursor()
        tags = get_anomaly_tags(cursor)
        if conn and cursor:
            for tag_anomaly_id, tag_name in tags:
                tag_name = tag_name.split("\\")[0] #remove the \AS from the tag name
                dataset= get_data_with_in_clause(conn, cursor, tag_name)
                current_path = os.path.dirname(os.path.abspath(__file__))
                model_path = os.path.join(current_path, tag_name)
                # dataset = pd.read_csv(os.path.join(current_path,'csv', 'Malha_11151A.csv'))
                dataset = prep.treat_data(dataset, tag_name)
                dataset_filtered = prep.filter_columns(dataset)

                m = load_model(f'./models/{tag_name}')
                predictions = predict_model(m, data=dataset_filtered.drop(columns=['Timestamp']))

                window = 15 #The script will run every 15 minutes

                predictions = pd.concat([predictions, dataset['Timestamp']], axis=1).tail(window)
                for index, row in predictions.iterrows():
                    # Create the INSERT query

                    query = f"""INSERT INTO {VARIABLES_SCHEMA}.inspections_calc (id_param_fk, value, dt) 
                            select * from
                            (
                            select %s as id_param_fk,
                                %s as value,
                                %s as dt
                                ) as sub
                            where sub.dt > (select case when max(dt) is null then '2025-01-01'::timestamptz else max(dt) end 
                                            from {VARIABLES_SCHEMA}.inspections_calc where id_param_fk = %s); """

                    values = (tag_anomaly_id, row['Anomaly_Score'], row['Timestamp'], tag_anomaly_id)
                    # Execute the query
                    cursor.execute(query, values)
                print("Success")
                conn.commit()
        else:
            print('No connection or cursor was found')
    except Exception as e:
        if conn:
            conn.rollback()
            print("Error while conecting to database!")
        print(e)
    finally:
        if conn and cursor:
            cursor.close()
            conn.close()
            print("PostgreSQL connection closed.")


