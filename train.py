from pycaret.anomaly import setup, create_model, save_model
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import scipy.signal
import os
import pre_processing2 as prep
import psycopg2 as pg
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

""" Query training data from postgreSQL database """
def get_training_data(conn, cursor,start_date, end_date, tag_name):
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
    and dt between '{start_date}'::Timestamptz and '{end_date}'::Timestamptz
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


def filter_columns(dftrain):
    """
    Filters the dataset and dftrain to include only columns containing '_1', '_6', or '_12', and the 'Timestamp' column.

    Args:
        dataset (pd.DataFrame): The original dataset.
        dftrain (pd.DataFrame): The training dataset.

    Returns:
        tuple: A tuple containing the filtered dataset and dftrain (dataset_filtered, dftrain_filtered).
    """
    selected_columns = ['Timestamp']
    for column in dftrain.columns:
        if '_1' in column or '_6' in column or '_12' in column:
            selected_columns.append(column)

    dftrain_filtered = dftrain[selected_columns]
    dftrain = dftrain[dftrain['II113RC001_U_mean_1h']>16.5]
    return dftrain_filtered

def set_negative_II113RC001_U_mean_to_zero(df):
  """
  Sets the values of all columns containing 'II113RC001_U_mean' to zero if they are less than zero.

  Args:
      df (pd.DataFrame): The input DataFrame.

  Returns:
      pd.DataFrame: The DataFrame with the updated values.
  """

  for column in df.columns:
      if 'II113RC001_U_mean' in column:
          df.loc[df[column] < 0, column] = 0

  return df


# reference_interval_start = input("Insert start date(YYYY-MM-DD HH:MM:SS): ")
# reference_interval_end = input("Insert end date(YYYY-MM-DD HH:MM:SS): ")
# if reference_interval_start == '' or reference_interval_end == '':
#     reference_interval_start = '2025-03-29 10:38:00'
#     reference_interval_end = '2025-03-31 10:00:00'

# verification_interval_start = '2025-03-31 10:00:00'
# verification_interval_end = '2025-03-31 10:36:00'
POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
DETECTOR_SCHEMA = os.environ.get("DETECTOR_SCHEMA")
MENDIX_SCHEMA = os.environ.get("MENDIX_SCHEMA")
VARIABLES_SCHEMA = os.environ.get("VARIABLES_SCHEMA")
POSTGRES_DB = os.environ.get("POSTGRES_DB")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")


db_config = {
    'database': POSTGRES_DB ,
    'user': POSTGRES_USER,
    'password': POSTGRES_PASSWORD,
    'host': POSTGRES_HOST,  # e.g., 'localhost' or an IP address
    'port': POSTGRES_PORT       # Default PostgreSQL port
}
conn = None
cursor = None
try:

    tags = [('2025-03-12 12:20:00', '2025-04-01 00:00:00', 'PIC11151A')]
    conn = pg.connect(**db_config)
    cursor = conn.cursor()
    current_path = os.path.dirname(os.path.abspath(__file__))
    # Create dftrain using the first function
    folder_path = os.getcwd()

    for start_date, end_date, tag_name in tags:
        dataset= get_training_data(conn, cursor, start_date, end_date, tag_name)
        dataset = prep.treat_data(dataset, tag_name)
        # dataset = prep.treat_data(pd.read_csv(os.path.join(folder_path,'training_data', f'{tag_name}.csv')), tag_name)
        dftrain = dataset

        # Filter columns using the second functio
        dftrain_filtered = prep.filter_columns(dftrain)


        s = setup(data=dftrain_filtered, session_id=123, normalize=False, ignore_features=['Timestamp'], remove_outliers = True)
        m = create_model('svm')
        model_path = os.path.join(current_path,'models', tag_name)
        save_model(m, model_path)
    conn.commit()
except Exception as e:
    print("failed to get training data, error: ")
    print(e)
    if conn:
        conn.rollback()

finally:
    if conn and cursor:
        cursor.close()
        conn.close()
        print("PostgreSQL connection closed")
