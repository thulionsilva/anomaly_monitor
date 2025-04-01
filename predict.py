from pycaret.anomaly import load_model, predict_model
import os
import pandas as pd
from sqlalchemy.engine import cursor
import pre_processing2 as prep
import tkinter as tk
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk

import psycopg2 as pg
import sys
import uuid
import pytz
from datetime import datetime, timedelta
from sqlalchemy import VARBINARY, create_engine, text
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


#         dataset (pd.DataFrame): The original dataset.
#         dftrain (pd.DataFrame): The training dataset.

""" ******************* PostgreSQL Connection and Querying ******************* """

def get_data_with_in_clause(db_params):
    """
    Connects to PostgreSQL, executes a SELECT query with a WHERE IN clause,
    and returns the results as a Pandas DataFrame.

    Args:
        db_params (dict): Dictionary containing database connection parameters
                          (e.g., {'database': 'mydb', 'user': 'user',
                                 'password': 'password', 'host': 'localhost',
                                 'port': '5432'}).
        table_name (str): The name of the table to query.
        column_name (str): The name of the column for the WHERE IN clause.
        filter_values (list or tuple): A list or tuple of values to filter by
                                       in the IN clause.

    Returns:
        pandas.DataFrame: A DataFrame containing the query results,
                          or None if an error occurs or no data is found.
                          Returns an empty DataFrame if filter_values is empty.
    """
    conn = None
    cursor = None
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
    WHERE (p.description like '%11151A%' or p.description = 'II113RC001\\U') and dt between now() - interval '12 HOUR' and now()
    GROUP BY "Timestamp", "Variable"
    ORDER BY "Timestamp" asc;
    """

    try:
        # Establish the connection
        print("Connecting to the PostgreSQL database...")
        conn = pg.connect(**db_params)

        # Create a cursor
        cursor = conn.cursor()

        # Execute the query
        # Pass the tuple of values as a single element within another tuple
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

    # finally:
    #     # Close the cursor and connection
    #     if cursor:
    #         cursor.close()
    #     if conn:
    #         conn.close()
    #         print("PostgreSQL connection closed.")

    return df, cursor, conn




"""******************* Vizualization of the predictions using Tkinter and Matplotlib *******************"""
class DataFrameLineChart(tk.Tk):
    def __init__(self, dataframe, x_column=None, y_column=None):
        super().__init__()
        
        self.title("DataFrame Line Chart")
        self.geometry("800x600")
        
        # Store the dataframe
        self.df = dataframe
        self.x_column = x_column
        self.y_column = y_column
        
        # Create the plot
        self.create_plot()
        # Add a quit button
        self.add_quit_button()
    
    def create_plot(self):
        # Create figure and axis
        self.figure, self.ax = plt.subplots(figsize=(10, 6))
        
        # Plot the data
        if self.x_column and self.y_column:
            # If specific columns are provided
            self.df.plot(kind='line', x=self.x_column, y=self.y_column, ax=self.ax)
        elif self.x_column:
            # If only x is provided, plot all other columns against it
            columns_to_plot = [col for col in self.df.columns if col != self.x_column]
            self.df.plot(kind='line', x=self.x_column, y=columns_to_plot, ax=self.ax)
        else:
            # Plot all columns using index as x-axis
            self.df.plot(kind='line', ax=self.ax)
        
        self.ax.set_title('DataFrame Line Chart')
        self.ax.grid(True)
        
        # Create a canvas to display the matplotlib figure in Tkinter
        self.canvas = FigureCanvasTkAgg(self.figure, self)

        self.canvas_widget = self.canvas.get_tk_widget()
        # Create a navigation toolbar
        self.toolbar = NavigationToolbar2Tk(self.canvas, self)
        self.toolbar.update()
        self.toolbar.pack(side=tk.TOP, fill=tk.X)
        self.canvas_widget.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

    def add_quit_button(self):
        # Create a frame for the quit button
        self.button_frame = tk.Frame(self)
        self.button_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # Add a quit button
        self.quit_button = tk.Button(
            self.button_frame, 
            text="Quit", 
            command=self.quit,  # Call our custom method
            bg="red",
            fg="white",
            width=10,
            height=1,
            font=("Arial", 10, "bold")
        )
        self.quit_button.pack(side=tk.RIGHT)



#POSTGRES_DB = os.environ.get("POSTGRES_DB")
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

# Parameters for the query
target_table = 'employees'   # Replace with your table name
target_column = 'department_id' # Replace with the column you want to filter
ids_to_select = [10, 30, 50] # Replace with the values you want to select

# --- Run the query ---
dataset, cursor, conn = get_data_with_in_clause(db_params=db_config)


current_path = os.path.dirname(os.path.abspath(__file__))
model_path = os.path.join(current_path, 'model')
# dataset = pd.read_csv(os.path.join(current_path,'csv', 'Malha_11151A.csv'))
dataset = prep.treat_data(dataset)
dataset_filtered, _ = prep.filter_columns(dataset, dataset)

m = load_model('model')
predictions = predict_model(m, data=dataset_filtered.drop(columns=['Timestamp']))

window = 5 #The script will run every 5 minutes

predictions = pd.concat([predictions, dataset['Timestamp']], axis=1).tail(window)
print(predictions.describe())
if cursor and conn:
    try:
        for index, row in predictions.iterrows():
            # Create the INSERT query
            query = f"""INSERT INTO {VARIABLES_SCHEMA}.inspections_calc ic (id_param_fk, value, dt) VALUES (%s, %s, %s) 
                    where """

            query = f"""INSERT INTO {VARIABLES_SCHEMA}.inspections_calc (id_param_fk, value, dt) 
                    select * from
                    (
                    select %s as id_param_fk,
                        %s as value,
                        %s as dt
                        ) as sub
                   where sub.dt > (select case when max(dt) is null then current_date else max(dt) end from variables.inspections_calc where id_param_fk = %s); """
            id = 1
            values = (id, row['Anomaly_Score'], row['Timestamp'], id)
            # Execute the query
            cursor.execute(query, values)
        print("Success")
    except Exception as e:
        conn.rollback()
        print("Error while inserting data!")
        print(e)
    finally:
        conn.commit()
        cursor.close()
        conn.close()
        print("PostgreSQL connection closed.")
else:
    print("no cursor or connection found")


    # Create and show the line chart window
app = DataFrameLineChart(predictions, x_column='Timestamp', y_column='Anomaly_Score')
app.mainloop()
