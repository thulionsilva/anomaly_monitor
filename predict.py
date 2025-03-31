from pycaret.anomaly import load_model, predict_model
import os
import pandas as pd
import pre_processing2 as prep
import tkinter as tk
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk

import psycopg2 as pg
import sys
import uuid
import pytz
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
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
current_path = os.path.dirname(os.path.abspath(__file__))
model_path = os.path.join(current_path, 'model')
dataset = pd.read_csv(os.path.join(current_path,'csv', 'Malha_11151A.csv'))
dataset = prep.treat_data(dataset)
dataset_filtered, _ = prep.filter_columns(dataset, dataset)

m = load_model('model')
predictions = predict_model(m, data=dataset_filtered.drop(columns=['Timestamp']))

predictions = pd.concat([predictions, dataset['Timestamp']], axis=1)
print(predictions.tail(10))



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



# Example usage
    # Create and show the line chart window
app = DataFrameLineChart(predictions, x_column='Timestamp', y_column='Anomaly_Score')
app.mainloop()
