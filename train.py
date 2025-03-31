from pycaret.anomaly import setup, create_model, save_model
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import scipy.signal
import os
import pre_processing2 as prep



def create_dftrain(dataset, reference_interval_start, reference_interval_end):
    """
    Creates a training dataset (dftrain) by filtering the original dataset based on a reference interval and specific column conditions.

    Args:
        dataset (pd.DataFrame): The original dataset.
        reference_interval_start (str): Start date of the reference interval.
        reference_interval_end (str): End date of the reference interval.

    Returns:
        pd.DataFrame: The filtered training dataset (dftrain).
    """
    dftrain = dataset[
        (dataset['Timestamp'] >= reference_interval_start)
        & (dataset['Timestamp'] <= reference_interval_end)
    ]
    return dftrain

def filter_columns(dataset, dftrain):
    """
    Filters the dataset and dftrain to include only columns containing '_1', '_6', or '_12', and the 'Timestamp' column.

    Args:
        dataset (pd.DataFrame): The original dataset.
        dftrain (pd.DataFrame): The training dataset.

    Returns:
        tuple: A tuple containing the filtered dataset and dftrain (dataset_filtered, dftrain_filtered).
    """
    selected_columns = ['Timestamp']
    for column in dataset.columns:
        if '_1' in column or '_6' in column or '_12' in column:
            selected_columns.append(column)

    dataset_filtered = dataset[selected_columns]
    dftrain_filtered = dftrain[selected_columns]
    dftrain = dftrain[dftrain['II113RC001_U_mean_1h']>16.5]
    return dataset_filtered, dftrain_filtered

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


reference_interval_start = '2025-03-29 10:38:00'
reference_interval_end = '2025-03-31 10:00:00'

verification_interval_start = '2025-03-31 10:00:00'
verification_interval_end = '2025-03-31 10:36:00'


# Create dftrain using the first function
folder_path = os.getcwd()
# dataset = pd.read_csv(os.path.join(folder_path,'csv', 'aggregated_data.csv'))
dataset = prep.treat_data(pd.read_csv(os.path.join(folder_path,'csv', 'Malha_11151A.csv')))
dftrain = create_dftrain(dataset, reference_interval_start, reference_interval_end)

# Filter columns using the second functio
dataset_filtered, dftrain_filtered = filter_columns(dataset, dftrain)

# ... (continue with your analysis using dataset_filtered and dftrain_filtered)

s = setup(data=dftrain_filtered, session_id=123, normalize=False, ignore_features=['Timestamp'], remove_outliers = True)
m = create_model('svm')
save_model(m, 'model')
