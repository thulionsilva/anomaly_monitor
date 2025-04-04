import pandas as pd
import os

#Get the current working directory
current_directory = os.getcwd()
folder_path = current_directory # Specify the folder containing the CSV files

# Initialize an empty list to store DataFrames

def pivot_dataframe_column(df, index_col, columns_col, values_col):
  """
  Pivots a Pandas DataFrame column.

  Args:
    df: The Pandas DataFrame to pivot.
    index_col: The column to use to make new DataFrame's index.
    columns_col: The column to use to make the new DataFrame's columns.
    values_col: The column(s) to use for populating the new DataFrame's values.

  Returns:
    A pivoted Pandas DataFrame. Returns None if there are errors.
  """
  try:
    pivoted_df = df.pivot(index=index_col, columns=columns_col, values=values_col)
    pivoted_df.reset_index(inplace=True)
    pivoted_df.columns.name = None  # Remove the name of the columns index
    new_column_names = {col: col.replace('\\', '_').replace('/', '_').replace(' ', '_') for col in pivoted_df.columns}
    pivoted_df = pivoted_df.rename(columns=new_column_names)

    # pivoted_df.set_index('Timestamp', inplace=True)
    return pivoted_df
  except KeyError as e:
    print(f"Error: Column '{e.args[0]}' not found in DataFrame.")
    return None
  except ValueError as e:
      print(f"ValueError: {e}")
      return None



def read_pivot_csv_file(filepath):
  df = pd.read_csv(filepath)
  # df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms')

  # Pivot the DataFrame
  df = pivot_dataframe_column(df, 'Timestamp', 'Variable', 'Value')
  return df



def create_aggregated_df(df, columns_to_aggregate, window_sizes):
    
  """
  Creates a new DataFrame with aggregated values for specified columns,
  including the number of times the original Value crosses the 6h moving average.

  Args:
    df: The input DataFrame.
    columns_to_aggregate: A list of column names to aggregate.
    window_sizes: A list of window sizes in hours.

  Returns:
    A new DataFrame with aggregated values.
  """

  df2 = pd.DataFrame()
  # df['Timestamp'] = pd.to_datetime(df['Timestamp'])
  df = df.set_index('Timestamp')

  for column in columns_to_aggregate:
    if column in df.columns:
      # Calculate 6h moving average for the entire column
      df[f'{column}_ma_6h'] = df[column].rolling('6H').mean()

      for window_size in window_sizes:
        # Calculate aggregations for the specified window
        df_agg = df[column].rolling(str(window_size) + 'H').agg(['mean', 'std', 'max', 'min', 'median'])
        df_agg['range'] = df_agg['max'] - df_agg['min']

        # Calculate crossings against the overall 6h moving average
        if window_size == 6:
          # Shift to compare with the previous Value
          df[f'{column}_shifted'] = df[column].shift(1)
          # Count crossings using a vectorized approach
          df_agg['crossings_6h'] = (
              (df[column] > df[f'{column}_ma_6h']) & (df[f'{column}_shifted'] < df[f'{column}_ma_6h'])
          ).astype(int).rolling('6H').sum()
          df = df.drop(columns=[f'{column}_shifted'])


        df_agg = df_agg.rename(columns={
            'mean': f'{column}_mean_{window_size}h',
            'std': f'{column}_std_{window_size}h',
            'max': f'{column}_max_{window_size}h',
            'min': f'{column}_min_{window_size}h',
            'median': f'{column}_median_{window_size}h',
            'range': f'{column}_range_{window_size}h',
            'crossings_6h': f'{column}_crossings_6h'
        })

        if df2.empty:
          df2 = df_agg
        else:
          df2 = pd.concat([df2, df_agg], axis=1)

      df = df.drop(columns=[f'{column}_ma_6h'])


  # Reset index to make Timestamp a regular column
  df2 = df2.reset_index()

  return df2

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


def treat_data(df, tag_name):
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms')
    df = pivot_dataframe_column(df, 'Timestamp', 'Variable', 'Value')
    # print(df.describe())
    # Filter columns based on specific criteria
    # Filter columns that contain '11151A' or 'II113RC001_U' in their names
    # df1 = df[['Timestamp'] + [col for col in df.columns if '11151A' in col] + [col for col in df.columns if 'II113RC001_U' in col]]

    # columns_to_aggregate = ['P11151A_U', 'PIC11151A_-_SP_Int_Ext', 'PIC11151A_MV', 'PIC11151A_PV_IN', 'PIC11151A_SP','II113RC001_U']
    columns_to_aggregate = [f'{tag_name}_-_SP_Int_Ext', f'{tag_name}_MV', f'{tag_name}_PV_IN', f'{tag_name}_SP','II113RC001_U']
    window_sizes = [1, 6, 12]

    df2 = create_aggregated_df(df, columns_to_aggregate, window_sizes)
    # Print the aggregated DataFrame
    # print(df2.describe())
    return df2
