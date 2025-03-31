import pandas as pd
import os

#Get the current working directory
current_directory = os.getcwd()
folder_path = current_directory # Specify the folder containing the CSV files

# Initialize an empty list to store DataFrames

def read_and_merge_csv_files(folder_path):
    """
    Reads all CSV files in the specified folder, renames columns, and merges them into a single DataFrame.
    
    Parameters:
    folder_path (str): The path to the folder containing CSV files.
    
    Returns:
    pd.DataFrame: A merged DataFrame containing data from all CSV files.
    """
    # Initialize an empty list to store DataFrames
    dfs = []

    # Iterate through all files in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            try:
                df = pd.read_csv(file_path)
                # Rename the column
                new_columns = {
                    'Timestamp 107P002A - Falha/FB_ON': 'Timestamp'
                }
                df = df.rename(columns=new_columns)

                # Replace '\', '/', and empty spaces in column names with '_'
                new_column_names = {col: col.replace('\\', '_').replace('/', '_').replace(' ', '_') for col in df.columns}
                df = df.rename(columns=new_column_names)

                dfs.append(df)
                print(filename)
            except Exception as e:
                print(f"Error reading file {filename}: {e}")

    # Concatenate all DataFrames into a single DataFrame
    merged_df = pd.concat(dfs, ignore_index=True)
    merged_df = merged_df.sort_values(by=['Timestamp'], ascending=True)

    # Now you have a single DataFrame 'merged_df' containing data from all CSV files.

    # Optional: Save the merged DataFrame to a new CSV file
    #merged_df.to_excel(os.path.join(folder_path, 'merged_df.xlsx'), index=False)
    return merged_df

merged_df = read_and_merge_csv_files(folder_path)
# Filter columns based on specific criteria
# Filter columns that contain '11151A' or 'II113RC001_U' in their names
df1 = merged_df[['Timestamp'] + [col for col in merged_df.columns if '11151A' in col] + [col for col in merged_df.columns if 'II113RC001_U' in col]]


def create_aggregated_df(df, columns_to_aggregate, window_sizes):
    
  """
  Creates a new DataFrame with aggregated values for specified columns,
  including the number of times the original value crosses the 6h moving average.

  Args:
    df: The input DataFrame.
    columns_to_aggregate: A list of column names to aggregate.
    window_sizes: A list of window sizes in hours.

  Returns:
    A new DataFrame with aggregated values.
  """

  df2 = pd.DataFrame()
  df['Timestamp'] = pd.to_datetime(df['Timestamp'])
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
          # Shift to compare with the previous value
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


columns_to_aggregate = ['P11151A_U', 'PIC11151A_-_SP_Int_Ext', 'PIC11151A_MV', 'PIC11151A_PV_IN', 'PIC11151A_SP','II113RC001_U']
window_sizes = [1, 6, 12]

df2 = create_aggregated_df(df1, columns_to_aggregate, window_sizes)
