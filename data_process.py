
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
