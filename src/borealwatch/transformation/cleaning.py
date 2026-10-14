def clean_wildfire_data(df):
    # Clean wildfire records.
    return df.drop_duplicates()
