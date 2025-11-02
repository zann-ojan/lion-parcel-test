# Import libraries
import pandas as pd
import numpy as np

# Load raw shipment data
df_shipments = pd.read_csv('Raw Data/shipments_raw.csv')

# Data Type Check
df_shipments.info()

# Check for Duplicates
print(f"Number of duplicate rows: {df_shipments.duplicated().sum() / len(df_shipments) * 100:.2f}%")

# Check for Missing Values
df_shipments.isna().sum()

# Check for Invalid Delivery Dates
df_shipments['delivered_date'] = pd.to_datetime(df_shipments['delivered_date'], format='mixed')
df_shipments['booked_date'] = pd.to_datetime(df_shipments['booked_date'], format='mixed')
invalid_ratio = ((df_shipments['delivered_date'] < df_shipments['booked_date']).sum() / len(df_shipments)) * 100
print(f"\n{invalid_ratio:.2f}% of data have invalid delivery dates.\n")

# Data Cleaning Function
def cleaned_data(df):
    # Delete Duplicate
    df = df.drop_duplicates().reset_index(drop=True)

    # Change date columns to datetime format
    for col in df.columns:
        if col.endswith('_date'):
            df[col] = pd.to_datetime(df[col], format = 'mixed')

    # Handle invalid dates: delivered_date earlier than booked_date
    df = df[~(df['delivered_date'] < df['booked_date'])].reset_index(drop=True)

    # Standardize 'status' column
    df['status'] = df['status'].str.replace('-', ' ').str.title()

    return df

# Apply data cleaning
df_shipments_cleaned = cleaned_data(df_shipments)

# Create useful derived columns 
df_shipments_cleaned['delivery_duration_days'] = (
    df_shipments_cleaned['delivered_date'] - df_shipments_cleaned['booked_date']
).dt.days

df_shipments_cleaned['delivery_delay_days'] = np.where(
    df_shipments_cleaned['status']=='Delivered', 
    (df_shipments_cleaned['delivered_date'] - df_shipments_cleaned['estimated_delivery_date']).dt.days, 
    np.nan
)

df_shipments_cleaned['is_delayed'] = np.where(
    df_shipments_cleaned['delivered_date'] > df_shipments_cleaned['estimated_delivery_date'],
    True,
    False
)

df_shipments_cleaned.to_csv('Output Data/shipment_transformed.csv', index=False)