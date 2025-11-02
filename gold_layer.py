import pandas as pd
import numpy as np

# Load raw shipment transformed data
df_shipment_transformed = pd.read_csv('Output Data/shipment_transformed.csv')

# Load raw customer data
df_customer = pd.read_csv('Raw Data/customers_raw.csv')

# Join shipment data with customer data
df_joined = pd.merge(df_shipment_transformed, df_customer, on='customer_id', how='left')

# Create month_year column from booked_date
df_joined['month_year'] = pd.to_datetime(df_joined['booked_date']).dt.strftime('%B %Y')

# Aggregate data at customer and month_year level
agg_data = df_joined.groupby(['customer_id', 'customer_name', 'month_year']).agg(
    total_shipments=('shipment_id', 'count'),
    delivered_shipments=('status', lambda x: (x == 'Delivered').sum()),
    on_process_shipments=('status', lambda x: x.isin(['In Transit', 'Pending']).sum()),
    cancelled_shipments=('status', lambda x: (x == 'Cancelled').sum()),
    avg_delivery_days=('delivery_duration_days', 'mean'),
    delayed_shipments=('is_delayed', lambda x: (x == True).sum())
).reset_index()

# Compute delayed rate
agg_data['delayed_rate'] = (agg_data['delayed_shipments'] / agg_data['delivered_shipments'])

# Round to 2 decimal places
agg_data['avg_delivery_days'] = agg_data['avg_delivery_days'].round(2)
agg_data['delayed_rate'] = agg_data['delayed_rate'].round(2)

# Save final dataset
agg_data.to_csv('Output Data/shipment_performance.csv', index=False)
