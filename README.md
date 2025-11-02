# Technical Test – Lion Parcel Data Analytic

## Overview
The purpose of this project is to transform raw shipment and customer data into a structured and analysis-ready dataset. The final goal is to build a data mart summarizing shipment performance by customer and month, which can be used for dashboards and reporting.
There are 3 layers for data architecture in this test:
1. Bronze layer/Raw layer : Contains unprocessed/raw shipment and customer data.
2. Silver layer : Cleans and standardizes shipment data (handles duplicates, invalid/missing values, standardizes formats).
3. Gold layer : Aggregates cleaned data with customer info to create month year shipment performance summaries.

## Approach

### Silver layer
1.  **Remove Duplicates**
    <br> Remove duplicate rows to ensure clean and consistent data.  
    ```python
    df = df.drop_duplicates().reset_index(drop=True)
    ```
2.  **Standardize Date Columns**
    <br> Convert all columns ending with _date into datetime format (mixed parsing to handle multiple formats).
    ``` python
    df[col] = pd.to_datetime(df[col], format='mixed')
    ```
3.  **Handle Invalid Delivery Dates**
    <br> Remove records where delivered_date < booked_date. These are considered invalid or data entry errors. Since this invalid data represents only 1.31% of all records, it is removed from the dataset to maintain data quality.
    ``` python
    df = df[~(df['delivered_date'] < df['booked_date'])].reset_index(drop=True)
    ```
4.  **Standardize Text Fields**
    <br> Clean up and normalize the Status column for consistent naming (e.g., "in-transit" → "In Transit").
    ``` python
    df['status'] = df['status'].str.replace('-', ' ').str.title()
    ```
5.  **Create Derived Columns**
    <br> 
    - delivery_duration_days: number of days between booked_date and delivered_date
    - delivery_delay_days = (delivered_date - estimated_delivery_date), if delivered
    - is_delayed = True if delivered_date > estimated_delivery_date, else False
    ``` python
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
    ```
6. **Handling Missing Values**
    <br>
    for this test, saya tidak menghapus nilai null, karena nilai null hanya ada di kolom delivered_date, yang mana ini benar karena delivery_date tetap null karena statusnya adalah Cancelled, In Transit, atau Pending

### Gold Layer
1.  **Join Shipment Data with Customer Data**
    ``` python
    df_joined = pd.merge(df_shipment_transformed, df_customer, on='customer_id', how='left')
    ```
2.  **Create Monthly Period Column**
    ``` python
    df_joined['month_year'] = pd.to_datetime(df_joined['booked_date']).dt.strftime('%B %Y')
    ```
3.  **Aggregate by Customer and Month**
    ``` python
    agg_data = df_joined.groupby(['customer_id', 'customer_name', 'month_year']).agg(
        total_shipments=('shipment_id', 'count'),
        delivered_shipments=('status', lambda x: (x == 'Delivered').sum()),
        on_process_shipments=('status', lambda x: x.isin(['In Transit', 'Pending']).sum()),
        cancelled_shipments=('status', lambda x: (x == 'Cancelled').sum()),
        avg_delivery_days=('delivery_duration_days', 'mean'),
        delayed_shipments=('is_delayed', lambda x: (x == True).sum())
    ).reset_index()
    ```
4.  **Compute Delay Rate**
    ``` python
    agg_data['delayed_rate'] = agg_data['delayed_shipments'] / agg_data['delivered_shipments']
    ```

## Assumptions
- Records with delivered_date < booked_date (1.31% of data) are considered invalid and removed.
- Only shipments with status "Delivered" are included in delay calculations.
- Missing delivered_date values indicate shipments still in process or cancelled.
- Date parsing uses flexible (mixed) format to handle multiple date input types.

## Business Logic
- Shipments must have a valid booking date (`booked_date`) before delivery (`delivered_date`).
- Only records with status = "Delivered" should have a delivery date.
- Cancelled or in-transit shipments are excluded from delivery duration and delay calculations.
- Delivery is considered delayed when `delivered_date` > `estimated_delivery_date`.
- Each shipment record must be unique; duplicates are removed.

## How to Reproduce
