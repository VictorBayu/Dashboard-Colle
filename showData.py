from imports import *
from insert import create_db_engine, test_connection

# Create engine and test connection
engine = create_db_engine()
if not test_connection(engine):
    sys.exit(1)

# Fetch data from the database
with engine.connect() as connection:
    query = text("SELECT TOP 10 * FROM AZ_Daily")
    results = connection.execute(query)
    rows = results.fetchall()
    columns = results.keys()

# Check if rows were fetched
if rows:
    # Convert datetime fields to strings
    rows = [
        tuple(str(item) if isinstance(item, datetime.datetime) else item for item in row)
        for row in rows
    ]

    # Create Polars DataFrame with explicit schema
    df_polars = pl.DataFrame(rows, schema=columns, orient='row')
    print(f"Polars DataFrame Columns: {df_polars.columns}")

    # Select specific columns and print
    try:
        selected_column = df_polars.select(["Periode", "AgreementNo", "Ang_ke", "Pokok_Awal", "Saldo_Pokok", "OVD"])
        print(selected_column)
        print(selected_column.describe())

    except Exception as select_error:
        print(f"Error selecting columns: {select_error}\nAvailable columns: {df_polars.columns}")
else:
    print("No rows fetched.")
