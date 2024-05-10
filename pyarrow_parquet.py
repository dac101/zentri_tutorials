import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pacsv
import datetime

today = datetime.datetime.now().strftime('%Y-%m-%d')
filename = f'stocks_{today}.csv'

# Read CSV into a PyArrow Table directly
table = pacsv.read_csv(filename)

# Convert PyArrow Table to Pandas DataFrame
table_pandas = table.to_pandas()

# Convert Pandas DataFrame back to a PyArrow Table
table_arrow = pa.Table.from_pandas(table_pandas)

# Write the PyArrow Table to a Parquet file
pq.write_table(table_arrow, filename + '.parquet')


# Read the Parquet file into a PyArrow Table
table = pq.read_table(filename)

# Convert the PyArrow Table to a Pandas DataFrame
df = table.to_pandas()

print(df)
