from snowflake.snowpark import Session

connection_parameters = {
   "account": "ti97672.east-us-2.azure",
   "user": "SVC_PYTHONIDE",
   "password": "Welcome12345",
   "role": "ACCOUNTADMIN",
   "database": "DEV_PROVIDER_REGISTRY",
   "schema": "PUBLIC",
   "warehouse": "DEV",
}

session = Session.builder.configs(connection_parameters).create()
df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
df = df.filter(df.a > 1)
df.show()
pandas_df = df.to_pandas()  # this requires pandas installed in the Python environment
result = df.collect()

# test pythom -m venv SnowparkDemo
# SnowparkDemo/Scripts/Activate.bat
# pip install "snowflake-snowpark-python[pandas]"