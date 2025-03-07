
import os
from hdbcli import dbapi
import pandas as pd

print("Start")

# Use connection settings from the environment
# connection = dbapi.connect(
#     address=os.environ.get("DB_ADDRESS"),
#     port=os.environ.get("DB_PORT"),
#     user=os.environ.get("DB_USER"),
#     password=os.environ.get("DB_PASSWORD"),
#     autocommit=True,
#     sslValidateCertificate=False,
# )

import pandas as pd
df = pd.read_csv('GRAPH_DOCU_QR.csv', low_memory=False) 
df.head(3)

print("test")

from hana_ml import ConnectionContext
cc = ConnectionContext(
    address=os.environ.get("DB_ADDRESS"),
    port=os.environ.get("DB_PORT"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"), encrypt=True
) 
print(cc.hana_version()) 
print(cc.get_current_schema())

# Create a table
cursor = cc.connection.cursor()
sql_command = '''CREATE TABLE GRAPH_DOCU_QRC3_2201(ID BIGINT, L1 NVARCHAR(3), L2 NVARCHAR(3), L3 NVARCHAR(3), FILENAME NVARCHAR(100), HEADER1 NVARCHAR(5000), HEADER2 NVARCHAR(5000), TEXT NCLOB, VECTOR_STR NCLOB);'''
cursor.execute(sql_command)
cursor.close()

from hana_ml.dataframe import create_dataframe_from_pandas v_hdf = create_dataframe_from_pandas(
connection_context=cc, pandas_df=df, table_name="GRAPH_DOCU_QRC3_2201", allow_bigint=True,
    append=True
    )

# Add REAL_VECTOR column
cursor = cc.connection.cursor()
sql_command = '''ALTER TABLE GRAPH_DOCU_QRC3_2201 ADD (VECTOR REAL_VECTOR(1536));'''
cursor.execute(sql_command)
cursor.close()

# Create vectors from strings
cursor = cc.connection.cursor()
sql_command = '''UPDATE GRAPH_DOCU_QRC3_2201 SET VECTOR = TO_REAL_VECTOR(VECTOR_STR);''' cursor.execute(sql_command)
cursor.close()

import hana_ml 
print(hana_ml.__version__)