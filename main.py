import os
import sys
from dotenv import load_dotenv

load_dotenv()

# %%
import psycopg2
import pandas as pd
import dask.dataframe as dd


DATABASE_URL = os.environ.get("DATABASE_URL")

# %%

# %%
