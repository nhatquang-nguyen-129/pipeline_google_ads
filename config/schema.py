"""
===================================================================
GOOGLE SCHEMA MODULE
-------------------------------------------------------------------
This module defines and manages **schema-related logic** for the  
Google Ads data pipeline, acting as a single source of truth  
for all required fields across different data layers.

It plays a key role in ensuring schema alignment and preventing  
data inconsistency between raw → staging → mart layers.

✔️ Declares expected column names for each entity type   
✔️ Supports schema enforcement in validation and ETL stages  
✔️ Prevents schema mismatch when handling dynamic Google API fields  

⚠️ This module does *not* fetch or transform data.  
It only provides schema utilities to support other pipeline components.
===================================================================
"""
# Add logging capability for tracking process execution and errors
import logging

# Add Python Pandas library for data processing
import pandas as pd

# Add Python NumPy library for numerical computing and array operations
import numpy as np