# Prepare file for time series

# Imports:

import numpy as np
import pandas as pd
from datetime import timedelta, datetime

import acquire as a


# Preparing function for Store Data

def prepare_store_data():
    

    """ Function to prepare store data"""
    df = a.get_store_data()                            # Acquire the data
    df['sale_date'] = pd.to_datetime(df['sale_date'])  # convert sales date to datetime format
    df = df.set_index(df.sale_date).sort_index()       # set index and sort index for sale_date
    df['month'] = df.sale_date.dt.month                # create a month column
    df['day of week'] = df.sale_date.dt.day_name()     # create a day name column
    df['sales_total'] = df.sale_amount * df.item_price # create a sales total column
    
    return df
    
    
    