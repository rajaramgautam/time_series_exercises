import requests
import pandas as pd
import os


def get_store_data():
    '''
    This function reads in retail data from the website if there are no csv files to pull from
    '''
    # Checks if .csv files are present. If any are missing, will acquire new data for all three datasets
    if (os.path.isfile('items.csv') == False) or (os.path.isfile('sales.csv') == False) or (os.path.isfile('stores.csv') == False):
        print("Data is not cached. Acquiring new data...")
        items_df, stores_df, sales_df = new_retail_data()
    else:
        print("Data is cached. Reading from .csv files")
        items_df = pd.read_csv('items.csv')
        print("Items data acquired...")
        stores_df = pd.read_csv('stores.csv')
        print("Stores data acquired...")
        sales_df = pd.read_csv('sales.csv')
        print("Sales data acquired...")

    combined_df = sales_df.merge(items_df, how='left', left_on='item', right_on='item_id').drop(columns=['item'])
    combined_df = combined_df.merge(stores_df, how='left', left_on='store', right_on='store_id').drop(columns=['store'])
    print("Acquisition complete")
    return combined_df


# acquire.py file will be created.
def api_df(a, host, api):
# parameters to pass for functions
    host = "https://python.zgulde.net/"
    api = "api/v1/"

    url = host + api + a

    response = requests.get(url)

    if response.ok:
        payload = response.json()["payload"]

        # endpoint should be "items", "sales", or "stores"
        contents = payload[a]

        # Make a dataframe of the contents
        df = pd.DataFrame(contents)
        # load next page
        next_page = payload["next_page"]

        # next_page is None when we're on the last page.
        # This loop only runs if there is a next_page defined.
        while next_page:
            # Append the next_page url piece
            url = host + next_page
            response = requests.get(url)

            payload = response.json()["payload"]

            next_page = payload["next_page"]    
            contents = payload[a]
            
            # concating dataframes
            df = pd.concat([df, pd.DataFrame(contents)])

            df = df.reset_index(drop=True)

    return df
def merged_df():
    # Merging sales with stores:
    sales_stores = pd.merge(sales_df, 
                            stores_df,
                            how="inner",
                            left_on="store",
                            right_on="store_id")
    # Merge three alltogether
    sales_stores_items = pd.merge(sales_stores,
                          items_df,
                          how="inner",
                          left_on="item",
                          right_on="item_id")
    return sales_stores_items

def new_power_data():
    opsd = pd.read_csv("https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv")
    opsd = opsd.fillna(0)
    print("Saving data to .csv file")
    opsd.to_csv('opsd_germany_daily_data.csv', index=False)
    return opsd

def get_power_data():
    if os.path.isfile('opsd_germany_daily_data.csv') == False:
        print("Data is not cached. Acquiring new power data.")
        opsd = new_power_data()
    else:
        print("Data is cached. Reading data from .csv file.")
        opsd = pd.read_csv('opsd_germany_daily_data.csv')
    print("Acquisition complete")
    return opsd

