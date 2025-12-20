import pandas as pd
def transform_csv(df,save_path):
    #standardizing column names
    df.columns = df.columns.str.lower().str.replace(" " , "_").str.replace("-","_")

    #add total_sales column
    df['sales'] = df['sales'].astype(float)
    df['quantity'] = df['quantity'].astype(int)
    df["total_sales"] = df['sales'] * df['quantity']

    #convert string to Datetime
    df["order_date"] = pd.to_datetime(df["order_date"],format="%m/%d/%Y", errors="coerce")
    df["ship_date"] = pd.to_datetime(df["ship_date"],format="%m/%d/%Y", errors="coerce")

    #validation check
    assert df['sales'].min() >= 0
    assert df['quantity'].min() > 0

    #saved processed file
    df.to_csv(save_path,index=False)

    return df