import pandas as pd
def load_to_mysql(df,connection):
    df = df.where(pd.notnull(df), None)

    cursor = connection.cursor()

    insert_query = """
        INSERT IGNORE INTO sales (row_id, order_id, order_date, ship_date, ship_mode,
       customer_id, customer_name, segment, country, city, state,
       postal_code, region, product_id, category, sub_category,
       product_name, sales, quantity, discount, profit,total_sales)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    data = [
        (
            row['row_id'],
            row['order_id'],
            row['order_date'].date() if row['order_date'] else None,
            row['ship_date'].date() if row['ship_date'] else None,
            row['ship_mode'],
      
            row['customer_id'],
            row['customer_name'],
            row['segment'],
            row['country'],
            row['city'],
            
            row['state'],
            row['postal_code'],
            row['region'],
            row['product_id'],
            row['category'],
      
            row['sub_category'],
            row['product_name'],
            row['sales'],
            row['quantity'],
            row['discount'],
 
            row['profit'],
            row['total_sales'])
        for _, row in df.iterrows()
    ]

    cursor.executemany(insert_query,data)
    connection.commit()
    cursor.close()