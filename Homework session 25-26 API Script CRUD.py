from flask import request, jsonify
from flask import Flask
import pandas as pd
import psycopg2 as pg


def insert_into_table(conn, df, table):
    """
    Using cursor.mogrify() to build the bulk insert query
    then cursor.execute() to execute the query
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]

    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))

    # SQL query to execute
    cursor = conn.cursor()
    values = [cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
    print("VALUES", values)
    query = "INSERT INTO %s(%s) VALUES " % (table, cols) + ",".join(values)

    # print("QUERY", query)
    try:
        cursor.execute(query, tuples)
        conn.commit()
    except (Exception, pg.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("INSERT DONE")
    cursor.close()

 

# set up configuration
host = "localhost"
port = "5432"
database = "homework"
user = "postgres"
password = "Jakarta12"
setting = "dbname=" + database + " user=" + user + " host=" + host + " port=" + port + " password=" + password
engine = pg.connect(setting)

application = Flask(__name__)
@application.route('/digital-skola/read', methods=['GET'])
def read():
    content = request.get_json(force=True)
    order_id = content['order_id']
    query = f"""
            SELECT *
            FROM homework.public.olist_order_items_dataset a 
            
            where 
                 order_id = '{order_id}'
            
    """
    #--and product_category_name like '%{product_name}%'
    df = pd.read_sql(query, con=engine)
    
    #arr = [df['product_category_name'].iloc[0]]
    #print("DF ", df)

    
    try:
        arr = [df['price'].iloc[0]]
        result = {
            "status" : 200,
            "message" : "getting data succeed",
            "order id" : order_id,
            "price": arr
        }
    except IndexError:
        result = {
            "status" : 204,
            "message" : "Nodata for order id = "+ order_id,
            
        }


 
    return result

@application.route('/digital-skola/insert', methods=['POST'])
def insert():
    
    content = request.get_json(force=True)
    order_id = content['order_id']
    order_item_id= content['order_item_id']
    product_id = content['product_id']
    seller_id = content['seller_id']
    shipping_limit_date = content['shipping_limit_date']
    price = content['price']
    freight_value = content['freight_value']
    

    df = pd.DataFrame()

    df.at[(0,'order_id')] = order_id
    df.at[(0,'order_item_id')] = order_item_id
    df.at[(0,'product_id')] = product_id
    df.at[(0,'seller_id')] = seller_id
    df.at[(0,'shipping_limit_date')] = shipping_limit_date
    df.at[(0,'price')] = price
    df.at[(0,'freight_value')] = freight_value
    
    insert_into_table(engine, df, 'olist_order_items_dataset')
    
    print(freight_value)
    result = {
            
            "order_item_id": str(df['order_item_id'].iloc[0]),
            "product_id": str(df['product_id'].iloc[0]),
            "seller_id": str(df['seller_id'].iloc[0]),
            "shipping_limit_date": str(df['shipping_limit_date'].iloc[0]),
            "price": str(df['price'].iloc[0]),
            "freight_value": str(df['freight_value'].iloc[0]),
            
        }


 
    return result

@application.route('/digital-skola/edit', methods=['PUT'])
def edit():
    content = request.get_json(force=True)
    order_id = content['order_id']
    price = content['price']
    query = f"""
            update olist_order_items_dataset 
            set price = '{price}'
            where 
                 order_id = '{order_id}'
            
    """
    conn=engine
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
 
    
    result = {
            "status" : 200,
            "message" : "update data succeed",
            "order id" : order_id,
            "price": price
        }
     


 
    return result

if __name__ == '__main__':
    application.run(host='0.0.0.0')
