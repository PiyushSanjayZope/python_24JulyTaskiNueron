import logging
import pymongo
import mysql.connector as conn
import pandas as pd
import json
import os


logging.basicConfig(filename="ineuron_task_24july.log", format='%(asctime)s : %(message)s', filemode="w", level=logging.DEBUG)
logger = logging.getLogger()


class Ineuron24JulyTask:

    def __init__(self, username, password):
        self.username = username
        self.password = password

    # Establish connection to MongoDB
    def mongodb_connection(self):
        try:
            client = pymongo.MongoClient("mongodb+srv://piyush:piyush123@cluster0.qdd7i.mongodb.net/?retryWrites=true&w=majority")
            logger.info("You are now connected with MongoDB")
            return client
        except Exception as e:
            logger.error("Error while connecting to MongoDB: " + str(e))

    # Establish connection to MySQL
    def mysqldb_connection(self):
        try:
            mysqldb = conn.connect(host="localhost", user=self.username, passwd=self.password)
            logger.info("You are now connected with MySQL")
            return mysqldb
        except Exception as e:
            logger.error("Error while connecting to MySQL: " + str(e))

    # Function to create table in MySQL
    def create_table(self,cursor):
        cursor.execute("create database if not exists shopping")
        logging.info("You have created database successfully")
        cursor.execute("use shopping")
        logging.info("shopping database is in use")
        attribute_dataset_table = "Dress_ID int, Style varchar(30), Price varchar(10), Rating FLOAT(5), Size varchar(10), Season varchar(20), NeckLine varchar(20), SleeveLength varchar(30), waiseline varchar(20), Material varchar(30), FabricType varchar(30), Decoration varchar(20), `Pattern Type` varchar(20), Recommendation int"
        cursor.execute("create table if not exists AttributeTable("+attribute_dataset_table+")")
        logging.info("AttributeTable creation done")
        dress_sale_dataset_table = "Dress_ID int,`29/8/2013` int,`31/8/2013` int,`2/9/2013` int,`4/9/2013` int,`6/9/2013` int,`8/9/2013` int,`10/9/2013` int,`12/9/2013` int,`14/9/2013` int,`16/9/2013` int,`18/9/2013` int,`20/9/2013` int,`22/9/2013` int,`24/9/2013` int,`26/9/2013` int,`28/9/2013` int,`30/9/2013` int,`2/10/2013` int,`4/10/2013` int,`6/10/2013` int,`8/10/2010` int,`10/10/2013` int,`12/10/2013` int"
        cursor.execute("create table if not exists DressSalesTable("+dress_sale_dataset_table+")")
        logging.info("DressSalesTable creation done")

    # Function to load the data into MySQL database table
    def bulk_load_data(self,cursor):
        attribute_dataset_df = pd.read_excel(r"D:\Next Steps\iNeuron\Datasets\Attribute DataSet.xlsx",header=None)
        attribute_dataset_df = attribute_dataset_df.fillna("none")
        dress_sale_dataset_df = pd.read_excel(r"D:\Next Steps\iNeuron\Datasets\Dress Sales.xlsx",header=None)
        dress_sale_dataset_df = dress_sale_dataset_df.fillna(0)

        for i in range(1, attribute_dataset_df.shape[0]):
            try:
                cursor.execute("insert into AttributeTable values" + str(tuple(attribute_dataset_df.loc[i])))
            except Exception as e:
                logging.error("Error while insertion of data into table " + str(e))

        for i in range(1, dress_sale_dataset_df.shape[0]):
            try:
                cursor.execute("insert into DressSalesTable values" + str(tuple(dress_sale_dataset_df.loc[i])))
            except Exception as e:
                logging.error("Error while insertion of data into table: " + str(e))

    # Function to read the data from dataset
    def read_dataset_pandas(self,conn):
        df_attribute_dataset = pd.read_sql("select * from shopping.AttributeTable", conn)
        print(df_attribute_dataset)
        df_dress_sale_dataset = pd.read_sql("select * from shopping.DressSalesTable", conn)
        print(df_dress_sale_dataset)
        logging.info("Data fetched successfully")

    # Function to convert data into JSON format
    def convert_to_json_upload_mongo(self,client):
        attribute_dataset_df = pd.read_excel(r"D:\Next Steps\iNeuron\Datasets\Attribute DataSet.xlsx")
        data = attribute_dataset_df.to_jsoxn(orient="records", default_handler=dict)
        logging.info("Data converted in to json")
        data = json.loads(data)
        print(data)
        client_db = client["shopping"]
        client_collection = client_db["attribute_data"]
        try:
            client_collection.insert_many(data)
            logging.info("Data upload successful")
        except Exception as e:
            logging.error("Error while inserting data in mongodb: " + str(e))

    # Function for LEFT Join
    def left_join(self,cursor):
        cursor.execute("select * from AttributeTable left join DressSalesTable on AttributeTable.Dress_ID = DressSalesTable.Dress_ID")
        logging.info("Left join performed now fetch the data")
        for row in cursor.fetchall():
            print(row)

    # Function to find unique dress based upon dress ID
    def find_unique_dress(self,cursor):
        cursor.execute("select distinct Dress_ID from DressSalesTable")
        logging.info("Successfully fetched the unique data based upon DressID")
        for row in cursor.fetchall():
            print(row)

    # Function to find dress having zero recommendation
    def find_zero_recommendation_dress(self,cursor):
        cursor.execute("select count(Dress_ID) from AttributeTable where recommendation =0")
        logging.info("No of dress having recommendation 0 detail fetched")
        print("No of dress having recommendation 0:", cursor.fetchone()[0])

    # Function to find total sell per ID
    def total_sell_per_id(self,cursor):
        cursor.execute("select dress_id,sum(`29/8/2013`  + `31/8/2013`  + `2/9/2013`  + `4/9/2013`  + `6/9/2013`  + `8/9/2013`  + `10/9/2013`  + `12/9/2013`  + `14/9/2013`  + `16/9/2013`  + `18/9/2013`  + `20/9/2013`  + `22/9/2013`  + `24/9/2013`  + `26/9/2013`  + `28/9/2013`  + `30/9/2013`  + `2/10/2013`  + `4/10/2013`  + `6/10/2013`  + `8/10/2010`  + `10/10/2013`  + `12/10/2013`) as `Total Sales` from DressSalesTable group by Dress_ID")
        for row in cursor.fetchall():
            print("Dress ID: ", row[0], "Sales: ", str(row[1]))

    # Function to find third highest selling dressid
    def third_highest_selling_dressid(self,cursor):
        cursor.execute("create view salesview as select Dress_ID,sum(`29/8/2013`  + `31/8/2013`  + `2/9/2013`  + `4/9/2013`  + `6/9/2013`  + `8/9/2013`  + `10/9/2013`  + `12/9/2013`  + `14/9/2013`  + `16/9/2013`  + `18/9/2013`  + `20/9/2013`  + `22/9/2013`  + `24/9/2013`  + `26/9/2013`  + `28/9/2013`  + `30/9/2013`  + `2/10/2013`  + `4/10/2013`  + `6/10/2013`  + `8/10/2010`  + `10/10/2013`  + `12/10/2013`) as `Total Sales` from DressSalesTable group by Dress_ID")
        cursor.execute("select dress_id, MIN(`Total sales`) from(select * from(select * from salesview order by `Total Sales` Desc) salesview limit 3) as a")
        result = cursor.fetchone()
        print("Thirst highest most selling dress id: ", result[0], "Sales amount: ", result[1])
        cursor.execute("drop view salesview")
        logging.info("Third highest saled dress details fetched")


# Code execution starts from here
if __name__ == '__main__':
    object = Ineuron24JulyTask("root", "Gerinfo@2022")
    conn = object.mysqldb_connection()
    if conn is not None:
        cursor = conn.cursor()
        object.create_table(cursor)
        object.bulk_load_data(cursor)
        conn.commit()
        cursor.execute("use shopping")
        object.read_dataset_pandas(conn)
        object.left_join(cursor)
        object.find_unique_dress(cursor)
        object.find_zero_recommendation_dress(cursor)
        object.total_sell_per_id(cursor)
    client = object.mongodb_connection()
    object.convert_to_json_upload_mongo(client)

    conn.close()
    client.close()