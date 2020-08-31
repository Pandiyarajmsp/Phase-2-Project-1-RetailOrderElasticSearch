from __future__ import print_function
from pyspark.sql.types import *
# from pyspark import SparkConf
# import configparser
#import pandas as pd
from pyspark.sql import *
import re
import sys
# import os
import datetime
from pyspark import StorageLevel
from pyspark.sql.functions import *
from pyspark.sql import HiveContext
from pyspark import SparkConf

#======================================Enable on Windows===============================================

# Please change the below variable value based on your project structure and storage level
# Windows   ---- enable below variables and spark builder when using windows


Mastername="local[1]"
appName="Spark_Hackathon"
insuranceinfo1 = "SourceFile/insuranceinfo1.csv"
insuranceinfo2 = "SourceFile/insuranceinfo2.csv"
cust_states = "SourceFile/custs_states.csv"
outputfolderJSON="SourceFile/output/JSON"
outputfolderCSV="SourceFile/output/CSV"
outputfolderparquet="sample_data/output/parquet"

# def spark_builder(Mastername,appName):
#     try:
#         spark = SparkSession.builder.master(Mastername).appName(appName).getOrCreate()
#         return spark
#     except Exception as spark_error:
#         print("Unable to create spark session. \n {}".format(spark_error))
#
#
# spark = spark_builder(Mastername,appName)
# sc = spark.sparkContext

spark=SparkSession.builder.appName("RetailOrderElasticSearch").config("hive.metastore.uris", "thrift://localhost:9083",conf=SparkConf()).enableHiveSupport().getOrCreate()
sc = spark.sparkContext
spark.sparkContext.setLogLevel("ERROR")

# ===========================Enable in VM=================================================

# VM   ---- enable below variables and create folder when using VM
#hadoop fs -mkdir -p /user/hduser/hackathon/source/
#hadoop fs -mkdir -p /user/hduser/hackathon/output/JSON
#hadoop fs -mkdir -p /user/hduser/hackathon/output/CSV
#hadoop fs -mkdir -p /user/hduser/hackathon/output/parquet

#
# insuranceinfo1 = "hdfs://localhost:54310/user/hduser/hackathon/source/insuranceinfo1.csv"
# insuranceinfo2 = "hdfs://localhost:54310/user/hduser/hackathon/source/insuranceinfo2.csv"
# cust_states = "hdfs://localhost:54310/user/hduser/hackathon/source/custs_states.csv"
# outputfolderJSON="hdfs://localhost:54310/user/hduser/hackathon/output/JSON"
# outputfolderCSV="hdfs://localhost:54310/user/hduser/hackathon/output/CSV"
# outputfolderparquet= "hdfs://localhost:54310/user/hduser/hackathon/output/parquet"
#
# #spark=SparkSession.builder.appName("Pandi_Hackathon").enableHiveSupport().getOrCreate()
# spark=SparkSession.builder.appName("Pandi_Hackathon").config("hive.metastore.uris", "thrift://localhost:9083",conf=SparkConf()).enableHiveSupport().getOrCreate()
# sc = spark.sparkContext
# spark.sparkContext.setLogLevel("ERROR")


def getRdbmsData(spark,DatabaseName, TableName, PartitionColumn):

    Driver = "com.mysql.jdbc.Driver"
    Host = "localhost"
    Port = "3306"
    User = "root"
    Pass = "root"
    url = "jdbc:mysql://{0}:{1}/{2}".format(Host, Port, DatabaseName)
    df_db = spark.read.format("jdbc") \
     .option("driver", Driver) \
     .option("url", url) \
     .option("user", User) \
     .option("lowerBound", 1) \
     .option("upperBound", 10000) \
     .option("numPartitions", 4) \
     .option("partitionColumn", PartitionColumn) \
     .option("password", Pass) \
     .option("dbtable", TableName).load()
    return df_db

def processCustPayData(spark):
    #processing customer details and registering as custdetcomplextypesp.qua
    spark.sql("""select customerNumber,customerName,concat(contactFirstName,' ',contactLastName) as
    contactfullname,addressLine1,city,state,postalCode,country,phone,creditLimit,checknumber,amount,paymentdate from cust_payment
    """).createOrReplaceTempView("custdetcomplextypes")
    #processing order details and registering as orddetcomplextypes
    spark.sql("""select
    customernumber,ordernumber,shippeddate,status,comments,productcode,quantityordered,priceeach,orderlinenumber,productName,
    productLine,productScale,productVendor,productDescription,quantityInStock,buyPrice,MSRP,orderdate from
    order_products""").createOrReplaceTempView("orddetcomplextypes")
    custpaymentcomplextypes = spark.sql("""select CONCAT(customerNumber, '~', checknumber, '~', CONCAT(creditLimit, '$', amount), '~',
    paymentdate) from cust_payment""")
    custpaymentcomplextypes.write.mode("overwrite").option("header","true").csv("hdfs://localhost:54310/user/hduser/output/CustPayment")
    #processing order details and registering as orddetcomplextypes
    spark.sql("""select
    customernumber,ordernumber,shippeddate,status,comments,productcode,quantityordered,priceeach,orderlinenumber,productName,
    productLine,productScale,productVendor,
    productDescription,quantityInStock,buyPrice,MSRP,orderdate from order_products""").createOrReplaceTempView("""orddetcomplextypes""")
    # getting customer payment detail and saving as CSV file
    custpaymentcomplextypes = spark.sql("""select
    CONCAT(customerNumber,'~',checknumber,'~',CONCAT(creditLimit,'$',amount),'~',paymentdate) from cust_payment""")
    custpaymentcomplextypes.write.mode("overwrite").option("header","true").csv("hdfs://localhost:54310/user/hduser/output/CustPayment")
    # creating retail_mart and custordfinal table in hive
    spark.sql("""create database if not exists retail_mart""")
    spark.sql("""create external table IF NOT EXISTS retail_mart.custordfinal (customernumber STRING, customername
    STRING, contactfullname string, addressLine1 string,city string,state string,country string,phone bigint,creditlimit
    float,checknum string,checkamt int,ordernumber STRING,shippeddate date,status string, comments string,productcode
    string,quantityordered int,priceeach double,orderlinenumber int,productName STRING,productLine STRING,productScale
    STRING,productVendor STRING,productDescription STRING,quantityInStock int,buyPrice double,MSRP double,orderdate
    date) stored as orcfile location 'hdfs://localhost:54310/user/hive/warehouse/retail_mart.db/custordfinal/'""")
    # Processing cust and order data and loading into orc table
    custfinal = spark.sql("""insert into retail_mart.custordfinal select cd.customernumber customernumber,cd.customername
    customername,cd.contactfullname contactfullname,cd.addressLine1 addressLine1,cd.city city,cd.state state,cd.country
    country,cast(cd.phone as bigint) phone,cast(cd.creditlimit as float) creditlimit,cd.checknumber checknum,cast(cd.amount as
    int) checkamt,o.ordernumber ordernumber,cast(o.shippeddate as date)
    shippeddate,o.status,o.comments,o.productcode,cast(o.quantityordered as int) quantityordered,cast(o.priceeach as double)
    priceeach,cast(o.orderlinenumber as int) orderlinenumber,o.productName
    ,o.productLine,productScale,o.productVendor,o.productDescription,cast(o.quantityInStock as int)
    quantityInStock,cast(o.buyPrice as double) buyPrice,cast(o.MSRP as double) MSRP,cast(o.orderdate as date) orderdate from
    custdetcomplextypes cd inner join orddetcomplextypes o on cd.customernumber=o.customernumber""")
    custfinales = spark.sql("""select cd.customernumber customernumber,cd.customername customername,cd.city
    city,cast(o.quantityInStock as int) quantityInStock,cast(o.MSRP as double) MSRP,cast(o.orderdate as date) orderdate from
    custdetcomplextypes cd inner join orddetcomplextypes o on cd.customernumber=o.customernumber""")
    writetoes(custfinales)
    print("""Calling the writetoes function to load the custfinales dataframe into Elastic search index""")


def writetoes(df):
    df.write.mode("overwrite").format("org.elasticsearch.spark.sql").option("es.resource", "custfinales2/doc1").save()


def main():
    db1 = getRdbmsData(spark,"empoffice","employees","employeeNumber")
    db1.createOrReplaceTempView("employee")
    print("Employee data successfully pulled from Mysql!!!" )
    #calling method to pull offices table data from mysql db under empoffice schema
    db2 = getRdbmsData(spark,"empoffice","offices","officeCode")
    db2.createOrReplaceTempView("offices")
    print("Offices data successfully pulled from Mysql!!!")
    #calling method to pull data from mysql db under custpayments schema using custom sql
    custSql="""(select c.customerNumber, upper(c.customerName) as customerName,c.contactFirstName,
            c.contactLastName,c.phone,c.addressLine1,c.city as city,c.state,c.postalCode,c.country
            ,c.salesRepEmployeeNumber,c.creditLimit ,p.checknumber,p.paymentdate,p.amount from customers c inner join payments p
            on c.customernumber=p.customernumber) as cust_pay"""
    db3= getRdbmsData(spark,"custpayments",custSql,"city")
    db3.createOrReplaceTempView("cust_payment") #registering as temporary table
    print("Customer and payment joined data successfully pulled from Mysql!!!")
    #calling method to pull data from mysql db under ordersproducts schema using custom sql
    ordSql="""(select o.customernumber,o.ordernumber,o.orderdate as
    orderdate,o.shippeddate,o.status,o.comments,od.quantityordered,od.priceeach,
    od.orderlinenumber,p.productCode,p.productName,p.productLine,p.productScale,p.productVendor,p.productDescription,p.quantityInStock,p.
    buyPrice,p.MSRP from orders o
    inner join orderdetails od on o.ordernumber=od.ordernumber inner join
    products p on od.productCode=p.productCode) as order_prod"""
    db4= getRdbmsData(spark,"ordersproducts",ordSql,"orderdate")
    db4.createOrReplaceTempView("order_products") #registering as temporary table
    print("Order data successfully pulled from Mysql!!!")
    #Calling method to process customer data
    processCustPayData(spark)
    print("Customer data has been processed successfully and loaded into retail_mart.custordfinal Hive table")
    print("Cutomer payment details availabe in /user/hduser/output/CustPayment file")


main()