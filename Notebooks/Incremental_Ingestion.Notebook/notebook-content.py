# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a6e0edf8-f5fb-41ad-9df2-5ae1c31f3ec5",
# META       "default_lakehouse_name": "LakeHouse",
# META       "default_lakehouse_workspace_id": "bcb2bd9b-abe7-4e78-8a3c-5467343aba58",
# META       "known_lakehouses": [
# META         {
# META           "id": "a6e0edf8-f5fb-41ad-9df2-5ae1c31f3ec5"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Parameters

file_path = "/lakehouse/default/Files/FakeStore_Orders_OrderDetails_Customers_Products_Linked.xlsx"
batch_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os

for root, dirs, files in os.walk("/lakehouse/default/Files"):
    print("\nFolder:", root)
    for file in files:
        print("  ", file)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import hashlib
import os
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    DoubleType,
    BooleanType
)

# Generate a deterministic batch ID from the Excel file itself.
# Same file = same batch ID.
with open(file_path, "rb") as f:
    batch_id = hashlib.sha256(f.read()).hexdigest()

print("File:", file_path)
print("Batch ID:", batch_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

control_df = (
    spark.table("incremental_ingestion_control")
    .filter(F.col("IsActive") == 1)
)

control_rows = control_df.collect()

print("Active incremental tables:")
for row in control_rows:
    print(
        row["TableName"],
        "->",
        row["BronzeTable"]
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

excel_data = {}

for row in control_rows:

    table_name = row["TableName"]
    sheet_name = row["SheetName"]

    print(f"Reading Excel sheet: {sheet_name}")

    pdf = pd.read_excel(
        file_path,
        sheet_name=sheet_name
    )

    excel_data[table_name] = pdf

    print(
        f"{table_name}: "
        f"{len(pdf)} rows, "
        f"{len(pdf.columns)} columns"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build a mapping from Excel CustomerID -> existing Bronze CustomerID.
#
# For existing customers we match by CompanyName.
# For genuinely new customers, the Excel CustomerID is kept.

bronze_customers = spark.table("Bronze_Customers")

bronze_customer_map = (
    bronze_customers
    .select(
        F.col("CustomerID").cast("string").alias("BronzeCustomerID"),
        F.trim(F.col("CompanyName")).alias("CompanyName")
    )
    .dropDuplicates(["CompanyName"])
)

bronze_customer_map_rows = bronze_customer_map.collect()

company_to_bronze_id = {
    row["CompanyName"]: row["BronzeCustomerID"]
    for row in bronze_customer_map_rows
}

customer_pdf = excel_data["Customers"].copy()

customer_id_mapping = {}

for _, row in customer_pdf.iterrows():

    excel_customer_id = str(row["CustomerID"]).strip()
    company_name = str(row["CompanyName"]).strip()

    existing_id = company_to_bronze_id.get(company_name)

    if existing_id is not None:
        customer_id_mapping[excel_customer_id] = existing_id
    else:
        customer_id_mapping[excel_customer_id] = excel_customer_id

print("Customer ID mappings:")
for excel_id, bronze_id in customer_id_mapping.items():
    print(f"{excel_id} -> {bronze_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_pdf = excel_data["Customers"].copy()

customer_pdf["CustomerID"] = (
    customer_pdf["CustomerID"]
    .astype(str)
    .str.strip()
    .map(customer_id_mapping)
)

customer_pdf["SourceSystem"] = "Excel"
customer_pdf["SourceBatchId"] = batch_id

display(customer_pdf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders_pdf = excel_data["Orders"].copy()

orders_pdf["CustomerID"] = (
    orders_pdf["CustomerID"]
    .astype(str)
    .str.strip()
    .map(customer_id_mapping)
)

orders_pdf["SourceSystem"] = "Excel"
orders_pdf["SourceBatchId"] = batch_id

display(orders_pdf[[
    "OrderID",
    "CustomerID",
    "Operation",
    "ModifiedDate"
]])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products_pdf = excel_data["Products"].copy()

products_pdf["SourceSystem"] = "Excel"
products_pdf["SourceBatchId"] = batch_id

order_details_pdf = excel_data["OrderDetails"].copy()

order_details_pdf["SourceSystem"] = "Excel"
order_details_pdf["SourceBatchId"] = batch_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def append_incremental_table(pdf, bronze_table):

    # Convert pandas DataFrame to Spark DataFrame
    sdf = spark.createDataFrame(pdf)

    # Check whether this exact batch already exists.
    existing_batch = (
        spark.table(bronze_table)
        .filter(F.col("SourceBatchId") == batch_id)
        .limit(1)
        .count()
    )

    if existing_batch > 0:
        print(
            f"SKIP {bronze_table}: "
            f"batch {batch_id} already exists."
        )
        return

    print(
        f"APPENDING {len(pdf)} rows "
        f"to {bronze_table}"
    )

    (
        sdf.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(bronze_table)
    )

    print(f"SUCCESS: {bronze_table}")

    append_incremental_table(
    customer_pdf,
    "Bronze_Customers"
)

append_incremental_table(
    products_pdf,
    "Bronze_Products"
)

append_incremental_table(
    orders_pdf,
    "Bronze_Orders"
)

append_incremental_table(
    order_details_pdf,
    "Bronze_Order_Details"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
