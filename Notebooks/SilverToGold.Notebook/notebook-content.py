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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "1f8bf7b0-b586-b7b0-4c90-336086c1f6ce",
# META       "known_warehouses": [
# META         {
# META           "id": "1f8bf7b0-b586-b7b0-4c90-336086c1f6ce",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    DoubleType,
    DecimalType,
    DateType
)

from delta.tables import DeltaTable

from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GOLD CONFIGURATION
# ============================================================

WAREHOUSE_NAME = "Data Warehouse"

print(f"Warehouse destination: {WAREHOUSE_NAME}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# SILVER TABLES USED BY GOLD
# ============================================================

SILVER_TABLES = {
    "categories": "silver_categories",
    "customers": "silver_customers",
    "employees": "silver_employees",
    "order_details": "silver_order_details",
    "orders": "silver_orders",
    "products": "silver_products",
    "regions": "silver_regions",
    "shippers": "silver_shippers",
    "suppliers": "silver_suppliers",
    "territories": "silver_territories"
}

print("Silver tables configured:")
for name, table in SILVER_TABLES.items():
    print(f"{name:20} -> {table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# READ SILVER TABLES
# ============================================================

silver_categories = spark.table(SILVER_TABLES["categories"])
silver_customers = spark.table(SILVER_TABLES["customers"])
silver_employees = spark.table(SILVER_TABLES["employees"])
silver_order_details = spark.table(SILVER_TABLES["order_details"])
silver_orders = spark.table(SILVER_TABLES["orders"])
silver_products = spark.table(SILVER_TABLES["products"])
silver_regions = spark.table(SILVER_TABLES["regions"])
silver_shippers = spark.table(SILVER_TABLES["shippers"])
silver_suppliers = spark.table(SILVER_TABLES["suppliers"])
silver_territories = spark.table(SILVER_TABLES["territories"])

print("All Silver tables loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CURRENT SILVER RECORDS
# ============================================================

def current_records(df):
    return df.filter(F.col("IsCurrent") == True)


customers_current = current_records(silver_customers)
employees_current = current_records(silver_employees)
products_current = current_records(silver_products)
categories_current = current_records(silver_categories)
regions_current = current_records(silver_regions)
shippers_current = current_records(silver_shippers)
suppliers_current = current_records(silver_suppliers)
territories_current = current_records(silver_territories)
orders_current = current_records(silver_orders)
order_details_current = current_records(silver_order_details)

print("Current Silver records prepared.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# DIM CUSTOMER
# ============================================================

dim_customer = (
    customers_current
    .select(
        "CustomerID",
        "CompanyName",
        "ContactName",
        "ContactTitle",
        "Address",
        "City",
        "Region",
        "PostalCode",
        "Country",
        "Phone",
        "Fax",
        "StartDate",
        "EndDate",
        "IsCurrent"
    )
)

print("DimCustomer rows:", dim_customer.count())
dim_customer.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# DIM EMPLOYEE
# ============================================================

dim_employee = (
    employees_current
    .select(
        "EmployeeID",
        "LastName",
        "FirstName",
        "Title",
        "TitleOfCourtesy",
        "BirthDate",
        "HireDate",
        "Address",
        "City",
        "Region",
        "PostalCode",
        "Country",
        "HomePhone",
        "Extension",
        "Notes",
        "ReportsTo",
        "ReportsToMissing",
        "StartDate",
        "EndDate",
        "IsCurrent"
    )
)

print("DimEmployee rows:", dim_employee.count())
dim_employee.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# DIM PRODUCT
# ============================================================

dim_product = (
    products_current
    .select(
        "ProductID",
        "ProductName",
        "SupplierID",
        "CategoryID",
        "QuantityPerUnit",
        "UnitPrice",
        "UnitsInStock",
        "UnitsOnOrder",
        "ReorderLevel",
        "Discontinued",
        "StartDate",
        "EndDate",
        "IsCurrent"
    )
)

print("DimProduct rows:", dim_product.count())
dim_product.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# DIM CATEGORY
# ============================================================

dim_category = (
    categories_current
    .select(
        "CategoryID",
        "CategoryName",
        "Description",
        "StartDate",
        "EndDate",
        "IsCurrent"
    )
)

print("DimCategory rows:", dim_category.count())
dim_category.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# DIM SUPPLIER
# ============================================================

dim_supplier = (
    suppliers_current
    .select(
        "SupplierID",
        "CompanyName",
        "ContactName",
        "ContactTitle",
        "Address",
        "City",
        "Region",
        "PostalCode",
        "Country",
        "Phone",
        "Fax",
        "HomePage",
        "StartDate",
        "EndDate",
        "IsCurrent"
    )
)

print("DimSupplier rows:", dim_supplier.count())
dim_supplier.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# DIM SHIPPER
# ============================================================

dim_shipper = (
    shippers_current
    .select(
        "ShipperID",
        "CompanyName",
        "Phone",
        "StartDate",
        "EndDate",
        "IsCurrent"
    )
)

print("DimShipper rows:", dim_shipper.count())
dim_shipper.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# DIM DATE
# ============================================================

# ------------------------------------------------------------
# Determine the required date range
#
# Fact_Orders uses three date columns:
#
#   OrderDate
#   RequiredDate
#   ShippedDate
#
# Therefore Dim_Date must cover all three.
#
# IMPORTANT:
# 9999-12-31 is treated as a sentinel value by the source
# system and must NOT be used to extend the date dimension.
# Otherwise the dimension becomes unnecessarily huge.
# ------------------------------------------------------------


# ------------------------------------------------------------
# Get all relevant dates from Fact_Orders
# ------------------------------------------------------------

order_dates = (
    orders_current
    .select(
        F.to_date("OrderDate").alias("Date")
    )
    .filter(
        F.col("Date").isNotNull()
    )
    .filter(
        F.col("Date") != F.lit("9999-12-31").cast("date")
    )
)


required_dates = (
    orders_current
    .select(
        F.to_date("RequiredDate").alias("Date")
    )
    .filter(
        F.col("Date").isNotNull()
    )
    .filter(
        F.col("Date") != F.lit("9999-12-31").cast("date")
    )
)


shipped_dates = (
    orders_current
    .select(
        F.to_date("ShippedDate").alias("Date")
    )
    .filter(
        F.col("Date").isNotNull()
    )
    .filter(
        F.col("Date") != F.lit("9999-12-31").cast("date")
    )
)


# ------------------------------------------------------------
# Combine all date roles
# ------------------------------------------------------------

all_fact_dates = (
    order_dates
    .union(required_dates)
    .union(shipped_dates)
)


# ------------------------------------------------------------
# Find overall minimum and maximum date
# ------------------------------------------------------------

date_range = (
    all_fact_dates
    .agg(
        F.min("Date").alias("MinDate"),
        F.max("Date").alias("MaxDate")
    )
    .collect()[0]
)


date_min = date_range["MinDate"]
date_max = date_range["MaxDate"]


print(
    "Minimum required date:",
    date_min
)

print(
    "Maximum required date:",
    date_max
)


# ------------------------------------------------------------
# Safety check
# ------------------------------------------------------------

if date_min is None or date_max is None:

    raise ValueError(
        "Could not determine a valid date range "
        "for Dim_Date."
    )


print()
print(
    "Dim_Date will cover all dates required by "
    "Fact_Orders."
)


# ------------------------------------------------------------
# Generate one row for every date in the required range
# ------------------------------------------------------------

date_df = (
    spark.sql(
        f"""
        SELECT explode(
            sequence(
                to_date('{date_min}'),
                to_date('{date_max}'),
                interval 1 day
            )
        ) AS Date
        """
    )
)


# ------------------------------------------------------------
# Build Dim_Date
# ------------------------------------------------------------

dim_date = (
    date_df

    # Surrogate date key: YYYYMMDD
    .withColumn(
        "DateKey",
        F.date_format(
            "Date",
            "yyyyMMdd"
        ).cast("int")
    )

    # Calendar attributes
    .withColumn(
        "Year",
        F.year("Date")
    )

    .withColumn(
        "Quarter",
        F.quarter("Date")
    )

    .withColumn(
        "Month",
        F.month("Date")
    )

    .withColumn(
        "MonthName",
        F.date_format(
            "Date",
            "MMMM"
        )
    )

    # Month number
    .withColumn(
        "MonthNumber",
        F.month("Date")
    )

    # Week number
    .withColumn(
        "WeekNumber",
        F.weekofyear("Date")
    )

    .withColumn(
        "Day",
        F.dayofmonth("Date")
    )

    .withColumn(
        "DayName",
        F.date_format(
            "Date",
            "EEEE"
        )
    )

    # Weekend flag
    # dayofweek:
    # 1 = Sunday
    # 7 = Saturday
    .withColumn(
        "IsWeekend",
        F.when(
            F.dayofweek("Date").isin([1, 7]),
            F.lit(True)
        ).otherwise(
            F.lit(False)
        )
    )

    # YYYY-MM
    .withColumn(
        "YearMonth",
        F.date_format(
            "Date",
            "yyyy-MM"
        )
    )

    # YYYY-Qn
    .withColumn(
        "YearQuarter",
        F.concat(
            F.year("Date").cast("string"),
            F.lit("-Q"),
            F.quarter("Date").cast("string")
        )
    )
)


# ------------------------------------------------------------
# Match Warehouse column order exactly
# ------------------------------------------------------------

dim_date = dim_date.select(
    "DateKey",
    "Date",
    "Year",
    "Quarter",
    "Month",
    "MonthName",
    "MonthNumber",
    "WeekNumber",
    "Day",
    "DayName",
    "IsWeekend",
    "YearMonth",
    "YearQuarter"
)


# ------------------------------------------------------------
# Final information
# ------------------------------------------------------------

print(
    "Dim_Date rows:",
    dim_date.count()
)

print(
    "Dim_Date minimum:",
    date_min
)

print(
    "Dim_Date maximum:",
    date_max
)

dim_date.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

WAREHOUSE_SERVER = "pecotg37dq5efkaj45i3vxdg4a-to63fphhvn4e5cr4krttiov2la.datawarehouse.fabric.microsoft.com"
print("Warehouse server:", WAREHOUSE_SERVER)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyodbc
import struct
import pandas as pd
# ============================================================
# WAREHOUSE CONNECTION
# ============================================================

def get_warehouse_connection():

    token = notebookutils.credentials.getToken(
        "https://database.windows.net/"
    )
    if not token:
        raise RuntimeError(
            "Could not obtain an access token for the Warehouse."
        )

    # SQL_COPT_SS_ACCESS_TOKEN expects UTF-16-LE
    token_bytes = token.encode("utf-16-le")

    SQL_COPT_SS_ACCESS_TOKEN = 1256

    access_token = struct.pack(
        f"<I{len(token_bytes)}s",
        len(token_bytes),
        token_bytes
    )

    connection_string = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{WAREHOUSE_SERVER},1433;"
        f"Database={WAREHOUSE_NAME};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    return pyodbc.connect(
        connection_string,
        attrs_before={
            SQL_COPT_SS_ACCESS_TOKEN: access_token
        }
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# FACT_ORDER_DETAILS — WAREHOUSE SURROGATE KEY LOOKUPS
# ============================================================
#
# IMPORTANT:
# ProductKey and CustomerKey are IDENTITY keys generated
# inside the Warehouse.
#
# Therefore they do NOT exist in dim_product / dim_customer
# Spark DataFrames.
#
# We read the actual mappings from the Warehouse instead.
# ============================================================

import pandas as pd
from pyspark.sql import functions as F


print("=" * 80)
print("BUILDING FACT_ORDER_DETAILS")
print("=" * 80)


# ============================================================
# 1. READ SURROGATE KEY MAPPINGS FROM WAREHOUSE
# ============================================================

connection = get_warehouse_connection()

try:

    print()
    print("Reading ProductID → ProductKey from Warehouse...")

    product_mapping = pd.read_sql(
        """
        SELECT
            ProductID,
            ProductKey
        FROM dbo.Dim_Product
        WHERE IsCurrent = 1
        """,
        connection
    )

    print(
        f"Product mappings loaded: "
        f"{len(product_mapping)}"
    )


    print()
    print("Reading CustomerID → CustomerKey from Warehouse...")

    customer_mapping = pd.read_sql(
        """
        SELECT
            CustomerID,
            CustomerKey
        FROM dbo.Dim_Customer
        WHERE IsCurrent = 1
        """,
        connection
    )

    print(
        f"Customer mappings loaded: "
        f"{len(customer_mapping)}"
    )


    print()
    print("Reading DateKey mappings from Warehouse...")

    date_mapping = pd.read_sql(
        """
        SELECT
            DateKey,
            [Date]
        FROM dbo.Dim_Date
        """,
        connection
    )

    print(
        f"Date mappings loaded: "
        f"{len(date_mapping)}"
    )


finally:

    connection.close()

    print()
    print("Warehouse connection closed.")


# ============================================================
# 2. CONVERT MAPPINGS TO SPARK DATAFRAMES
# ============================================================

product_mapping_spark = spark.createDataFrame(
    product_mapping
)

customer_mapping_spark = spark.createDataFrame(
    customer_mapping
)

date_mapping_spark = spark.createDataFrame(
    date_mapping
)


# ============================================================
# 3. PREPARE ORDER DETAILS
# ============================================================

fact_sales = (
    order_details_current

    # --------------------------------------------------------
    # ProductID → Warehouse ProductKey
    # --------------------------------------------------------

    .join(
        product_mapping_spark.select(
            "ProductID",
            "ProductKey"
        ),
        on="ProductID",
        how="left"
    )

    # --------------------------------------------------------
    # OrderID → CustomerID
    # --------------------------------------------------------

    .join(
        orders_current.select(
            "OrderID",
            "CustomerID"
        ),
        on="OrderID",
        how="left"
    )

    # --------------------------------------------------------
    # CustomerID → Warehouse CustomerKey
    # --------------------------------------------------------

    .join(
        customer_mapping_spark.select(
            "CustomerID",
            "CustomerKey"
        ),
        on="CustomerID",
        how="left"
    )

    # --------------------------------------------------------
    # OrderID → OrderDate
    # --------------------------------------------------------

    .join(
        orders_current.select(
            "OrderID",
            "OrderDate"
        ),
        on="OrderID",
        how="left"
    )

    # --------------------------------------------------------
    # OrderDate → Warehouse DateKey
    # --------------------------------------------------------

    .join(
        date_mapping_spark.select(
            F.col("Date").alias("OrderDate"),
            F.col("DateKey").alias("OrderDateKey")
        ),
        on="OrderDate",
        how="left"
    )
)


# ============================================================
# 4. VALIDATE PRODUCT KEY LOOKUP
# ============================================================

missing_product_key = fact_sales.filter(
    F.col("ProductKey").isNull()
)

missing_product_count = missing_product_key.count()

if missing_product_count > 0:

    print()
    print("ERROR: ProductKey lookup failed.")

    missing_product_key.select(
        "OrderID",
        "ProductID"
    ).show(20, truncate=False)

    raise ValueError(
        f"STOP: {missing_product_count} fact rows "
        f"could not be mapped to Dim_Product."
    )

else:

    print()
    print("ProductID → ProductKey: PASSED")


# ============================================================
# 5. VALIDATE CUSTOMER KEY LOOKUP
# ============================================================

missing_customer_key = fact_sales.filter(
    F.col("CustomerKey").isNull()
)

missing_customer_count = missing_customer_key.count()

if missing_customer_count > 0:

    print()
    print("ERROR: CustomerKey lookup failed.")

    missing_customer_key.select(
        "OrderID",
        "CustomerID"
    ).show(20, truncate=False)

    raise ValueError(
        f"STOP: {missing_customer_count} fact rows "
        f"could not be mapped to Dim_Customer."
    )

else:

    print()
    print("CustomerID → CustomerKey: PASSED")


# ============================================================
# 6. VALIDATE DATE KEY LOOKUP
# ============================================================

missing_date_key = fact_sales.filter(
    F.col("OrderDateKey").isNull()
)

missing_date_count = missing_date_key.count()

if missing_date_count > 0:

    print()
    print("ERROR: OrderDateKey lookup failed.")

    missing_date_key.select(
        "OrderID",
        "OrderDate"
    ).show(20, truncate=False)

    raise ValueError(
        f"STOP: {missing_date_count} fact rows "
        f"could not be mapped to Dim_Date."
    )

else:

    print()
    print("OrderDate → OrderDateKey: PASSED")


# ============================================================
# 7. FINAL FACT SHAPE
# ============================================================

fact_sales = fact_sales.select(
    "OrderID",
    "ProductKey",
    "CustomerKey",
    "OrderDateKey",
    "UnitPrice",
    "Quantity",
    "Discount"
)


# ============================================================
# 8. LINE COUNT
# ============================================================

fact_sales = fact_sales.withColumn(
    "LineCount",
    F.lit(1)
)


# ============================================================
# 9. FINAL CHECK
# ============================================================

print()
print("=" * 80)
print("FACT_ORDER_DETAILS READY")
print("=" * 80)

print(
    f"Rows: {fact_sales.count()}"
)

fact_sales.printSchema()

fact_sales.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# FACT_ORDERS — WAREHOUSE SURROGATE KEY LOOKUPS
# ============================================================
#
# IMPORTANT:
# CustomerKey, EmployeeKey and ShipperKey are IDENTITY keys
# generated by the Warehouse.
#
# Therefore, they are NOT available inside the Spark
# dimension DataFrames.
#
# We read the real surrogate-key mappings from the Warehouse.
# ============================================================

import pandas as pd
from pyspark.sql import functions as F


print("=" * 80)
print("BUILDING FACT_ORDERS")
print("=" * 80)


# ============================================================
# 1. READ SURROGATE KEY MAPPINGS FROM WAREHOUSE
# ============================================================

connection = get_warehouse_connection()

try:

    # --------------------------------------------------------
    # CustomerID → CustomerKey
    # --------------------------------------------------------

    print()
    print("Reading CustomerID → CustomerKey...")

    customer_mapping = pd.read_sql(
        """
        SELECT
            CustomerID,
            CustomerKey
        FROM dbo.Dim_Customer
        WHERE IsCurrent = 1
        """,
        connection
    )

    print(
        f"Customer mappings loaded: "
        f"{len(customer_mapping)}"
    )


    # --------------------------------------------------------
    # EmployeeID → EmployeeKey
    # --------------------------------------------------------

    print()
    print("Reading EmployeeID → EmployeeKey...")

    employee_mapping = pd.read_sql(
        """
        SELECT
            EmployeeID,
            EmployeeKey
        FROM dbo.Dim_Employee
        WHERE IsCurrent = 1
        """,
        connection
    )

    print(
        f"Employee mappings loaded: "
        f"{len(employee_mapping)}"
    )


    # --------------------------------------------------------
    # ShipperID → ShipperKey
    #
    # Fact_Orders contains ShipVia.
    # ShipVia corresponds to ShipperID.
    # --------------------------------------------------------

    print()
    print("Reading ShipperID → ShipperKey...")

    shipper_mapping = pd.read_sql(
        """
        SELECT
            ShipperID,
            ShipperKey
        FROM dbo.Dim_Shipper
        WHERE IsCurrent = 1
        """,
        connection
    )

    print(
        f"Shipper mappings loaded: "
        f"{len(shipper_mapping)}"
    )


    # --------------------------------------------------------
    # Date → DateKey
    # --------------------------------------------------------

    print()
    print("Reading Date → DateKey...")

    date_mapping = pd.read_sql(
        """
        SELECT
            DateKey,
            [Date]
        FROM dbo.Dim_Date
        """,
        connection
    )

    print(
        f"Date mappings loaded: "
        f"{len(date_mapping)}"
    )


finally:

    connection.close()

    print()
    print("Warehouse connection closed.")


# ============================================================
# 2. CONVERT WAREHOUSE MAPPINGS TO SPARK DATAFRAMES
# ============================================================

customer_mapping_spark = spark.createDataFrame(
    customer_mapping
)

employee_mapping_spark = spark.createDataFrame(
    employee_mapping
)

shipper_mapping_spark = spark.createDataFrame(
    shipper_mapping
)

date_mapping_spark = spark.createDataFrame(
    date_mapping
)


# ============================================================
# 3. PREPARE FACT_ORDERS
# ============================================================

fact_orders = orders_current


# ============================================================
# CUSTOMER
# CustomerID → CustomerKey
# ============================================================

fact_orders = fact_orders.join(
    customer_mapping_spark.select(
        "CustomerID",
        "CustomerKey"
    ),
    on="CustomerID",
    how="left"
)


# ============================================================
# EMPLOYEE
# EmployeeID → EmployeeKey
# ============================================================

fact_orders = fact_orders.join(
    employee_mapping_spark.select(
        "EmployeeID",
        "EmployeeKey"
    ),
    on="EmployeeID",
    how="left"
)


# ============================================================
# SHIPPER
# ShipVia → ShipperID → ShipperKey
# ============================================================

fact_orders = fact_orders.join(
    shipper_mapping_spark.select(
        F.col("ShipperID").alias("ShipVia"),
        F.col("ShipperKey")
    ),
    on="ShipVia",
    how="left"
)


# ============================================================
# ORDER DATE
# OrderDate → OrderDateKey
# ============================================================

fact_orders = fact_orders.join(
    date_mapping_spark.select(
        F.col("Date").alias("OrderDate"),
        F.col("DateKey").alias("OrderDateKey")
    ),
    on="OrderDate",
    how="left"
)


# ============================================================
# REQUIRED DATE
# RequiredDate → RequiredDateKey
# ============================================================

fact_orders = fact_orders.join(
    date_mapping_spark.select(
        F.col("Date").alias("RequiredDate"),
        F.col("DateKey").alias("RequiredDateKey")
    ),
    on="RequiredDate",
    how="left"
)


# ============================================================
# SHIPPED DATE
# ============================================================
#
# 9999-12-31 is used in the source data as a sentinel value
# meaning that the order has not actually been shipped.
#
# Therefore:
#
#   Real ShippedDate  → Dim_Date → ShippedDateKey
#   9999-12-31        → NULL ShippedDateKey
#
# This prevents the sentinel date from being treated as a
# real calendar date.
# ============================================================

fact_orders = (
    fact_orders

    # Convert the sentinel date to NULL before the lookup
    .withColumn(
        "ShippedDateLookup",
        F.when(
            F.to_date("ShippedDate") == F.lit("9999-12-31").cast("date"),
            F.lit(None).cast("date")
        ).otherwise(
            F.to_date("ShippedDate")
        )
    )

    .join(
        date_mapping_spark.select(
            F.col("Date").alias("ShippedDateLookup"),
            F.col("DateKey").alias("ShippedDateKey")
        ),
        on="ShippedDateLookup",
        how="left"
    )

    # Remove temporary lookup column
    .drop("ShippedDateLookup")
)

# ============================================================
# 4. VALIDATE CUSTOMER KEY
# ============================================================

missing_customer_key = fact_orders.filter(
    F.col("CustomerKey").isNull()
)

missing_count = missing_customer_key.count()

if missing_count > 0:

    print()
    print("ERROR: CustomerKey lookup failed.")

    missing_customer_key.select(
        "OrderID",
        "CustomerID"
    ).show(20, truncate=False)

    raise ValueError(
        f"STOP: {missing_count} orders could not be "
        f"mapped to Dim_Customer."
    )

else:

    print()
    print("CustomerID → CustomerKey: PASSED")


# ============================================================
# 5. VALIDATE EMPLOYEE KEY
# ============================================================

missing_employee_key = fact_orders.filter(
    F.col("EmployeeKey").isNull()
)

missing_count = missing_employee_key.count()

if missing_count > 0:

    print()
    print("ERROR: EmployeeKey lookup failed.")

    missing_employee_key.select(
        "OrderID",
        "EmployeeID"
    ).show(20, truncate=False)

    raise ValueError(
        f"STOP: {missing_count} orders could not be "
        f"mapped to Dim_Employee."
    )

else:

    print()
    print("EmployeeID → EmployeeKey: PASSED")


# ============================================================
# 6. VALIDATE SHIPPER KEY
# ============================================================

missing_shipper_key = fact_orders.filter(
    F.col("ShipperKey").isNull()
)

missing_count = missing_shipper_key.count()

if missing_count > 0:

    print()
    print("ERROR: ShipperKey lookup failed.")

    missing_shipper_key.select(
        "OrderID",
        "ShipVia"
    ).show(20, truncate=False)

    raise ValueError(
        f"STOP: {missing_count} orders could not be "
        f"mapped to Dim_Shipper."
    )

else:

    print()
    print("ShipVia → ShipperKey: PASSED")


# ============================================================
# 7. VALIDATE ORDER DATE
# ============================================================

missing_order_date_key = fact_orders.filter(
    F.col("OrderDateKey").isNull()
)

missing_count = missing_order_date_key.count()

if missing_count > 0:

    print()
    print("ERROR: OrderDateKey lookup failed.")

    missing_order_date_key.select(
        "OrderID",
        "OrderDate"
    ).show(20, truncate=False)

    raise ValueError(
        f"STOP: {missing_count} orders could not be "
        f"mapped to Dim_Date using OrderDate."
    )

else:

    print()
    print("OrderDate → OrderDateKey: PASSED")


# ============================================================
# 8. VALIDATE REQUIRED DATE
# ============================================================

missing_required_date_key = fact_orders.filter(
    F.col("RequiredDateKey").isNull()
)

missing_count = missing_required_date_key.count()

if missing_count > 0:

    print()
    print("ERROR: RequiredDateKey lookup failed.")

    missing_required_date_key.select(
        "OrderID",
        "RequiredDate"
    ).show(20, truncate=False)

    raise ValueError(
        f"STOP: {missing_count} orders could not be "
        f"mapped to Dim_Date using RequiredDate."
    )

else:

    print()
    print("RequiredDate → RequiredDateKey: PASSED")


# ============================================================
# 9. VALIDATE SHIPPED DATE
# ============================================================
#
# NULL ShippedDateKey is valid when:
#   - ShippedDate is NULL
#   - ShippedDate is 9999-12-31
#
# Any other real date must exist in Dim_Date.
# ============================================================

missing_shipped_date_key = fact_orders.filter(
    F.col("ShippedDate").isNotNull()
    & (F.to_date("ShippedDate") != F.lit("9999-12-31").cast("date"))
    & F.col("ShippedDateKey").isNull()
)

missing_count = missing_shipped_date_key.count()

if missing_count > 0:

    print()
    print("ERROR: ShippedDateKey lookup failed.")

    missing_shipped_date_key.select(
        "OrderID",
        "ShippedDate"
    ).show(20, truncate=False)

    raise ValueError(
        f"STOP: {missing_count} real shipped orders "
        f"could not be mapped to Dim_Date."
    )

else:

    print()
    print(
        "ShippedDate → ShippedDateKey: PASSED"
    )
# ============================================================
# 10. CREATE PHYSICAL FACT MEASURES
# ============================================================

fact_orders = (
    fact_orders

    # One row = one order
    .withColumn(
        "OrderCount",
        F.lit(1)
    )
)


# ============================================================
# 11. FINAL WAREHOUSE SHAPE
# ============================================================

fact_orders = fact_orders.select(

    "OrderID",

    "CustomerKey",
    "EmployeeKey",
    "ShipperKey",

    "OrderDateKey",
    "RequiredDateKey",
    "ShippedDateKey",

    "Freight",

    "OrderCount",

    "ShipName",
    "ShipAddress",
    "ShipCity",
    "ShipRegion",
    "ShipPostalCode",
    "ShipCountry",

    "ShippedStatus"
)


# ============================================================
# 12. FINAL OUTPUT CHECK
# ============================================================

print()
print("=" * 80)
print("FACT_ORDERS READY")
print("=" * 80)

print(
    f"Fact_Orders rows: {fact_orders.count()}"
)

print()
print("Final columns:")

for column in fact_orders.columns:
    print(f"  ✓ {column}")

print()
print("Sample:")
fact_orders.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_dataframes = {
    "Dim_Customer": dim_customer,
    "Dim_Employee": dim_employee,
    "Dim_Product": dim_product,
    "Dim_Category": dim_category,
    "Dim_Supplier": dim_supplier,
    "Dim_Shipper": dim_shipper,
    "Dim_Date": dim_date,
    "Fact_Sales": fact_sales,
    "Fact_Orders": fact_orders
}

# ------------------------------------------------------------
# Corresponding Silver tables
# ------------------------------------------------------------

gold_to_silver = {
    "Dim_Customer": "Silver_Customers",
    "Dim_Employee": "Silver_Employees",
    "Dim_Product": "Silver_Products",
    "Dim_Category": "Silver_Categories",
    "Dim_Supplier": "Silver_Suppliers",
    "Dim_Shipper": "Silver_Shippers",
    "Dim_Date": None,
    "Fact_Sales": "Silver_Order_Details",
    "Fact_Orders": "Silver_Orders"
}

print("Gold DataFrames loaded:")
for name, df in gold_dataframes.items():
    print(f"{name}: {df.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 19 — Verify Warehouse destination tables
# ============================================================

warehouse_tables = list(gold_dataframes.keys())

print("Checking Warehouse destination tables...")
print()

for table_name in warehouse_tables:

    print(f"{table_name}")

    # Warehouse tables are destinations only.
    # They are not Spark Lakehouse tables.
    print("  Destination:", f"{WAREHOUSE_NAME}.dbo.{table_name}")

print()
print("Warehouse destination list is ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 20 — Read current Silver Delta versions
# ============================================================

def get_delta_version(table_name):

    history = (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
        .select("version")
        .orderBy(F.desc("version"))
        .limit(1)
        .collect()
    )

    if not history:
        return None

    return int(history[0]["version"])


silver_versions = {}

for gold_table, silver_table in gold_to_silver.items():

    if silver_table is None:
        silver_versions[gold_table] = None
        continue

    try:

        version = get_delta_version(silver_table)

        silver_versions[gold_table] = version

        print(
            f"{gold_table} <- {silver_table} "
            f"| Silver version = {version}"
        )

    except Exception as e:

        silver_versions[gold_table] = None

        print(
            f"{gold_table} <- {silver_table} "
            f"| Silver version could not be read"
        )

print()
print("Silver version check completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 22 — Add SilverVersion to Gold DataFrames
# ============================================================

gold_dataframes_versioned = {}

for gold_table, df in gold_dataframes.items():

    silver_version = silver_versions.get(gold_table)

    if silver_version is not None:

        df_versioned = (
            df
            .withColumn(
                "SilverVersion",
                F.lit(silver_version).cast("long")
            )
        )

    else:

        # Dim_Date is generated from dates rather than
        # directly from a Silver Delta table.
        df_versioned = (
            df
            .withColumn(
                "SilverVersion",
                F.lit(None).cast("long")
            )
        )

    gold_dataframes_versioned[gold_table] = df_versioned

    print(
        f"{gold_table}: "
        f"{df_versioned.count()} rows"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 23 — Display Gold schemas
# ============================================================

for table_name, df in gold_dataframes_versioned.items():

    print("=" * 70)
    print(table_name)

    df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

WAREHOUSE_SERVER = "pecotg37dq5efkaj45i3vxdg4a-to63fphhvn4e5cr4krttiov2la.datawarehouse.fabric.microsoft.com"
print("Warehouse server:", WAREHOUSE_SERVER)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 24 — Warehouse connection + version-aware writer
# ============================================================

import pyodbc
import struct
import pandas as pd


# ============================================================
# WAREHOUSE CONNECTION
# ============================================================

def get_warehouse_connection():

    token = notebookutils.credentials.getToken(
        "https://database.windows.net/"
    )

    if not token:
        raise RuntimeError(
            "Could not obtain an access token for the Warehouse."
        )

    # SQL_COPT_SS_ACCESS_TOKEN expects UTF-16-LE
    token_bytes = token.encode("utf-16-le")

    SQL_COPT_SS_ACCESS_TOKEN = 1256

    access_token = struct.pack(
        f"<I{len(token_bytes)}s",
        len(token_bytes),
        token_bytes
    )

    connection_string = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{WAREHOUSE_SERVER},1433;"
        f"Database={WAREHOUSE_NAME};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    return pyodbc.connect(
        connection_string,
        attrs_before={
            SQL_COPT_SS_ACCESS_TOKEN: access_token
        }
    )


# ============================================================
# CLEAN PYTHON VALUES
# ============================================================

def clean_python_value(value):

    if value is None:
        return None

    try:

        if pd.isna(value):
            return None

    except Exception:
        pass

    if hasattr(value, "to_pydatetime"):

        return value.to_pydatetime()

    if hasattr(value, "item"):

        try:
            return value.item()

        except Exception:
            pass

    return value


# ============================================================
# GET WAREHOUSE LOADED SILVER VERSION
# ============================================================

def get_warehouse_loaded_version(
    connection,
    gold_table
):

    cursor = connection.cursor()

    try:

        cursor.execute(
            """
            SELECT SilverVersion
            FROM dbo.Warehouse_Load_Control
            WHERE GoldTable = ?
            """,
            (gold_table,)
        )

        result = cursor.fetchone()

        if result is None:
            return None

        return result[0]

    finally:

        cursor.close()


# ============================================================
# UPDATE WAREHOUSE LOAD CONTROL
# ============================================================

def update_warehouse_load_control(
    cursor,
    gold_table,
    silver_table,
    silver_version
):

    cursor.execute(
        """
        DELETE FROM dbo.Warehouse_Load_Control
        WHERE GoldTable = ?
        """,
        (gold_table,)
    )

    cursor.execute(
        """
        INSERT INTO dbo.Warehouse_Load_Control
        (
            GoldTable,
            SilverTable,
            SilverVersion,
            LastLoadedAt
        )
        VALUES
        (
            ?,
            ?,
            ?,
            SYSUTCDATETIME()
        )
        """,
        (
            gold_table,
            silver_table,
            silver_version
        )
    )

def normalize_sql_date(value):

    if value is None:
        return None

    try:

        if pd.isna(value):
            return None

    except Exception:
        pass


    # --------------------------------------------------------
    # Python datetime/date
    # --------------------------------------------------------

    if hasattr(value, "strftime"):

        return value.strftime(
            "%Y-%m-%d"
        )


    # --------------------------------------------------------
    # String
    # --------------------------------------------------------

    text = str(value).strip()


    if not text:
        return None


    # Handle strings such as:
    #
    # 1996-07-04
    # 1996-07-04 00:00:00
    # 9999-12-31 00:00:00

    return text[:10]


# ============================================================
# WAREHOUSE WRITER
# Version-aware + Warehouse-schema-aware
#
# Uses existing:
# dbo.Warehouse_Load_Control
#
# Control columns:
# GoldTable
# SilverTable
# SilverVersion
# LastLoadedAt
# ============================================================

def write_to_warehouse(
    df,
    warehouse_table,
    silver_table=None
):

    print()
    print("=" * 70)
    print(f"Processing Warehouse table: {warehouse_table}")
    print("=" * 70)


    # ========================================================
    # BASIC INFORMATION
    # ========================================================

    gold_table = warehouse_table

    # Fact_Sales is called Fact_Order_Details in Warehouse.
    if warehouse_table == "Fact_Order_Details":
        gold_table = "Fact_Sales"


    row_count = df.count()

    print(
        f"Gold rows: {row_count}"
    )


    if row_count == 0:

        print(
            f"SKIPPED: {gold_table} contains 0 rows."
        )

        return


    # ========================================================
    # GET SILVER VERSION
    # ========================================================

    silver_version = None

    if "SilverVersion" in df.columns:

        version_row = (
            df
            .select("SilverVersion")
            .where(F.col("SilverVersion").isNotNull())
            .first()
        )

        if version_row is not None:
            silver_version = version_row["SilverVersion"]


    print(
        f"Gold SilverVersion: {silver_version}"
    )


    # ========================================================
    # CONNECT TO WAREHOUSE
    # ========================================================

    print(
        "Connecting to Warehouse..."
    )

    connection = get_warehouse_connection()

    print(
        "Warehouse connection successful."
    )

    cursor = connection.cursor()


    try:

        # ====================================================
        # CHECK CONTROL TABLE
        # ====================================================

        previous_version = None

        cursor.execute(
            """
            SELECT MAX(SilverVersion)
            FROM dbo.Warehouse_Load_Control
            WHERE GoldTable = ?
            """,
            (gold_table,)
        )

        result = cursor.fetchone()

        if result is not None:
            previous_version = result[0]


        print(
            f"Previously loaded SilverVersion: "
            f"{previous_version}"
        )


        # ====================================================
        # RERUN LOGIC
        # ====================================================

        if (
            silver_version is not None
            and previous_version is not None
            and int(previous_version)
            == int(silver_version)
        ):

            print()
            print(
                f"SKIPPED: {gold_table} is already "
                f"loaded from SilverVersion "
                f"{silver_version}."
            )

            return

        # ====================================================
        # PREPARE DATA FOR WAREHOUSE
        # ====================================================

        # ----------------------------------------------------
        # FACT_ORDER_DETAILS
        #
        # Gold:
        # Fact_Sales
        #
        # Warehouse:
        # Fact_Order_Details
        #
        # Gold:
        # ProductID
        # CustomerID
        # OrderDate
        #
        # become:
        # ProductKey
        # CustomerKey
        # OrderDateKey
        # ----------------------------------------------------

        if warehouse_table == "Fact_Order_Details":

            print()
            print(
                "Preparing Fact_Sales "
                "for dbo.Fact_Order_Details..."
            )


            required = [
                "OrderID",
                "ProductID",
                "CustomerID",
                "OrderDate",
                "UnitPrice",
                "Quantity",
                "Discount"
            ]


            missing = [
                c
                for c in required
                if c not in df.columns
            ]


            if missing:

                raise ValueError(
                    "Fact_Sales is missing required "
                    f"columns: {missing}"
                )


            # ------------------------------------------------
            # Load dimensions from Warehouse
            # ------------------------------------------------

            print(
                "Loading Product dimension..."
            )

            product_dim = pd.read_sql(
                """
                SELECT
                    ProductKey,
                    ProductID
                FROM dbo.Dim_Product
                """,
                connection
            )


            print(
                "Loading Customer dimension..."
            )

            customer_dim = pd.read_sql(
                """
                SELECT
                    CustomerKey,
                    CustomerID
                FROM dbo.Dim_Customer
                """,
                connection
            )


            print(
                "Loading Date dimension..."
            )

            date_dim = pd.read_sql(
                """
                SELECT
                    DateKey,
                    [Date]
                FROM dbo.Dim_Date
                """,
                connection
            )


            # ------------------------------------------------
            # Convert Gold to Pandas
            # ------------------------------------------------

            pandas_df = df.toPandas()


            # ------------------------------------------------
            # Product lookup
            # ------------------------------------------------

            pandas_df = pandas_df.merge(
                product_dim,
                on="ProductID",
                how="left",
                validate="many_to_one"
            )


            # ------------------------------------------------
            # Customer lookup
            # ------------------------------------------------

            pandas_df = pandas_df.merge(
                customer_dim,
                on="CustomerID",
                how="left",
                validate="many_to_one"
            )


            # ------------------------------------------------
            # Date lookup
            # ------------------------------------------------

            pandas_df["OrderDate"] = pd.to_datetime(
                pandas_df["OrderDate"]
            ).dt.date

            date_dim["Date"] = pd.to_datetime(
                date_dim["Date"]
            ).dt.date


            pandas_df = pandas_df.merge(
                date_dim,
                left_on="OrderDate",
                right_on="Date",
                how="left",
                validate="many_to_one"
            )


            # ------------------------------------------------
            # Validate dimension lookups
            # ------------------------------------------------

            if pandas_df["ProductKey"].isna().any():

                bad = pandas_df.loc[
                    pandas_df["ProductKey"].isna(),
                    "ProductID"
                ].drop_duplicates().tolist()

                raise ValueError(
                    "Fact_Order_Details contains ProductID "
                    "values that do not exist in "
                    f"Dim_Product: {bad[:20]}"
                )


            if pandas_df["CustomerKey"].isna().any():

                bad = pandas_df.loc[
                    pandas_df["CustomerKey"].isna(),
                    "CustomerID"
                ].drop_duplicates().tolist()

                raise ValueError(
                    "Fact_Order_Details contains CustomerID "
                    "values that do not exist in "
                    f"Dim_Customer: {bad[:20]}"
                )


            if pandas_df["DateKey"].isna().any():

                bad = pandas_df.loc[
                    pandas_df["DateKey"].isna(),
                    "OrderDate"
                ].drop_duplicates().tolist()

                raise ValueError(
                    "Fact_Order_Details contains OrderDate "
                    "values that do not exist in "
                    f"Dim_Date: {bad[:20]}"
                )


            # ------------------------------------------------
            # Final Warehouse shape
            # ------------------------------------------------

            pandas_df["LineCount"] = 1


            pandas_df = pandas_df[
                [
                    "OrderID",
                    "ProductKey",
                    "CustomerKey",
                    "DateKey",
                    "UnitPrice",
                    "Quantity",
                    "Discount",
                    "LineCount"
                ]
            ].rename(
                columns={
                    "DateKey": "OrderDateKey"
                }
            )


        # ----------------------------------------------------
        # FACT_ORDERS
        # ----------------------------------------------------

        elif warehouse_table == "Fact_Orders":

            print()
            print(
                "Preparing Fact_Orders "
                "for Warehouse..."
            )


            required = [
                "OrderID",
                "CustomerID",
                "EmployeeID",
                "ShipVia",
                "OrderDate",
                "RequiredDate",
                "ShippedDate",
                "Freight",
                "ShippedStatus"
            ]


            missing = [
                c
                for c in required
                if c not in df.columns
            ]


            if missing:

                raise ValueError(
                    "Fact_Orders is missing required "
                    f"columns: {missing}"
                )


            # ------------------------------------------------
            # Load dimensions
            # ------------------------------------------------

            print(
                "Loading Customer dimension..."
            )

            customer_dim = pd.read_sql(
                """
                SELECT
                    CustomerKey,
                    CustomerID
                FROM dbo.Dim_Customer
                """,
                connection
            )


            print(
                "Loading Employee dimension..."
            )

            employee_dim = pd.read_sql(
                """
                SELECT
                    EmployeeKey,
                    EmployeeID
                FROM dbo.Dim_Employee
                """,
                connection
            )


            print(
                "Loading Shipper dimension..."
            )

            shipper_dim = pd.read_sql(
                """
                SELECT
                    ShipperKey,
                    ShipperID
                FROM dbo.Dim_Shipper
                """,
                connection
            )


            print(
                "Loading Date dimension..."
            )

            date_dim = pd.read_sql(
                """
                SELECT
                    DateKey,
                    [Date]
                FROM dbo.Dim_Date
                """,
                connection
            )


            # ------------------------------------------------
            # Convert Gold
            # ------------------------------------------------

            pandas_df = df.toPandas()


            # ------------------------------------------------
            # Customer lookup
            # ------------------------------------------------

            pandas_df = pandas_df.merge(
                customer_dim,
                on="CustomerID",
                how="left",
                validate="many_to_one"
            )


            # ------------------------------------------------
            # Employee lookup
            # ------------------------------------------------

            pandas_df = pandas_df.merge(
                employee_dim,
                on="EmployeeID",
                how="left",
                validate="many_to_one"
            )


            # ------------------------------------------------
            # Shipper lookup
            # ------------------------------------------------

            pandas_df = pandas_df.merge(
                shipper_dim,
                left_on="ShipVia",
                right_on="ShipperID",
                how="left",
                validate="many_to_one"
            )


            # ------------------------------------------------
            # Convert dates
            # ------------------------------------------------

            for column in [ "OrderDate", "RequiredDate", "ShippedDate" ]: 
                pandas_df[column] = ( pandas_df[column] .apply(normalize_sql_date) ) 
            

            date_dim["Date"] = ( date_dim["Date"] .apply(normalize_sql_date) )

            # ------------------------------------------------
            # OrderDate lookup
            # ------------------------------------------------

            pandas_df = pandas_df.merge(
                date_dim.rename(
                    columns={
                        "DateKey": "OrderDateKey"
                    }
                ),
                left_on="OrderDate",
                right_on="Date",
                how="left",
                validate="many_to_one"
            )


            # ------------------------------------------------
            # RequiredDate lookup
            # ------------------------------------------------

            pandas_df = pandas_df.merge(
                date_dim.rename(
                    columns={
                        "DateKey": "RequiredDateKey"
                    }
                ),
                left_on="RequiredDate",
                right_on="Date",
                how="left",
                validate="many_to_one"
            )


            # ------------------------------------------------
            # ShippedDate lookup
            # ------------------------------------------------

            pandas_df = pandas_df.merge(
                date_dim.rename(
                    columns={
                        "DateKey": "ShippedDateKey"
                    }
                ),
                left_on="ShippedDate",
                right_on="Date",
                how="left",
                validate="many_to_one"
            )


            # ------------------------------------------------
            # Validate lookups
            # ------------------------------------------------

            for key_column, source_column in [
                ("CustomerKey", "CustomerID"),
                ("EmployeeKey", "EmployeeID"),
                ("ShipperKey", "ShipVia")
            ]:

                if pandas_df[key_column].isna().any():

                    bad = pandas_df.loc[
                        pandas_df[key_column].isna(),
                        source_column
                    ].drop_duplicates().tolist()

                    raise ValueError(
                        f"Fact_Orders contains "
                        f"{source_column} values that do not "
                        f"exist in the corresponding dimension: "
                        f"{bad[:20]}"
                    )


            for key_column, date_column in [
                ("OrderDateKey", "OrderDate"),
                ("RequiredDateKey", "RequiredDate")
            ]:

                if pandas_df[key_column].isna().any():

                    bad = pandas_df.loc[
                        pandas_df[key_column].isna(),
                        date_column
                    ].drop_duplicates().tolist()

                    raise ValueError(
                        f"Fact_Orders contains dates in "
                        f"{date_column} that do not exist in "
                        f"Dim_Date: {bad[:20]}"
                    )


            # ShippedDate can legitimately be NULL
            # for orders that have not shipped yet.


            # ------------------------------------------------
            # Final Warehouse shape
            # ------------------------------------------------

            pandas_df["OrderCount"] = 1


            # The current Gold Fact_Orders does not carry
            # shipping-address attributes.
            #
            # The Warehouse columns are nullable, therefore
            # NULL is appropriate rather than inventing values.

            for column in [
                "ShipName",
                "ShipAddress",
                "ShipCity",
                "ShipRegion",
                "ShipPostalCode",
                "ShipCountry"
            ]:

                pandas_df[column] = None


            pandas_df = pandas_df[
                [
                    "OrderID",
                    "CustomerKey",
                    "EmployeeKey",
                    "ShipperKey",
                    "OrderDateKey",
                    "RequiredDateKey",
                    "ShippedDateKey",
                    "Freight",
                    "OrderCount",
                    "ShipName",
                    "ShipAddress",
                    "ShipCity",
                    "ShipRegion",
                    "ShipPostalCode",
                    "ShipCountry",
                    "ShippedStatus"
                ]
            ]


        # ----------------------------------------------------
        # DIMENSIONS / OTHER TABLES
        # ----------------------------------------------------

        else:

            print(
                "Preparing standard dimension DataFrame..."
            )

            pandas_df = df.toPandas()

            # SilverVersion is control metadata,
            # not a business Warehouse column.
            if "SilverVersion" in pandas_df.columns:

                pandas_df = pandas_df.drop(
                    columns=["SilverVersion"]
                )


        # ====================================================
        # PREPARE INSERT
        # ====================================================

        columns = list(
            pandas_df.columns
        )


        column_list = ", ".join(
            f"[{column}]"
            for column in columns
        )


        placeholders = ", ".join(
            "?"
            for _ in columns
        )


        insert_sql = f"""
            INSERT INTO dbo.[{warehouse_table}]
            ({column_list})
            VALUES ({placeholders})
        """


        # ====================================================
        # PREPARE PYTHON ROWS
        # ====================================================

        print(
            "Preparing rows for Warehouse..."
        )


        rows = []


        for row in pandas_df.itertuples(
            index=False,
            name=None
        ):

            rows.append(
                tuple(
                    clean_python_value(value)
                    for value in row
                )
            )


        print(
            f"Prepared {len(rows)} rows."
        )


        # ====================================================
        # DELETE OLD WAREHOUSE DATA
        #
        # IMPORTANT:
        # This happens ONLY after the version check.
        # ====================================================

        print()
        print(
            f"Clearing existing rows from "
            f"dbo.{warehouse_table}..."
        )


        cursor.execute(
            f"""
            DELETE FROM dbo.[{warehouse_table}]
            """
        )


        print(
            "Existing rows cleared."
        )


        # ====================================================
        # INSERT IN BATCHES
        #
        # No fast_executemany.
        # ====================================================

        batch_size = 500

        total_rows = len(rows)


        print(
            f"Inserting {total_rows} rows..."
        )


        for start in range(
            0,
            total_rows,
            batch_size
        ):

            end = min(
                start + batch_size,
                total_rows
            )

            batch = rows[start:end]


            cursor.executemany(
                insert_sql,
                batch
            )


            print(
                f"Inserted rows "
                f"{start + 1} - {end} "
                f"of {total_rows}"
            )


        # ====================================================
        # UPDATE CONTROL TABLE
        # ====================================================

        if silver_version is not None:

            print(
                "Updating Warehouse_Load_Control..."
            )


            cursor.execute(
                """
                DELETE FROM dbo.Warehouse_Load_Control
                WHERE GoldTable = ?
                """,
                (gold_table,)
            )


            cursor.execute(
                """
                INSERT INTO dbo.Warehouse_Load_Control
                (
                    GoldTable,
                    SilverTable,
                    SilverVersion,
                    LastLoadedAt
                )
                VALUES
                (
                    ?,
                    ?,
                    ?,
                    SYSUTCDATETIME()
                )
                """,
                (
                    gold_table,
                    silver_table,
                    int(silver_version),
                )
            )


            print(
                f"Recorded SilverVersion "
                f"{silver_version}."
            )


        # ====================================================
        # COMMIT
        # ====================================================

        connection.commit()


        print()
        print("=" * 70)
        print(
            f"SUCCESS: {warehouse_table} "
            f"loaded with {len(rows)} rows."
        )
        print("=" * 70)


    except Exception as e:

        print()
        print(
            f"ERROR writing {warehouse_table}:"
        )

        print(
            str(e)
        )


        print(
            "Rolling back transaction..."
        )

        connection.rollback()


        print(
            "Rollback completed."
        )

        raise


    finally:

        cursor.close()
        connection.close()

        print(
            f"Warehouse connection closed for "
            f"{warehouse_table}."
        )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# OPTIMIZED DIM_DATE WAREHOUSE WRITER
# ============================================================

def write_dim_date_to_warehouse(df):

    print()
    print("=" * 70)
    print("Writing Dim_Date to Warehouse")
    print("=" * 70)


    # --------------------------------------------------------
    # Count rows
    # --------------------------------------------------------

    print("Checking Dim_Date row count...")

    row_count = df.count()

    print(
        f"Dim_Date rows to write: {row_count}"
    )


    if row_count == 0:

        print(
            "SKIPPED: Dim_Date contains 0 rows."
        )

        return


    # --------------------------------------------------------
    # Get date range from Gold
    # --------------------------------------------------------

    print("Checking Gold date range...")

    date_range = (
        df
        .select(
            F.min("Date").alias("MinDate"),
            F.max("Date").alias("MaxDate")
        )
        .first()
    )

    gold_min_date = date_range["MinDate"]
    gold_max_date = date_range["MaxDate"]

    print(
        f"Gold date range: "
        f"{gold_min_date} → {gold_max_date}"
    )


    # --------------------------------------------------------
    # Connect to Warehouse
    # --------------------------------------------------------

    print("Connecting to Warehouse...")

    connection = get_warehouse_connection()

    print("Warehouse connection successful.")

    cursor = connection.cursor()


    try:

        # ====================================================
        # CHECK EXISTING DATE RANGE
        # ====================================================

        print(
            "Checking existing Warehouse Dim_Date..."
        )

        cursor.execute(
            """
            SELECT
                MIN([Date]),
                MAX([Date]),
                COUNT(*)
            FROM dbo.[Dim_Date]
            """
        )

        result = cursor.fetchone()

        warehouse_min_date = result[0]
        warehouse_max_date = result[1]
        warehouse_row_count = result[2]


        print(
            f"Warehouse date range: "
            f"{warehouse_min_date} → "
            f"{warehouse_max_date}"
        )

        print(
            f"Warehouse rows: "
            f"{warehouse_row_count}"
        )


        # ====================================================
        # RERUN CHECK
        # ====================================================

        if (
            warehouse_min_date == gold_min_date
            and
            warehouse_max_date == gold_max_date
            and
            warehouse_row_count == row_count
        ):

            print()
            print(
                "SKIPPED: Dim_Date is already "
                "up to date."
            )

            return


        print()
        print(
            "Dim_Date requires a reload."
        )


        # ====================================================
        # GET DATA FROM SPARK
        # ====================================================

        print(
            "Converting Dim_Date to Pandas..."
        )

        pandas_df = df.toPandas()

        print(
            f"Pandas conversion complete: "
            f"{len(pandas_df)} rows."
        )


        # ----------------------------------------------------
        # Remove SilverVersion if present
        # ----------------------------------------------------

        if "SilverVersion" in pandas_df.columns:

            pandas_df = pandas_df.drop(
                columns=["SilverVersion"]
            )


        # ----------------------------------------------------
        # Prepare columns
        # ----------------------------------------------------

        columns = list(
            pandas_df.columns
        )

        column_list = ", ".join(
            f"[{column}]"
            for column in columns
        )


        # ====================================================
        # DELETE + BULK INSERT
        # ====================================================

        print()
        print(
            "Starting Dim_Date transaction..."
        )


        # ----------------------------------------------------
        # Delete existing rows
        # ----------------------------------------------------

        print(
            "Clearing existing Dim_Date rows..."
        )

        cursor.execute(
            """
            DELETE FROM dbo.[Dim_Date]
            """
        )

        print(
            "Existing Dim_Date rows cleared."
        )


        # ====================================================
        # MULTI-ROW INSERT
        # ====================================================

        # Number of rows sent to SQL Server per statement.
        #
        # 500 rows is a safe size for this table.
        #
        # This is MUCH faster than:
        #
        # cursor.executemany(...)
        #
        # because each batch is one SQL execution.

        batch_size = 500

        total_rows = len(pandas_df)


        print()
        print(
            f"Inserting {total_rows} rows "
            f"in batches of {batch_size}..."
        )


        for start in range(
            0,
            total_rows,
            batch_size
        ):

            end = min(
                start + batch_size,
                total_rows
            )

            batch_df = pandas_df.iloc[
                start:end
            ]


            # ------------------------------------------------
            # Build VALUES section
            # ------------------------------------------------

            values = []

            for row in batch_df.itertuples(
                index=False,
                name=None
            ):

                cleaned_row = []

                for value in row:

                    value = clean_python_value(
                        value
                    )

                    if value is None:

                        cleaned_row.append(
                            "NULL"
                        )

                    elif isinstance(
                        value,
                        bool
                    ):

                        cleaned_row.append(
                            "1"
                            if value
                            else "0"
                        )

                    elif isinstance(
                        value,
                        (int, float)
                    ):

                        cleaned_row.append(
                            str(value)
                        )

                    else:

                        # Escape single quotes
                        escaped = str(
                            value
                        ).replace(
                            "'",
                            "''"
                        )

                        cleaned_row.append(
                            f"'{escaped}'"
                        )


                values.append(
                    "("
                    + ", ".join(cleaned_row)
                    + ")"
                )


            insert_sql = f"""
                INSERT INTO dbo.[Dim_Date]
                ({column_list})
                VALUES
                {", ".join(values)}
            """


            cursor.execute(
                insert_sql
            )


            print(
                f"Inserted rows "
                f"{start + 1} - {end} "
                f"of {total_rows}"
            )


        # ====================================================
        # COMMIT
        # ====================================================

        print()
        print(
            "Committing Dim_Date transaction..."
        )

        connection.commit()


        print()
        print("=" * 70)
        print(
            f"SUCCESS: Dim_Date loaded with "
            f"{row_count} rows."
        )
        print("=" * 70)


    except Exception as e:

        print()
        print(
            "ERROR writing Dim_Date:"
        )

        print(
            str(e)
        )

        print()
        print(
            "Rolling back transaction..."
        )

        connection.rollback()

        print(
            "Rollback completed."
        )

        raise


    finally:

        cursor.close()
        connection.close()

        print(
            "Warehouse connection closed."
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 25 — Simple Warehouse connection test
# ============================================================

connection = None
cursor = None

try:

    print("Testing Warehouse connection...")

    connection = get_warehouse_connection()

    cursor = connection.cursor()

    cursor.execute("""
        SELECT
            DB_NAME() AS DatabaseName
    """)

    result = cursor.fetchone()

    print()
    print("Warehouse connection successful.")
    print("Connected database:", result[0])


except Exception as e:

    print()
    print("Warehouse connection FAILED.")
    print("Error:", str(e))

    raise


finally:

    if cursor is not None:
        cursor.close()

    if connection is not None:
        connection.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 26 — Write Gold dimension tables to Warehouse
# ============================================================

dimension_sources = {

    "Dim_Customer": "Silver_Customers",
    "Dim_Employee": "Silver_Employees",
    "Dim_Product": "Silver_Products",
    "Dim_Category": "Silver_Categories",
    "Dim_Supplier": "Silver_Suppliers",
    "Dim_Shipper": "Silver_Shippers"
}


# ------------------------------------------------------------
# Silver-derived dimensions
# ------------------------------------------------------------
for warehouse_table, silver_table in dimension_sources.items():

    print()
    print("#" * 70)
    print(
        f"Processing dimension: "
        f"{warehouse_table}"
    )

    write_to_warehouse(
        gold_dataframes_versioned[warehouse_table],
        warehouse_table,
        silver_table
    )
# ------------------------------------------------------------
# Dim_Date
# ------------------------------------------------------------
"""
print()
print("#" * 70)
print("Processing dimension: Dim_Date")

write_dim_date_to_warehouse(
    dim_date
)
"""

print()
print("#" * 70)
print("ALL DIMENSIONS PROCESSED.")
print("#" * 70)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# FINAL CELL — LOAD FACT TABLES
# ============================================================

fact_tables = [
    (
        "Fact_Sales",
        "Fact_Order_Details",
        "Silver_Order_Details"
    ),
    (
        "Fact_Orders",
        "Fact_Orders",
        "Silver_Orders"
    )
]


for gold_table, warehouse_table, silver_table in fact_tables:

    print()
    print("=" * 70)

    print(
        f"Gold table:      {gold_table}"
    )

    print(
        f"Warehouse table: dbo.{warehouse_table}"
    )

    print(
        f"Silver table:    {silver_table}"
    )

    print("=" * 70)


    df = gold_dataframes_versioned[
        gold_table
    ]


    write_to_warehouse(
        df=df,
        warehouse_table=warehouse_table,
        silver_table=silver_table
    )


print()
print("=" * 70)
print(
    "ALL FACT TABLES WRITTEN SUCCESSFULLY."
)
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# FINAL GOLD FACT VALIDATION — FAST VERSION
# ============================================================
#
# Purpose:
#   1. Validate Gold physical measures against Silver
#   2. Validate every fact-table foreign key
#   3. Validate fact-table grain / duplicates
#   4. Avoid pd.read_sql() completely
#   5. Avoid loading Warehouse fact tables into Pandas
#
# Notebook naming:
#
#   Gold Fact_Sales
#       -> Warehouse Fact_Order_Details
#
#   Gold Fact_Orders
#       -> Warehouse Fact_Orders
#
# IMPORTANT:
#   This cell ONLY validates.
#   It does NOT modify Silver, Gold, or Warehouse.
# ============================================================

import pandas as pd
from decimal import Decimal


# ============================================================
# 1. RESULT STORAGE
# ============================================================

validation_results = []


def record_result(check_name, passed, details=""):

    status = "PASS" if passed else "FAIL"

    validation_results.append({
        "Check": check_name,
        "Status": status,
        "Details": details
    })

    print(
        f"[{status}] {check_name}"
        + (f" | {details}" if details else "")
    )


def sql_scalar(cursor, sql, params=None):

    if params is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, params)

    row = cursor.fetchone()

    if row is None:
        return None

    return row[0]


def sql_row(cursor, sql, params=None):

    if params is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, params)

    return cursor.fetchone()

def decimal_value(value):

    if value is None:
        return Decimal("0")

    return Decimal(str(value))


def compare_decimal(check_name, gold_value, silver_value, tolerance=Decimal("0.01")):

    gold_value = decimal_value(gold_value)
    silver_value = decimal_value(silver_value)

    difference = abs(gold_value - silver_value)

    passed = difference <= tolerance

    record_result(
        check_name,
        passed,
        (
            f"Gold={gold_value}, "
            f"Silver={silver_value}, "
            f"Difference={difference}"
        )
    )


# ============================================================
# 2. START
# ============================================================

print()
print("=" * 90)
print("FINAL GOLD FACT VALIDATION — FAST VERSION")
print("=" * 90)

print()
print("This validation uses direct Warehouse SQL.")
print("No pd.read_sql() is used.")
print()


# ============================================================
# 3. CHECK GOLD DATAFRAMES
# ============================================================

required_gold_tables = [
    "Fact_Sales",
    "Fact_Orders"
]

missing_gold = [
    table
    for table in required_gold_tables
    if table not in gold_dataframes_versioned
]

if missing_gold:

    raise RuntimeError(
        "Missing Gold DataFrames: "
        + ", ".join(missing_gold)
    )

print("Gold DataFrames found:")
print("  ✓ Fact_Sales")
print("  ✓ Fact_Orders")


# ============================================================
# 4. GOLD ROW COUNTS
# ============================================================

print()
print("=" * 90)
print("FACT ROW COUNTS")
print("=" * 90)

gold_sales_count = (
    gold_dataframes_versioned["Fact_Sales"]
    .count()
)

gold_orders_count = (
    gold_dataframes_versioned["Fact_Orders"]
    .count()
)

print(
    f"Gold Fact_Sales rows : {gold_sales_count}"
)

print(
    f"Gold Fact_Orders rows: {gold_orders_count}"
)


# ============================================================
# 5. LOAD SILVER COUNTS USING SPARK
# ============================================================

silver_details_count = (
    spark.table("Silver_Order_Details")
    .filter(F.col("IsCurrent") == 1)
    .count()
)

silver_orders_count = (
    spark.table("Silver_Orders")
    .filter(F.col("IsCurrent") == 1)
    .count()
)


record_result(
    "Fact_Sales row count",
    gold_sales_count == silver_details_count,
    (
        f"Gold={gold_sales_count}, "
        f"Silver={silver_details_count}"
    )
)


record_result(
    "Fact_Orders row count",
    gold_orders_count == silver_orders_count,
    (
        f"Gold={gold_orders_count}, "
        f"Silver={silver_orders_count}"
    )
)


# ============================================================
# 6. PHYSICAL MEASURES — FACT_SALES
# ============================================================

print()
print("=" * 90)
print("FACT_SALES — PHYSICAL MEASURES")
print("=" * 90)


gold_sales = (
    gold_dataframes_versioned["Fact_Sales"]
)


# ------------------------------------------------------------
# Silver expected measures
# ------------------------------------------------------------

silver_sales = (
    spark.table("Silver_Order_Details")
    .filter(F.col("IsCurrent") == 1)
    .select(
        "UnitPrice",
        "Quantity",
        "Discount"
    )
)


silver_sales_metrics = (
    silver_sales
    .agg(
        F.sum(
            F.col("UnitPrice") * F.col("Quantity")
        ).alias("GrossSales"),

        F.sum(
            F.col("UnitPrice")
            * F.col("Quantity")
            * F.col("Discount")
        ).alias("DiscountAmount"),

        F.sum(
            F.col("UnitPrice")
            * F.col("Quantity")
            * (F.lit(1) - F.col("Discount"))
        ).alias("NetSales"),

        F.sum("Quantity").alias("Quantity"),

        F.count(F.lit(1)).alias("OrderLines")
    )
    .collect()[0]
)
# ------------------------------------------------------------
# Gold physical measures for Fact_Order_Details
#
# IMPORTANT:
# Fact_Order_Details does NOT physically store:
# GrossSalesAmount
# DiscountAmount
# NetSalesAmount
#
# Therefore calculate them from the actual Warehouse/Gold
# columns:
#
# Gross Sales    = UnitPrice * Quantity
# Discount       = UnitPrice * Quantity * Discount
# Net Sales      = Gross Sales - Discount
# ------------------------------------------------------------

gold_sales_metrics = (
    gold_sales
    .agg(
        F.sum(
            F.col("UnitPrice") * F.col("Quantity")
        ).alias("GrossSales"),

        F.sum(
            F.col("UnitPrice")
            * F.col("Quantity")
            * F.col("Discount")
        ).alias("DiscountAmount"),

        F.sum(
            F.col("UnitPrice")
            * F.col("Quantity")
            * (F.lit(1) - F.col("Discount"))
        ).alias("NetSales"),

        F.sum("Quantity").alias("Quantity"),

        F.sum("LineCount").alias("OrderLines")
    )
    .collect()[0]
)


# ============================================================
# 7. FACT_SALES ROW-LEVEL CALCULATION CHECK
# ============================================================

print()
print("Checking Fact_Sales row-level calculations...")

# ------------------------------------------------------------
# IMPORTANT:
#
# Fact_Sales / Fact_Order_Details physically stores:
#   UnitPrice
#   Quantity
#   Discount
#   LineCount
#
# It does NOT physically store:
#   GrossSales
#   DiscountAmount
#   NetSales
#
# Therefore we calculate the expected values from the
# physical columns and compare them with the Silver source.
# ------------------------------------------------------------


# ------------------------------------------------------------
# Compare Gold physical transaction values directly
# against Silver source values.
#
# This verifies that the physical measures are based on
# the correct source data and were not changed incorrectly.
# ------------------------------------------------------------

silver_sales_for_check = (
    spark.table("Silver_Order_Details")
    .filter(F.col("IsCurrent") == 1)
    .select(
        "OrderID",
        "ProductID",
        "UnitPrice",
        "Quantity",
        "Discount"
    )
)


gold_sales_for_check = (
    gold_sales
    .select(
        "OrderID",
        "UnitPrice",
        "Quantity",
        "Discount",
        "LineCount"
    )
)


# ------------------------------------------------------------
# Aggregate Gold and Silver calculations independently
# ------------------------------------------------------------

gold_row_calc = (
    gold_sales_for_check
    .agg(
        F.sum(
            F.col("UnitPrice") * F.col("Quantity")
        ).alias("GrossSales"),

        F.sum(
            F.col("UnitPrice")
            * F.col("Quantity")
            * F.col("Discount")
        ).alias("DiscountAmount"),

        F.sum(
            F.col("UnitPrice")
            * F.col("Quantity")
            * (F.lit(1) - F.col("Discount"))
        ).alias("NetSales"),

        F.sum("Quantity").alias("Quantity"),

        F.sum("LineCount").alias("OrderLines")
    )
    .collect()[0]
)


silver_row_calc = (
    silver_sales_for_check
    .agg(
        F.sum(
            F.col("UnitPrice") * F.col("Quantity")
        ).alias("GrossSales"),

        F.sum(
            F.col("UnitPrice")
            * F.col("Quantity")
            * F.col("Discount")
        ).alias("DiscountAmount"),

        F.sum(
            F.col("UnitPrice")
            * F.col("Quantity")
            * (F.lit(1) - F.col("Discount"))
        ).alias("NetSales"),

        F.sum("Quantity").alias("Quantity"),

        F.count("*").alias("OrderLines")
    )
    .collect()[0]
)


# ------------------------------------------------------------
# Compare physical calculation results
# ------------------------------------------------------------

compare_decimal(
    "Fact_Sales Gross Sales calculation",
    gold_row_calc["GrossSales"],
    silver_row_calc["GrossSales"]
)

compare_decimal(
    "Fact_Sales Discount Amount calculation",
    gold_row_calc["DiscountAmount"],
    silver_row_calc["DiscountAmount"]
)

compare_decimal(
    "Fact_Sales Net Sales calculation",
    gold_row_calc["NetSales"],
    silver_row_calc["NetSales"]
)


record_result(
    "Fact_Sales Quantity",
    int(gold_row_calc["Quantity"] or 0)
    ==
    int(silver_row_calc["Quantity"] or 0),
    (
        f"Gold={gold_row_calc['Quantity']}, "
        f"Silver={silver_row_calc['Quantity']}"
    )
)


record_result(
    "Fact_Sales Order Lines",
    int(gold_row_calc["OrderLines"] or 0)
    ==
    int(silver_row_calc["OrderLines"] or 0),
    (
        f"Gold={gold_row_calc['OrderLines']}, "
        f"Silver={silver_row_calc['OrderLines']}"
    )
)


# ------------------------------------------------------------
# 7- Row-level physical column checks
# ------------------------------------------------------------

print()
print("Checking Gold physical columns for invalid values...")


invalid_unit_price = (
    gold_sales
    .filter(
        F.col("UnitPrice").isNull()
        | (F.col("UnitPrice") < 0)
    )
    .count()
)

record_result(
    "Fact_Sales UnitPrice validity",
    invalid_unit_price == 0,
    f"Invalid rows={invalid_unit_price}"
)


invalid_quantity = (
    gold_sales
    .filter(
        F.col("Quantity").isNull()
        | (F.col("Quantity") < 0)
    )
    .count()
)

record_result(
    "Fact_Sales Quantity validity",
    invalid_quantity == 0,
    f"Invalid rows={invalid_quantity}"
)


invalid_discount = (
    gold_sales
    .filter(
        F.col("Discount").isNull()
        |
        (F.col("Discount") < 0)
        |
        (F.col("Discount") > 1)
    )
    .count()
)

record_result(
    "Fact_Sales Discount validity",
    invalid_discount == 0,
    f"Invalid rows={invalid_discount}"
)


invalid_line_count = (
    gold_sales
    .filter(
        F.col("LineCount").isNull()
        |
        (F.col("LineCount") != 1)
    )
    .count()
)

record_result(
    "Fact_Sales LineCount validity",
    invalid_line_count == 0,
    f"Invalid rows={invalid_line_count}"
)

# ============================================================
# 8. FACT_ORDERS — PHYSICAL MEASURES
# ============================================================

print()
print("=" * 90)
print("FACT_ORDERS — PHYSICAL MEASURES")
print("=" * 90)


gold_orders = (
    gold_dataframes_versioned["Fact_Orders"]
)


silver_orders = (
    spark.table("Silver_Orders")
    .filter(F.col("IsCurrent") == 1)
)


# ------------------------------------------------------------
# Silver expected order measures
# ------------------------------------------------------------

silver_order_metrics = (
    silver_orders
    .agg(
        F.count("*").alias("OrderCount"),
        F.sum("Freight").alias("Freight")
    )
    .collect()[0]
)


# ------------------------------------------------------------
# Gold measures
# ------------------------------------------------------------

gold_order_metrics = (
    gold_orders
    .agg(
        F.sum("OrderCount").alias("OrderCount"),
        F.sum("Freight").alias("Freight")
    )
    .collect()[0]
)


record_result(
    "Fact_Orders OrderCount",
    int(gold_order_metrics["OrderCount"] or 0)
    ==
    int(silver_order_metrics["OrderCount"] or 0),
    (
        f"Gold={gold_order_metrics['OrderCount']}, "
        f"Silver={silver_order_metrics['OrderCount']}"
    )
)


compare_decimal(
    "Fact_Orders Freight",
    gold_order_metrics["Freight"],
    silver_order_metrics["Freight"]
)


# ============================================================
# 9. FACT_ORDERS GRAIN / DUPLICATE CHECK
# ============================================================

duplicate_orders = (
    gold_orders
    .groupBy("OrderID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)


record_result(
    "Fact_Orders unique OrderID",
    duplicate_orders == 0,
    f"Duplicate OrderIDs={duplicate_orders}"
)


# ============================================================
# 10. FACT_ORDER_DETAILS GRAIN CHECK
# ============================================================

duplicate_order_details = (
    gold_sales
    .groupBy(
        "OrderID",
        "ProductKey"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Fact_Order_Details repeated OrderID/ProductKey combinations:",
    duplicate_order_details
)


# NOTE:
# This is NOT automatically a failure because the same product
# can legitimately appear on multiple lines in some source data.
#
# Therefore we only report it.

print(
    "Fact_Order_Details repeated OrderID/ProductID combinations:",
    duplicate_order_details
)


# ============================================================
# 11. OPEN WAREHOUSE CONNECTION
# ============================================================

print()
print("=" * 90)
print("FOREIGN KEY VALIDATION")
print("=" * 90)

connection = get_warehouse_connection()
cursor = connection.cursor()


try:

    # ========================================================
    # 12. FACT_ORDERS → CUSTOMER
    # ========================================================

    invalid = sql_scalar(
        cursor,
        """
        SELECT COUNT(*)
        FROM dbo.Fact_Orders f
        LEFT JOIN dbo.Dim_Customer d
            ON f.CustomerKey = d.CustomerKey
        WHERE f.CustomerKey IS NOT NULL
          AND d.CustomerKey IS NULL
        """
    )

    record_result(
        "Fact_Orders → Dim_Customer",
        invalid == 0,
        f"Invalid CustomerKeys={invalid}"
    )


    # ========================================================
    # 13. FACT_ORDERS → EMPLOYEE
    # ========================================================

    invalid = sql_scalar(
        cursor,
        """
        SELECT COUNT(*)
        FROM dbo.Fact_Orders f
        LEFT JOIN dbo.Dim_Employee d
            ON f.EmployeeKey = d.EmployeeKey
        WHERE f.EmployeeKey IS NOT NULL
          AND d.EmployeeKey IS NULL
        """
    )

    record_result(
        "Fact_Orders → Dim_Employee",
        invalid == 0,
        f"Invalid EmployeeKeys={invalid}"
    )


    # ========================================================
    # 14. FACT_ORDERS → SHIPPER
    # ========================================================

    invalid = sql_scalar(
        cursor,
        """
        SELECT COUNT(*)
        FROM dbo.Fact_Orders f
        LEFT JOIN dbo.Dim_Shipper d
            ON f.ShipperKey = d.ShipperKey
        WHERE f.ShipperKey IS NOT NULL
          AND d.ShipperKey IS NULL
        """
    )

    record_result(
        "Fact_Orders → Dim_Shipper",
        invalid == 0,
        f"Invalid ShipperKeys={invalid}"
    )


    # ========================================================
    # 15. FACT_ORDERS → ORDER DATE
    # ========================================================

    invalid = sql_scalar(
        cursor,
        """
        SELECT COUNT(*)
        FROM dbo.Fact_Orders f
        LEFT JOIN dbo.Dim_Date d
            ON f.OrderDateKey = d.DateKey
        WHERE f.OrderDateKey IS NOT NULL
          AND d.DateKey IS NULL
        """
    )

    record_result(
        "Fact_Orders → Dim_Date (OrderDate)",
        invalid == 0,
        f"Invalid OrderDateKeys={invalid}"
    )


    # ========================================================
    # 16. FACT_ORDERS → REQUIRED DATE
    # ========================================================

    invalid = sql_scalar(
        cursor,
        """
        SELECT COUNT(*)
        FROM dbo.Fact_Orders f
        LEFT JOIN dbo.Dim_Date d
            ON f.RequiredDateKey = d.DateKey
        WHERE f.RequiredDateKey IS NOT NULL
          AND d.DateKey IS NULL
        """
    )

    record_result(
        "Fact_Orders → Dim_Date (RequiredDate)",
        invalid == 0,
        f"Invalid RequiredDateKeys={invalid}"
    )


    # ========================================================
    # 17. FACT_ORDERS → SHIPPED DATE
    # ========================================================
    #
    # NULL ShippedDateKey is VALID.
    #
    # It represents:
    #   ShippedDate = NULL
    #   OR
    #   ShippedDate = 9999-12-31
    #
    # Any non-null key must exist in Dim_Date.
    # ========================================================

    invalid = sql_scalar(
        cursor,
        """
        SELECT COUNT(*)
        FROM dbo.Fact_Orders f
        LEFT JOIN dbo.Dim_Date d
            ON f.ShippedDateKey = d.DateKey
        WHERE f.ShippedDateKey IS NOT NULL
          AND d.DateKey IS NULL
        """
    )

    record_result(
        "Fact_Orders → Dim_Date (ShippedDate)",
        invalid == 0,
        f"Invalid ShippedDateKeys={invalid}"
    )


    # ========================================================
    # 18. FACT_ORDER_DETAILS → PRODUCT
    # ========================================================

    invalid = sql_scalar(
        cursor,
        """
        SELECT COUNT(*)
        FROM dbo.Fact_Order_Details f
        LEFT JOIN dbo.Dim_Product d
            ON f.ProductKey = d.ProductKey
        WHERE f.ProductKey IS NOT NULL
          AND d.ProductKey IS NULL
        """
    )

    record_result(
        "Fact_Order_Details → Dim_Product",
        invalid == 0,
        f"Invalid ProductKeys={invalid}"
    )


    # ========================================================
    # 19. FACT_ORDER_DETAILS → CUSTOMER
    # ========================================================

    invalid = sql_scalar(
        cursor,
        """
        SELECT COUNT(*)
        FROM dbo.Fact_Order_Details f
        LEFT JOIN dbo.Dim_Customer d
            ON f.CustomerKey = d.CustomerKey
        WHERE f.CustomerKey IS NOT NULL
          AND d.CustomerKey IS NULL
        """
    )

    record_result(
        "Fact_Order_Details → Dim_Customer",
        invalid == 0,
        f"Invalid CustomerKeys={invalid}"
    )


    # ========================================================
    # 20. FACT_ORDER_DETAILS → DATE
    # ========================================================

    invalid = sql_scalar(
        cursor,
        """
        SELECT COUNT(*)
        FROM dbo.Fact_Order_Details f
        LEFT JOIN dbo.Dim_Date d
            ON f.OrderDateKey = d.DateKey
        WHERE f.OrderDateKey IS NOT NULL
          AND d.DateKey IS NULL
        """
    )

    record_result(
        "Fact_Order_Details → Dim_Date",
        invalid == 0,
        f"Invalid OrderDateKeys={invalid}"
    )


finally:

    cursor.close()
    connection.close()

    print()
    print("Warehouse validation connection closed.")


# ============================================================
# 21. FINAL SUMMARY
# ============================================================

results_df = pd.DataFrame(
    validation_results
)

print()
print("=" * 90)
print("FINAL VALIDATION SUMMARY")
print("=" * 90)

print()

print(
    results_df[
        ["Check", "Status", "Details"]
    ].to_string(index=False)
)

total_checks = len(results_df)

passed_checks = (
    results_df["Status"]
    .eq("PASS")
    .sum()
)

failed_checks = (
    results_df["Status"]
    .eq("FAIL")
    .sum()
)

print()
print("=" * 90)

print(
    f"TOTAL CHECKS : {total_checks}"
)

print(
    f"PASSED       : {passed_checks}"
)

print(
    f"FAILED       : {failed_checks}"
)

print()

if failed_checks == 0:

    print("=" * 90)
    print("FINAL RESULT: PASS")
    print("=" * 90)

    print(
        "All physical measures and foreign-key relationships "
        "passed validation."
    )

else:

    print("=" * 90)
    print("FINAL RESULT: FAIL")
    print("=" * 90)

    print(
        "One or more validations failed. "
        "Review the checks marked FAIL."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
