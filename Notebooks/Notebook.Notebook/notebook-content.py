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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

schema = StructType([
    StructField("TableName", StringType(), False),
    StructField("RelativeURL", StringType(), False),
    StructField("BronzeTable", StringType(), False),
    StructField("IsActive", IntegerType(), False),
    StructField("SpecialHandling", IntegerType(), False),
    StructField("Status", StringType(), False),
    StructField("LastRunID", StringType(), True),
    StructField("ErrorMessage", StringType(), True)
])

data = [
    ("Customers", "Customers", "Bronze_Customers", 1, 0, "PENDING", None, None),
    ("Categories", "Categories", "Bronze_Categories", 1, 0, "PENDING", None, None),
    ("Suppliers", "Suppliers", "Bronze_Suppliers", 1, 0, "PENDING", None, None),
    ("Employees", "Employees", "Bronze_Employees", 1, 0, "PENDING", None, None),
    ("Shippers", "Shippers", "Bronze_Shippers", 1, 0, "PENDING", None, None),
    ("Regions", "Regions", "Bronze_Regions", 1, 0, "PENDING", None, None),
    ("Territories", "Territories", "Bronze_Territories", 1, 0, "PENDING", None, None),
    ("Products", "Products", "Bronze_Products", 1, 0, "PENDING", None, None),
    ("Orders", "Orders", "Bronze_Orders", 1, 0, "PENDING", None, None),
    ("Order_Details", "Order_Details", "Bronze_Order_Details", 1, 0, "PENDING", None, None)
]

df = spark.createDataFrame(data, schema)

df.write.format("delta").mode("overwrite").saveAsTable("Ingestion_Control")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validation_schema = StructType([
    StructField("TableName", StringType(), False),
    StructField("ValidationType", StringType(), False),
    StructField("ColumnName", StringType(), False),
    StructField("ReferencedTable", StringType(), True),
    StructField("ReferencedColumn", StringType(), True),
    StructField("IsActive", IntegerType(), False)
])

validation_data = [

    # Primary Key validations
    ("Customers", "PRIMARY_KEY", "CustomerID", None, None, 1),
    ("Categories", "PRIMARY_KEY", "CategoryID", None, None, 1),
    ("Suppliers", "PRIMARY_KEY", "SupplierID", None, None, 1),
    ("Employees", "PRIMARY_KEY", "EmployeeID", None, None, 1),
    ("Shippers", "PRIMARY_KEY", "ShipperID", None, None, 1),
    ("Regions", "PRIMARY_KEY", "RegionID", None, None, 1),
    ("Territories", "PRIMARY_KEY", "TerritoryID", None, None, 1),
    ("Products", "PRIMARY_KEY", "ProductID", None, None, 1),
    ("Orders", "PRIMARY_KEY", "OrderID", None, None, 1),

    # Order Details has a composite primary key
    ("Order_Details", "PRIMARY_KEY_COMPOSITE", "OrderID,ProductID", None, None, 1),

    # Foreign Keys
    ("Products", "FOREIGN_KEY", "CategoryID", "Categories", "CategoryID", 1),
    ("Products", "FOREIGN_KEY", "SupplierID", "Suppliers", "SupplierID", 1),

    ("Orders", "FOREIGN_KEY", "CustomerID", "Customers", "CustomerID", 1),
    ("Orders", "FOREIGN_KEY", "EmployeeID", "Employees", "EmployeeID", 1),
    ("Orders", "FOREIGN_KEY", "ShipVia", "Shippers", "ShipperID", 1),

    ("Territories", "FOREIGN_KEY", "RegionID", "Regions", "RegionID", 1),

    ("Order_Details", "FOREIGN_KEY", "OrderID", "Orders", "OrderID", 1),
    ("Order_Details", "FOREIGN_KEY", "ProductID", "Products", "ProductID", 1)
]

validation_df = spark.createDataFrame(
    validation_data,
    validation_schema
)

validation_df.write.format("delta").mode("overwrite").saveAsTable(
    "Validation_Control"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
ALTER TABLE ingestion_control
ADD COLUMNS (Translator STRING)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

schema = StructType([
    StructField("TableName", StringType(), False),
    StructField("RelativeURL", StringType(), False),
    StructField("BronzeTable", StringType(), False),
    StructField("IsActive", IntegerType(), False),
    StructField("Status", StringType(), False)
])

data = [
    ("Customers", "Customers", "Bronze_Customers", 1, "PENDING"),
    ("Categories", "Categories", "Bronze_Categories", 1, "PENDING"),
    ("Suppliers", "Suppliers", "Bronze_Suppliers", 1, "PENDING"),
    ("Employees", "Employees", "Bronze_Employees", 1, "PENDING"),
    ("Shippers", "Shippers", "Bronze_Shippers", 1, "PENDING"),
    ("Regions", "Regions", "Bronze_Regions", 1, "PENDING"),
    ("Territories", "Territories", "Bronze_Territories", 1, "PENDING"),
    ("Products", "Products", "Bronze_Products", 1, "PENDING"),
    ("Orders", "Orders", "Bronze_Orders", 1, "PENDING"),
    ("Order_Details", "Order_Details", "Bronze_Order_Details", 1, "PENDING")
]

df = spark.createDataFrame(data, schema)

df.write.format("delta").mode("overwrite").saveAsTable(
    "Ingestion_Control"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

column_types = {

    "Categories": {
        "CategoryID": "Int64",
        "CategoryName": "String",
        "Description": "String",
        "Picture": "String"
    },

    "CustomerDemographics": {
        "CustomerTypeID": "String",
        "CustomerDesc": "String"
    },

    "Customers": {
        "CustomerID": "String",
        "CompanyName": "String",
        "ContactName": "String",
        "ContactTitle": "String",
        "Address": "String",
        "City": "String",
        "Region": "String",
        "PostalCode": "String",
        "Country": "String",
        "Phone": "String",
        "Fax": "String"
    },

    "Employees": {
        "EmployeeID": "Int64",
        "LastName": "String",
        "FirstName": "String",
        "Title": "String",
        "TitleOfCourtesy": "String",
        "BirthDate": "DateTime",
        "HireDate": "DateTime",
        "Address": "String",
        "City": "String",
        "Region": "String",
        "PostalCode": "String",
        "Country": "String",
        "HomePhone": "String",
        "Extension": "String",
        "Photo": "String",
        "Notes": "String",
        "ReportsTo": "Int64",
        "PhotoPath": "String"
    },

    "Order_Details": {
        "OrderID": "Int64",
        "ProductID": "Int64",
        "UnitPrice": "Double",
        "Quantity": "Int64",
        "Discount": "Double"
    },

    "Orders": {
        "OrderID": "Int64",
        "CustomerID": "String",
        "EmployeeID": "Int64",
        "OrderDate": "DateTime",
        "RequiredDate": "DateTime",
        "ShippedDate": "DateTime",
        "ShipVia": "Int64",
        "Freight": "Double",
        "ShipName": "String",
        "ShipAddress": "String",
        "ShipCity": "String",
        "ShipRegion": "String",
        "ShipPostalCode": "String",
        "ShipCountry": "String"
    },

    "Products": {
        "ProductID": "Int64",
        "ProductName": "String",
        "SupplierID": "Int64",
        "CategoryID": "Int64",
        "QuantityPerUnit": "String",
        "UnitPrice": "Double",
        "UnitsInStock": "Int64",
        "UnitsOnOrder": "Int64",
        "ReorderLevel": "Int64",
        "Discontinued": "Boolean"
    },

    "Regions": {
        "RegionID": "String",
        "RegionDescription": "String"
    },

    "Shippers": {
        "ShipperID": "Int64",
        "CompanyName": "String",
        "Phone": "String"
    },

    "Suppliers": {
        "SupplierID": "Int64",
        "CompanyName": "String",
        "ContactName": "String",
        "ContactTitle": "String",
        "Address": "String",
        "City": "String",
        "Region": "String",
        "PostalCode": "String",
        "Country": "String",
        "Phone": "String",
        "Fax": "String",
        "HomePage": "String"
    },

    "Territories": {
        "TerritoryID": "String",
        "TerritoryDescription": "String",
        "RegionID": "String"
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from delta.tables import DeltaTable

table_columns = {

    "Categories": [
        "CategoryID",
        "CategoryName",
        "Description",
        "Picture"
    ],


    "Customers": [
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
        "Fax"
    ],

    "Employees": [
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
        "Photo",
        "Notes",
        "ReportsTo",
        "PhotoPath"
    ],

    "Order_Details": [
        "OrderID",
        "ProductID",
        "UnitPrice",
        "Quantity",
        "Discount"
    ],

    "Orders": [
        "OrderID",
        "CustomerID",
        "EmployeeID",
        "OrderDate",
        "RequiredDate",
        "ShippedDate",
        "ShipVia",
        "Freight",
        "ShipName",
        "ShipAddress",
        "ShipCity",
        "ShipRegion",
        "ShipPostalCode",
        "ShipCountry"
    ],

    "Products": [
        "ProductID",
        "ProductName",
        "SupplierID",
        "CategoryID",
        "QuantityPerUnit",
        "UnitPrice",
        "UnitsInStock",
        "UnitsOnOrder",
        "ReorderLevel",
        "Discontinued"
    ],

    "Regions": [
        "RegionID",
        "RegionDescription"
    ],

    "Shippers": [
        "ShipperID",
        "CompanyName",
        "Phone"
    ],

    "Suppliers": [
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
        "HomePage"
    ],

    "Territories": [
        "TerritoryID",
        "TerritoryDescription",
        "RegionID"
    ]
}


# Build translator JSON for every table
mapping_rows = []

for table_name, columns in table_columns.items():

    mappings = []

    for column in columns:

        data_type = column_types[table_name][column]

        mappings.append({
            "source": {
                "path": f"['{column}']",
                "type": data_type
            },
            "sink": {
                "name": column,
                "type": data_type
            }
        })

    translator = {
        "type": "TabularTranslator",
        "collectionReference": "$['value']",
        "mappings": mappings
    }

    mapping_rows.append(
        (
            table_name,
            f"Bronze_{table_name}",
            json.dumps(translator, separators=(",", ":"))
        )
    )

mapping_df = spark.createDataFrame(
    mapping_rows,
    ["RelativeURL", "BronzeTable", "Translator"]
)

delta_table = DeltaTable.forName(
    spark,
    "ingestion_control"
)

(
    delta_table.alias("target")
    .merge(
        mapping_df.alias("source"),
        "target.RelativeURL = source.RelativeURL"
    )
    .whenMatchedUpdate(
        set={
            "Translator": "source.Translator"
        }
    )
    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from datetime import datetime

schema = StructType([
    StructField("TableName", StringType(), True),
    StructField("ValidationType", StringType(), True),
    StructField("ColumnName", StringType(), True),
    StructField("ReferencedTable", StringType(), True),
    StructField("ReferencedColumn", StringType(), True),
    StructField("Result", StringType(), True),
    StructField("ProblemCount", LongType(), True),
    StructField("RowCount", LongType(), True),
    StructField("Message", StringType(), True),
    StructField("CheckedAt", TimestampType(), True)
])

empty_df = spark.createDataFrame([], schema)

empty_df.write.format("delta").mode("overwrite").saveAsTable(
    "validation_results"
)

print("validation_results created successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Drop Silver tables
silver_tables = [
    "silver_categories",
    "silver_customers",
    "silver_employees",
    "silver_order_details",
    "silver_orders",
    "silver_products",
    "silver_regions",
    "silver_shippers",
    "silver_suppliers",
    "silver_territories"
]

for table in silver_tables:
    spark.sql(f"DROP TABLE IF EXISTS `{table}`")
    print(f"Dropped {table}")


# Drop old profiling and validation results
spark.sql("DROP TABLE IF EXISTS Data_Profiling_Results")
spark.sql("DROP TABLE IF EXISTS Validation_Results")

print("Cleanup completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS bronze_completness_results")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType
)

schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("bronze_version", LongType(), False),
    StructField("processed_at", TimestampType(), True),
    StructField("run_id", StringType(), True)
])

empty_df = spark.createDataFrame([], schema)

(
    empty_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_processing_control")
)

print("silver_processing_control created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS validation_results")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row

control_data = [
    Row(
        TableName="Customers",
        SheetName="Customers",
        BronzeTable="Bronze_Customers",
        IsActive=1
    ),
    Row(
        TableName="Products",
        SheetName="Products",
        BronzeTable="Bronze_Products",
        IsActive=1
    ),
    Row(
        TableName="Orders",
        SheetName="Orders",
        BronzeTable="Bronze_Orders",
        IsActive=1
    ),
    Row(
        TableName="OrderDetails",
        SheetName="OrderDetails",
        BronzeTable="Bronze_Order_Details",
        IsActive=1
    )
]

control_df = spark.createDataFrame(control_data)

(
    control_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("incremental_ingestion_control")
)

display(control_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_tables = [
    "Bronze_Customers",
    "Bronze_Products",
    "Bronze_Orders",
    "Bronze_Order_Details"
]

for table_name in bronze_tables:

    columns = [
        field.name
        for field in spark.table(table_name).schema.fields
    ]

    if "SourceSystem" not in columns:
        spark.sql(f"""
            ALTER TABLE {table_name}
            ADD COLUMNS (SourceSystem STRING)
        """)

    columns = [
        field.name
        for field in spark.table(table_name).schema.fields
    ]

    if "SourceBatchId" not in columns:
        spark.sql(f"""
            ALTER TABLE {table_name}
            ADD COLUMNS (SourceBatchId STRING)
        """)

    print(f"Metadata checked: {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table_name in [
    "Bronze_Customers",
    "Bronze_Products",
    "Bronze_Orders",
    "Bronze_Order_Details"
]:
    spark.sql(f"""
        UPDATE {table_name}
        SET SourceSystem = 'API'
        WHERE SourceSystem IS NULL
    """)

    print(f"Updated metadata for {table_name}")
    print(f"\n--- {table_name} ---")
    spark.table(table_name).select(
        "SourceSystem",
        "SourceBatchId"
    ).show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
ALTER TABLE bronze_completeness_results
ADD COLUMNS (
    source_type STRING
)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table_name in [
    "Silver_Customers",
    "Silver_Order_Details",
    "Silver_Orders",
    "Silver_Products"
]:
    
    print("\n" + "=" * 70)
    print(table_name)
    
    (
        DeltaTable
        .forName(spark, table_name)
        .history(10)
        .select(
            "version",
            "timestamp",
            "operation"
        )
        .orderBy("version", ascending=False)
        .show(10, False)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

# =========================================================
# RESTORE SILVER TABLES TO STATE BEFORE EXCEL SCD2 TEST
# =========================================================

restore_versions = {
    "Silver_Customers": 1,
    "Silver_Order_Details": 1,
    "Silver_Orders": 2,
    "Silver_Products": 1
}

for table_name, version in restore_versions.items():

    print("=" * 70)
    print(f"Restoring {table_name} to version {version}")

    delta_table = DeltaTable.forName(
        spark,
        table_name
    )

    delta_table.restoreToVersion(version)

    print(f"RESTORED: {table_name} -> version {version}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT
    CustomerID,
    CompanyName,
    ContactName,
    StartDate,
    EndDate,
    IsCurrent
FROM silver_customers
WHERE CustomerID = 'CHOPS'
ORDER BY StartDate, IsCurrent
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table in [
    "silver_customers",
    "silver_order_details",
    "silver_orders",
    "silver_products"
]:

    print("\n" + "=" * 70)
    print(table)

    spark.sql(f"""
        SELECT
            COUNT(*) AS TotalRows,
            SUM(CASE WHEN IsCurrent = true THEN 1 ELSE 0 END) AS CurrentRows,
            SUM(CASE WHEN IsCurrent = false THEN 1 ELSE 0 END) AS HistoricalRows
        FROM {table}
    """).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
