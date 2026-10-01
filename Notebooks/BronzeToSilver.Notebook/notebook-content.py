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

# Parameters

source_layer = "Bronze"
target_layer = "Silver"

run_id = ""
excel_batch_id = "e5c91ef7cfce349a9020da7ef23d4cf02b090d6d8518904bf7c36bf4a72fef36"

import uuid

if not run_id:
    run_id = str(uuid.uuid4())

print("Source layer:", source_layer)
print("Target layer:", target_layer)
print("Run ID:", run_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
validation_control = (
    spark.table("validation_control")
    .filter(F.col("IsActive") == 1)
)

pk_rows = (
    validation_control
    .filter(
        F.col("ValidationType").isin(
            "PRIMARY_KEY",
            "PRIMARY_KEY_COMPOSITE"
        )
    )
    .collect()
)

primary_keys = {}

for row in pk_rows:
    table_name = row["TableName"]

    primary_keys[table_name] = [
        c.strip()
        for c in row["ColumnName"].split(",")
    ]

print("Primary keys loaded:")
for table_name, keys in primary_keys.items():
    print(table_name, "->", keys)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_compare_columns(source_df):

    excluded_columns = {
        "SourceSystem",
        "SourceBatchId",
        "CreatedDate",
        "ModifiedDate",
        "Operation",
        "StartDate",
        "EndDate",
        "IsCurrent"
    }

    return [
        c
        for c in source_df.columns
        if c not in excluded_columns
    ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    BooleanType,
    DateType
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_bronze_version(table_name):

    history_df = spark.sql(
        f"DESCRIBE HISTORY {table_name}"
    )

    latest_version = (
        history_df
        .select("version")
        .orderBy(F.desc("version"))
        .first()["version"]
    )

    return int(latest_version)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_last_processed_version(table_name):

    rows = (
        spark.table("silver_processing_control")
        .filter(
            F.col("table_name") == table_name
        )
        .orderBy(
            F.desc("processed_at")
        )
        .select("bronze_version")
        .limit(1)
        .collect()
    )

    if not rows:
        return None

    return int(rows[0]["bronze_version"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def apply_scd2(source_df, target_table, key_columns):
    """
    SCD Type 2 using row hashing.

    New record:
        INSERT as current

    Existing record + changed hash:
        CLOSE old version
        INSERT new version

    Existing record + same hash:
        DO NOTHING
    """

    # =========================================================
    # 1. FIRST LOAD
    # =========================================================

    if not spark.catalog.tableExists(target_table):

        result_df = add_scd_columns(source_df)

        (
            result_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )

        print(f"CREATED: {target_table}")

        return


    # =========================================================
    # 2. READ EXISTING SILVER
    # =========================================================

    silver_df = spark.table(target_table)

    # Only compare the business/source columns.
    compare_columns = get_compare_columns(source_df)

    compare_columns = [
        c
        for c in compare_columns
        if c in silver_df.columns
    ]

    # Current Silver records only.
    current_df = (
        silver_df
        .filter(F.col("IsCurrent") == True)
    )


    # =========================================================
    # 3. CREATE ROW HASHES
    # =========================================================

    # Convert every compared value to a string representation.
    # coalesce ensures NULL values are represented consistently.
    #
    # concat_ws + sha2 gives us one hash representing the
    # complete business row.

    def create_row_hash(df, alias_name):

        return F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(
                        F.col(f"{alias_name}.{c}").cast("string"),
                        F.lit("<NULL>")
                    )
                    for c in compare_columns
                ]
            ),
            256
        )


    # =========================================================
    # 4. JOIN SOURCE WITH CURRENT SILVER
    # =========================================================

    join_condition = [
        F.col(f"src.{k}") == F.col(f"tgt.{k}")
        for k in key_columns
    ]

    joined = (
        source_df.alias("src")
        .join(
            current_df.alias("tgt"),
            join_condition,
            "left"
        )
    )


    # =========================================================
    # 5. NEW RECORDS
    # =========================================================

    new_condition = F.col(
        f"tgt.{key_columns[0]}"
    ).isNull()


    new_rows = (
        joined
        .filter(new_condition)
        .select("src.*")
    )


    # =========================================================
    # 6. ROW HASH COMPARISON
    # =========================================================

    source_hash = create_row_hash(
        joined,
        "src"
    )

    target_hash = create_row_hash(
        joined,
        "tgt"
    )


    changed_rows = (
        joined
        .filter(
            (~new_condition) &
            (source_hash != target_hash)
        )
        .select("src.*")
    )


    # =========================================================
    # 7. COUNTS
    # =========================================================

    new_count = new_rows.count()
    changed_count = changed_rows.count()

    print(f"New records: {new_count}")
    print(f"Changed records: {changed_count}")


    # =========================================================
    # 8. NOTHING TO DO
    # =========================================================

    if new_count == 0 and changed_count == 0:

        print(
            f"SKIPPED: No new or changed records for "
            f"{target_table}"
        )

        return


    # =========================================================
    # 9. CLOSE OLD VERSIONS
    # =========================================================

    if changed_count > 0:

        changed_keys = (
            changed_rows
            .select(*key_columns)
            .dropDuplicates()
        )

        delta_target = DeltaTable.forName(
            spark,
            target_table
        )

        merge_condition = " AND ".join(
            [
                f"t.{k} = s.{k}"
                for k in key_columns
            ]
        )

        (
            delta_target.alias("t")
            .merge(
                changed_keys.alias("s"),
                f"{merge_condition} AND t.IsCurrent = true"
            )
            .whenMatchedUpdate(
                set={
                    "EndDate": "current_date()",
                    "IsCurrent": "false"
                }
            )
            .execute()
        )

        print(
            f"CLOSED: {changed_count} old Silver versions"
        )


    # =========================================================
    # 10. INSERT NEW + CHANGED RECORDS
    # =========================================================

    rows_to_insert = (
        new_rows
        .unionByName(changed_rows)
    )

    rows_to_insert = add_scd_columns(
        rows_to_insert
    )


    # Match existing Silver schema
    target_columns = spark.table(
        target_table
    ).columns

    rows_to_insert = rows_to_insert.select(
        *target_columns
    )


    # =========================================================
    # 11. WRITE
    # =========================================================

    (
        rows_to_insert.write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )

    print(
        f"UPDATED: {target_table} "
        f"({new_count} new, {changed_count} changed)"
    )

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


def record_silver_processing(
    table_name,
    bronze_version,
    run_id
):

    schema = StructType([
        StructField(
            "table_name",
            StringType(),
            False
        ),
        StructField(
            "bronze_version",
            LongType(),
            False
        ),
        StructField(
            "processed_at",
            TimestampType(),
            True
        ),
        StructField(
            "run_id",
            StringType(),
            True
        )
    ])

    record = [(
        table_name,
        int(bronze_version),
        None,
        run_id
    )]

    df = spark.createDataFrame(
        record,
        schema=schema
    )

    df = df.withColumn(
        "processed_at",
        F.current_timestamp()
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            "silver_processing_control"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_string_columns(df):

    for field in df.schema.fields:

        if isinstance(field.dataType, StringType):

            column_name = field.name

            df = df.withColumn(
                column_name,
                F.when(
                    F.trim(F.col(column_name)) == "",
                    None
                ).otherwise(
                    F.trim(F.col(column_name))
                )
            )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def cast_columns(df, type_map):

    for column_name, data_type in type_map.items():

        if column_name in df.columns:

            df = df.withColumn(
                column_name,
                F.col(column_name).cast(data_type)
            )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fill_categorical_nulls(
    df,
    columns,
    replacement="Unknown"
):

    for column_name in columns:

        if column_name in df.columns:

            df = df.withColumn(
                column_name,
                F.coalesce(
                    F.col(column_name),
                    F.lit(replacement)
                )
            )

    return df


def fill_numeric_nulls_with_median(
    df,
    columns
):

    for column_name in columns:

        if column_name not in df.columns:
            continue

        median_value = (
            df.select(
                F.expr(
                    f"percentile_approx({column_name}, 0.5)"
                ).alias("median")
            )
            .first()["median"]
        )

        if median_value is not None:

            df = df.withColumn(
                column_name,
                F.coalesce(
                    F.col(column_name),
                    F.lit(median_value)
                )
            )

    return df


def fill_numeric_nulls_with_zero(
    df,
    columns
):

    for column_name in columns:

        if column_name in df.columns:

            df = df.withColumn(
                column_name,
                F.coalesce(
                    F.col(column_name),
                    F.lit(0)
                )
            )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def standardize_categories(df, mappings):

    for column_name, mapping in mappings.items():

        mapping_expr = F.create_map(
            *[
                item
                for pair in mapping.items()
                for item in (
                    F.lit(pair[0]),
                    F.lit(pair[1])
                )
            ]
        )

        df = df.withColumn(
            column_name,
            F.coalesce(
                mapping_expr[F.col(column_name)],
                F.col(column_name)
            )
        )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def drop_columns(df, columns):

    existing_columns = [
        column
        for column in columns
        if column in df.columns
    ]

    if existing_columns:

        df = df.drop(*existing_columns)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_categories(df):

    df = clean_string_columns(df)

    df = cast_columns(
        df,
        {
            "CategoryID": "int"
        }
    )

    df = drop_columns(
        df,
        ["Picture"]
    )
    df = fill_categorical_nulls(
    df,
    ["CategoryName"],
    "Unknown"
    )

    df = fill_categorical_nulls(
        df,
        ["Description"],
        "Not Provided"
    )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_customers(df):

    df = clean_string_columns(df)

    df = cast_columns(
        df,
        {
            "CustomerID": "string"
        }
    )
    df = fill_categorical_nulls(
    df,
    [
        "Region",
        "Fax"
    ],
    "Not Provided"
    )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_employees(df):

    df = clean_string_columns(df)

    df = cast_columns(
        df,
        {
            "EmployeeID": "int",
            "ReportsTo": "int"
        }
    )

    df = drop_columns(
        df,
        [
            "Photo",
            "PhotoPath"
        ]
    )
    df = df.withColumn(
    "ReportsToMissing",
    F.col("ReportsTo").isNull()
    )

    df = df.withColumn(
        "ReportsTo",
        F.coalesce(
            F.col("ReportsTo"),
            F.lit(-1)
        )
    )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_products(df):

    df = clean_string_columns(df)

    df = cast_columns(
        df,
        {
            "ProductID": "int",
            "SupplierID": "int",
            "CategoryID": "int",
            "UnitPrice": DecimalType(18, 2),
            "UnitsInStock": "int",
            "UnitsOnOrder": "int",
            "ReorderLevel": "int",
            "Discontinued": "boolean"
        }
    )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_order_details(df):

    df = clean_string_columns(df)

    df = cast_columns(
        df,
        {
            "OrderID": "int",
            "ProductID": "int",
            "UnitPrice": DecimalType(18, 2),
            "Quantity": "int",
            "Discount": DecimalType(5, 2)
        }
    )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_orders(df):

    df = clean_string_columns(df)

    df = cast_columns(
        df,
        {
            "OrderID": "int",
            "CustomerID": "string",
            "EmployeeID": "int",
            "ShipVia": "int",
            "Freight": DecimalType(18, 2)
        }
    )
        # Handle missing ShipRegion
    df = df.withColumn(
        "ShipRegion",
        F.when(
            F.col("ShipRegion").isNull() |
            (F.trim(F.col("ShipRegion")) == ""),
            F.lit("Unknown")
        ).otherwise(
            F.trim(F.col("ShipRegion"))
        )
    )

    # Convert timestamps to dates
    for column_name in [
        "OrderDate",
        "RequiredDate",
        "ShippedDate"
    ]:

        if column_name in df.columns:

            df = df.withColumn(
                column_name,
                F.to_date(F.col(column_name))
            )
    df = df.withColumn(
    "ShippedStatus",
    F.when(
        F.col("ShippedDate").isNull(),
        F.lit("Not Shipped")
    )
    .otherwise(
        F.lit("Shipped")
    )
    )

    df = df.withColumn(
        "ShippedDate",
        F.coalesce(
            F.col("ShippedDate"),
            F.lit("9999-12-31").cast("date")
        )
    )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_regions(df):

    df = clean_string_columns(df)

    df = cast_columns(
        df,
        {
            "RegionID": "string"
        }
    )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_shippers(df):

    df = clean_string_columns(df)

    df = cast_columns(
        df,
        {
            "ShipperID": "int"
        }
    )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_suppliers(df):

    df = clean_string_columns(df)

    df = cast_columns(
        df,
        {
            "SupplierID": "int"
        }
    )

    df = fill_categorical_nulls(
    df,
    [
        "Region",
        "Fax",
        "HomePage"
    ],
    "Not Provided"
    )
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_territories(df):

    df = clean_string_columns(df)

    df = cast_columns(
        df,
        {
            "TerritoryID": "string",
            "RegionID": "string"
        }
    )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import DateType


def add_scd_columns(df):
    """
    Add SCD Type 2 metadata columns.
    Existing records are treated as current when first loaded.
    """

    return (
        df
        .withColumn("StartDate", F.current_date())
        .withColumn(
            "EndDate",
            F.lit(None).cast("date")
        )
        .withColumn("IsCurrent", F.lit(True))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

transformers = {

    "Categories": transform_categories,
    "Customers": transform_customers,
    "Employees": transform_employees,
    "Order_Details": transform_order_details,
    "Orders": transform_orders,
    "Products": transform_products,
    "Regions": transform_regions,
    "Shippers": transform_shippers,
    "Suppliers": transform_suppliers,
    "Territories": transform_territories
}


for table_name, transform_function in transformers.items():

    bronze_table = f"Bronze_{table_name}"
    silver_table = f"Silver_{table_name}"

    print("=" * 60)
    print(f"Processing: {table_name}")

    # ---------------------------------------------------------
    # 1. Get current Bronze Delta version
    # ---------------------------------------------------------

    current_bronze_version = get_bronze_version(
        bronze_table
    )

    print(
        f"Current Bronze version: "
        f"{current_bronze_version}"
    )

    # ---------------------------------------------------------
    # 2. Get version previously used for Silver
    # ---------------------------------------------------------

    last_processed_version = get_last_processed_version(
        table_name
    )

    print(
        f"Last processed Bronze version: "
        f"{last_processed_version}"
    )

   
    # ---------------------------------------------------------
    # 3. Decide whether this table really needs processing
    # ---------------------------------------------------------

    should_process = True

    if spark.catalog.tableExists(silver_table):

        # -----------------------------------------------------
        # API processing
        # -----------------------------------------------------

        if not excel_batch_id:

            if (
                last_processed_version is not None
                and current_bronze_version == last_processed_version
            ):
                should_process = False

        # -----------------------------------------------------
        # Excel processing
        # -----------------------------------------------------

        else:

            bronze_columns = spark.table(bronze_table).columns

            # Only tables containing Excel metadata can be
            # processed using SourceSystem / SourceBatchId
            if (
                "SourceSystem" in bronze_columns
                and "SourceBatchId" in bronze_columns
            ):

                excel_count = (
                    spark.table(bronze_table)
                    .filter(
                        (F.col("SourceSystem") == "Excel") &
                        (F.col("SourceBatchId") == excel_batch_id)
                    )
                    .count()
                )

                print(
                    f"Excel records available: {excel_count}"
                )

                if excel_count == 0:
                    should_process = False

                else:
                    print(
                        f"Excel batch {excel_batch_id} "
                        f"requires checking."
                )

            else:

                # This table has no Excel metadata,
                # so don't try to process it as an Excel table.
                print(
                    f"SKIPPED: {bronze_table} "
                    f"does not contain Excel metadata."
                )

                should_process = False


    if not should_process:

        print(
            f"SKIPPED: {silver_table} "
            f"is already up to date."
        )

        continue

    # ---------------------------------------------------------
    # 4. Read Bronze
    # ---------------------------------------------------------

    bronze_df = spark.table(bronze_table)

    # ---------------------------------------------------------
    # 5. Select ONLY the records belonging to this run
    #
    #    Excel run:
    #       SourceSystem = Excel
    #       SourceBatchId = excel_batch_id
    #
    #    API run:
    #       SourceSystem = API
    # ---------------------------------------------------------

    if excel_batch_id:

        print(
            f"Processing Excel batch: "
            f"{excel_batch_id}"
        )

        batch_df = bronze_df.filter(
            (F.col("SourceSystem") == "Excel") &
            (F.col("SourceBatchId") == excel_batch_id)
        )

    else:

        print(
            "Processing API source."
        )

        batch_df = bronze_df.filter(
            F.col("SourceSystem") == "API"
        )

    batch_count = batch_df.count()

    print(
        f"Records selected from Bronze: "
        f"{batch_count}"
    )

    # ---------------------------------------------------------
    # 6. Nothing new for this source/batch
    # ---------------------------------------------------------

    if batch_count == 0:

        print(
            f"SKIPPED: No records to process "
            f"for {table_name}"
        )

        # IMPORTANT:
        # Do not mark the Bronze version as processed here.
        # There was nothing to process.
        continue

    # ---------------------------------------------------------
    # 7. Transform ONLY the selected records
    # ---------------------------------------------------------

    print(
        f"TRANSFORMING: {bronze_table} "
        f"→ {silver_table}"
    )

    transformed_df = transform_function(
        batch_df
    )

    # ---------------------------------------------------------
    # 8. Get primary key
    # ---------------------------------------------------------

    key_columns = primary_keys.get(
        table_name,
        []
    )

    if not key_columns:

        raise ValueError(
            f"No primary key defined for {table_name}"
        )

    print(
        f"Primary key: {key_columns}"
    )

    # ---------------------------------------------------------
    # 9. Apply SCD Type 2
    # ---------------------------------------------------------

    apply_scd2(
        transformed_df,
        silver_table,
        key_columns
    )

    # ---------------------------------------------------------
    # 10. Record successful Bronze processing
    # ---------------------------------------------------------

    record_silver_processing(
        table_name,
        current_bronze_version,
        run_id
    )

    print(
        f"UPDATED: {silver_table}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
