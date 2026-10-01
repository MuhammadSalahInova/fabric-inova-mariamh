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

layer = "Bronze"
run_id = ""
base_url = "https://services.odata.org/V4/Northwind/Northwind.svc"
excel_batch_id = "e5c91ef7cfce349a9020da7ef23d4cf02b090d6d8518904bf7c36bf4a72fef36"
excel_file_path = "/lakehouse/default/Files/FakeStore_Orders_OrderDetails_Customers_Products_Linked.xlsx"
source_type = "Excel"

import uuid

if not run_id:
    run_id = str(uuid.uuid4())

print("Layer:", layer)
print("Run ID:", run_id)
print("Excel Batch ID:", excel_batch_id)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import NumericType, DateType, TimestampType
import requests
import pandas as pd

excel_source = pd.read_excel(
    excel_file_path,
    sheet_name=None
)

print("Excel sheets found:")
for sheet_name in excel_source.keys():
    print(" -", sheet_name)
    
# Active ingestion metadata drives which source endpoints are checked.
control_df = (
    spark.table("ingestion_control")
    .filter(F.col("IsActive") == 1)
)

control_rows = control_df.collect()

# Primary-key metadata comes from the existing validation_control table.
validation_control = (
    spark.table("validation_control")
    .filter(F.col("IsActive") == 1)
)

pk_rows = validation_control.filter(
    F.col("ValidationType").isin("PRIMARY_KEY", "PRIMARY_KEY_COMPOSITE")
).collect()

primary_keys = {}
for row in pk_rows:
    primary_keys[row["TableName"]] = [
        c.strip() for c in row["ColumnName"].split(",")
    ]

print("Tables to validate:", len(control_rows))
print("Primary-key definitions:", len(primary_keys))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

excel_sheet_map = {
    "Customers": "Customers",
    "Orders": "Orders",
    "OrderDetails": "Order_Details",
    "Products": "Products"
}

def get_excel_source_records(entity_name):

    # Find the Excel sheet corresponding to this entity
    sheet_name = None

    for excel_sheet, entity in excel_sheet_map.items():
        if entity == entity_name:
            sheet_name = excel_sheet
            break

    if sheet_name is None:
        return None

    if sheet_name not in excel_source:
        raise ValueError(
            f"Excel sheet '{sheet_name}' was not found."
        )

    df = excel_source[sheet_name]

    # Convert NaN to None
    df = df.where(pd.notnull(df), None)

    records = df.to_dict(orient="records")

    return records

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_excel_bronze_df(table_name, batch_id):

    return (
        spark.table(table_name)
        .filter(
            (F.col("SourceSystem") == "Excel") &
            (F.col("SourceBatchId") == batch_id)
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_source_records(relative_url):
    """Read all pages from the Northwind OData endpoint."""
    url = relative_url
    if not url.startswith("http"):
        url = f"{base_url.rstrip('/')}/{url.lstrip('/')}"

    records = []

    while url:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        payload = response.json()

        records.extend(payload.get("value", []))

        next_link = payload.get("@odata.nextLink")
        if next_link:
            if next_link.startswith("http"):
                url = next_link
            else:
                url = f"{base_url.rstrip('/')}/{next_link.lstrip('/')}"
        else:
            url = None

    return records


def normalize_source_value(value):
    """
    Normalize API values before comparing them with Spark values.
    This prevents false failures such as:
    1998-05-06T00:00:00Z vs 1998-05-06 00:00:00
    0 vs 0.0
    """

    if value is None:
        return None

    text = str(value)

    # Normalize OData UTC timestamp representation.
    if text.endswith("Z"):
        text = text[:-1]

    if "T" in text:
        text = text.replace("T", " ")

    # Normalize numeric strings such as 0.0 -> 0
    try:
        numeric = float(text)

        if numeric.is_integer():
            return str(int(numeric))

        return str(numeric)

    except Exception:
        return text.strip()


def source_column_stats(records, columns, bronze_df):
    stats = {}
    total = len(records)

    bronze_types = {
        field.name: field.dataType
        for field in bronze_df.schema.fields
    }

    for column in columns:
        values = [r.get(column) for r in records]
        non_null = [v for v in values if v is not None]

        null_count = total - len(non_null)

        min_value = None
        max_value = None

        # Only calculate min/max for numeric/date columns.
        dtype = bronze_types.get(column)

        if non_null and isinstance(
            dtype,
            (NumericType, DateType, TimestampType)
        ):
            try:
                # -------------------------------------------------
                # Numeric columns
                # -------------------------------------------------
                if isinstance(dtype, NumericType):

                    # Convert all source values to float first.
                    # This prevents LongType/DoubleType merge errors.
                    converted_values = [
                        float(v) for v in non_null
                    ]

                    min_value = min(converted_values)
                    max_value = max(converted_values)

                # -------------------------------------------------
                # Date / Timestamp columns
                # -------------------------------------------------
                elif isinstance(dtype, (DateType, TimestampType)):

                    temp_df = spark.createDataFrame(
                        [(str(v),) for v in non_null],
                        [column]
                    )

                    temp_df = temp_df.withColumn(
                        column,
                        F.to_timestamp(F.col(column))
                    )

                    stats_row = temp_df.select(
                        F.min(F.col(column)).alias("min_value"),
                        F.max(F.col(column)).alias("max_value")
                    ).first()

                    min_value = stats_row["min_value"]
                    max_value = stats_row["max_value"]

            except Exception as e:
                print(
                    f"Could not calculate numeric/date min/max "
                    f"for {column}: {e}"
                )

        stats[column] = {
            "null_count": null_count,
            "min_value": min_value,
            "max_value": max_value
        }

    return stats
        


def source_duplicate_count(records, key_columns):
    if not key_columns:
        return None

    counts = {}
    for record in records:
        key = tuple(record.get(c) for c in key_columns)
        if any(v is None for v in key):
            continue
        counts[key] = counts.get(key, 0) + 1

    return sum(1 for count in counts.values() if count > 1)

def compare_key_sets(
    source_records,
    bronze_df,
    key_columns
):
    """
    Compare source primary-key values with Bronze primary-key values.

    Returns:
        missing_in_bronze
        extra_in_bronze
    """

    if not key_columns:
        return None, None

    # ---------------------------------------------------------
    # Source keys
    # ---------------------------------------------------------

    source_keys = set()

    for record in source_records:

        key = tuple(
            normalize_source_value(
                record.get(column)
            )
            for column in key_columns
        )

        source_keys.add(key)


    # ---------------------------------------------------------
    # Bronze keys
    # ---------------------------------------------------------

    bronze_rows = (
        bronze_df
        .select(*key_columns)
        .collect()
    )

    bronze_keys = set()

    for row in bronze_rows:

        key = tuple(
            normalize_source_value(
                row[column]
            )
            for column in key_columns
        )

        bronze_keys.add(key)


    # ---------------------------------------------------------
    # Compare
    # ---------------------------------------------------------

    missing_in_bronze = (
        source_keys - bronze_keys
    )

    extra_in_bronze = (
        bronze_keys - source_keys
    )

    return (
        missing_in_bronze,
        extra_in_bronze
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_bronze_source_df(
    bronze_df,
    source_type,
    source_batch_id=None
):
    """
    Return only the Bronze records belonging to the
    source being validated.

    API:
        SourceSystem = API

    Excel:
        SourceSystem = Excel
        and optionally SourceBatchId = source_batch_id

    Older Bronze tables that do not contain source metadata:
        return the Bronze table unchanged.
    """

    # ---------------------------------------------------------
    # If Bronze does not contain source metadata,
    # we cannot filter it by source.
    # ---------------------------------------------------------

    if "SourceSystem" not in bronze_df.columns:

        print(
            "WARNING: Bronze table does not contain "
            "SourceSystem metadata."
        )

        return bronze_df


    # ---------------------------------------------------------
    # API
    # ---------------------------------------------------------

    if source_type == "API":

        return bronze_df.filter(
            F.col("SourceSystem") == "API"
        )


    # ---------------------------------------------------------
    # Excel
    # ---------------------------------------------------------

    if source_type == "Excel":

        result = bronze_df.filter(
            F.col("SourceSystem") == "Excel"
        )

        if (
            source_batch_id is not None
            and "SourceBatchId" in bronze_df.columns
        ):

            result = result.filter(
                F.col("SourceBatchId") == source_batch_id
            )

        return result


    raise ValueError(
        f"Unsupported source type: {source_type}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# BRONZE SOURCE COMPLETENESS VALIDATION
# ============================================================
#
# Validates actual Bronze data against the actual source:
#
#   1. API  -> current API data vs API Bronze records
#   2. Excel -> current Excel batch vs Excel Bronze records
#
# Checks:
#   - ROW_COUNT
#   - KEY_SET
#   - DUPLICATE_COUNT
#   - NULL_COUNT
#   - MIN_VALUE
#   - MAX_VALUE
#
# Bronze is always filtered by SourceSystem.
# Excel Bronze is additionally filtered by SourceBatchId.
# ============================================================

results = []


# ------------------------------------------------------------
# Helper: add one validation result
# ------------------------------------------------------------

def add_result(
    table_name,
    check_type,
    column_name,
    source_value,
    bronze_value,
    status,
    message,
    source_type
):
    results.append((
        layer,
        run_id,
        table_name,
        check_type,
        column_name,
        source_type,
        str(source_value) if source_value is not None else None,
        str(bronze_value) if bronze_value is not None else None,
        status,
        message,
        datetime.now()
    ))


# ============================================================
# FUNCTION TO VALIDATE ONE TABLE
# ============================================================

def validate_source_table(
    table_name,
    source_type,
    source_records,
    bronze_df,
    source_batch_id=None
):

    print("=" * 80)
    print(f"Validating: {table_name} | Source: {source_type}")

    # --------------------------------------------------------
    # 1. Filter Bronze to the correct source
    # --------------------------------------------------------

    bronze_source_df = get_bronze_source_df(
        bronze_df=bronze_df,
        source_type=source_type,
        source_batch_id=source_batch_id
    )

    # --------------------------------------------------------
    # 2. Basic counts
    # --------------------------------------------------------

    source_count = len(source_records)
    bronze_count = bronze_source_df.count()

    print(f"Source rows : {source_count}")
    print(f"Bronze rows : {bronze_count}")

    # --------------------------------------------------------
    # 3. Get primary key
    # --------------------------------------------------------

    key_columns = primary_keys.get(
        table_name,
        []
    )

    if not key_columns:
        print(
            f"WARNING: No primary key defined for {table_name}"
        )

        add_result(
            table_name=table_name,
            check_type="KEY_SET",
            column_name="*",
            source_value=None,
            bronze_value=None,
            status="ERROR",
            message=(
                f"No primary key definition found for "
                f"{table_name}."
            ),
            source_type=source_type
        )

    else:

        print(f"Primary key: {key_columns}")

        # ----------------------------------------------------
        # 4. Compare source and Bronze key sets
        # ----------------------------------------------------

        missing_in_bronze, extra_in_bronze = compare_key_sets(
            source_records=source_records,
            bronze_df=bronze_source_df,
            key_columns=key_columns
        )

        missing_count = len(missing_in_bronze)
        extra_count = len(extra_in_bronze)

        source_key_count = len(
            set(
                tuple(
                    normalize_source_value(
                        record.get(column)
                    )
                    for column in key_columns
                )
                for record in source_records
            )
        )

        bronze_key_rows = (
            bronze_source_df
            .select(*key_columns)
            .collect()
        )

        bronze_key_count = len(
            set(
                tuple(
                    normalize_source_value(
                        row[column]
                    )
                    for column in key_columns
                )
                for row in bronze_key_rows
            )
        )

        print(f"Source unique keys : {source_key_count}")
        print(f"Bronze unique keys : {bronze_key_count}")
        print(f"Missing in Bronze  : {missing_count}")
        print(f"Extra in Bronze    : {extra_count}")

        # ----------------------------------------------------
        # 5. ROW COUNT
        # ----------------------------------------------------
        #
        # Row count passes only when both the number of rows
        # and the key sets agree.
        #
        # This prevents a case such as:
        #
        # Source = 91
        # Bronze = 91
        #
        # from incorrectly passing when the records themselves
        # are different.
        # ----------------------------------------------------

        if (
            source_count == bronze_count
            and
            missing_count == 0
            and
            extra_count == 0
        ):

            row_count_status = "PASS"

            row_count_message = (
                f"{source_type} rows={source_count}; "
                f"Bronze rows={bronze_count}; "
                f"key sets match."
            )

        else:

            row_count_status = "FAIL"

            row_count_message = (
                f"{source_type} rows={source_count}; "
                f"Bronze rows={bronze_count}; "
                f"missing keys={missing_count}; "
                f"extra keys={extra_count}."
            )

        add_result(
            table_name=table_name,
            check_type="ROW_COUNT",
            column_name="*",
            source_value=source_count,
            bronze_value=bronze_count,
            status=row_count_status,
            message=row_count_message,
            source_type=source_type
        )

        # ----------------------------------------------------
        # 6. KEY SET
        # ----------------------------------------------------

        if (
            missing_count == 0
            and
            extra_count == 0
        ):

            key_status = "PASS"

        else:

            key_status = "FAIL"

        add_result(
            table_name=table_name,
            check_type="KEY_SET",
            column_name=",".join(key_columns),
            source_value=source_key_count,
            bronze_value=bronze_key_count,
            status=key_status,
            message=(
                f"Missing in Bronze: {missing_count}; "
                f"Extra in Bronze: {extra_count}."
            ),
            source_type=source_type
        )

        # ----------------------------------------------------
        # 7. Display missing / extra keys
        # ----------------------------------------------------

        if missing_count > 0:

            print("WARNING: Keys missing from Bronze:")

            for key in list(missing_in_bronze)[:20]:
                print("  ", key)

        if extra_count > 0:

            print("WARNING: Extra keys in Bronze:")

            for key in list(extra_in_bronze)[:20]:
                print("  ", key)

    # ========================================================
    # 8. DUPLICATE COUNT
    # ========================================================

    if key_columns:

        source_duplicate_groups = source_duplicate_count(
            source_records,
            key_columns
        )

        # --------------------------------------------
        # Bronze duplicate groups
        # --------------------------------------------

        bronze_duplicate_df = (
            bronze_source_df
            .filter(
                F.col(key_columns[0]).isNotNull()
            )
        )

        # For composite keys, require every key column
        # to be non-null.
        for column in key_columns[1:]:
            bronze_duplicate_df = bronze_duplicate_df.filter(
                F.col(column).isNotNull()
            )

        bronze_duplicate_groups = (
            bronze_duplicate_df
            .groupBy(*key_columns)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print(
            f"Source duplicate groups : "
            f"{source_duplicate_groups}"
        )

        print(
            f"Bronze duplicate groups : "
            f"{bronze_duplicate_groups}"
        )

        if (
            source_duplicate_groups
            ==
            bronze_duplicate_groups
        ):

            duplicate_status = "PASS"

        else:

            duplicate_status = "FAIL"

        add_result(
            table_name=table_name,
            check_type="DUPLICATE_COUNT",
            column_name=",".join(key_columns),
            source_value=source_duplicate_groups,
            bronze_value=bronze_duplicate_groups,
            status=duplicate_status,
            message=(
                f"{source_type} duplicate groups="
                f"{source_duplicate_groups}; "
                f"Bronze duplicate groups="
                f"{bronze_duplicate_groups}."
            ),
            source_type=source_type
        )

    # ========================================================
    # 9. COLUMN STATISTICS
    # ========================================================

    # Only compare columns that actually exist in both:
    #
    # Source columns
    #       AND
    # Bronze business columns
    #
    # This automatically excludes Bronze metadata columns such
    # as SourceSystem and SourceBatchId.
    # ========================================================

    source_columns = set()

    for record in source_records:
        source_columns.update(record.keys())

    bronze_columns = set(
        bronze_source_df.columns
    )

    common_columns = [
        column
        for column in source_columns
        if column in bronze_columns
    ]

    # --------------------------------------------------------
    # Get source statistics
    # --------------------------------------------------------

    try:

        source_stats = source_column_stats(
            records=source_records,
            columns=common_columns,
            bronze_df=bronze_source_df
        )

    except Exception as e:

        print(
            f"ERROR calculating source statistics "
            f"for {table_name}: {e}"
        )

        source_stats = {}

    # ========================================================
    # 10. NULL COUNT + MIN/MAX
    # ========================================================

    for column in common_columns:

        # ----------------------------------------------------
        # Source statistics
        # ----------------------------------------------------

        if column not in source_stats:
            continue

        source_null_count = source_stats[column][
            "null_count"
        ]

        source_min_value = source_stats[column][
            "min_value"
        ]

        source_max_value = source_stats[column][
            "max_value"
        ]

        # ----------------------------------------------------
        # Bronze NULL count
        # ----------------------------------------------------

        bronze_null_count = (
            bronze_source_df
            .filter(
                F.col(column).isNull()
            )
            .count()
        )

        # ----------------------------------------------------
        # NULL COUNT validation
        # ----------------------------------------------------

        if (
            source_null_count
            ==
            bronze_null_count
        ):

            null_status = "PASS"

        else:

            null_status = "FAIL"

        add_result(
            table_name=table_name,
            check_type="NULL_COUNT",
            column_name=column,
            source_value=source_null_count,
            bronze_value=bronze_null_count,
            status=null_status,
            message=(
                f"{source_type} NULLs="
                f"{source_null_count}; "
                f"Bronze NULLs="
                f"{bronze_null_count}."
            ),
            source_type=source_type
        )

        # ----------------------------------------------------
        # Bronze min/max
        #
        # Only compare when the source statistics function
        # actually calculated them.
        # ----------------------------------------------------

        if (
            source_min_value is not None
            or
            source_max_value is not None
        ):

            bronze_stats = (
                bronze_source_df
                .select(
                    F.min(
                        F.col(column)
                    ).alias("min_value"),

                    F.max(
                        F.col(column)
                    ).alias("max_value")
                )
                .first()
            )

            bronze_min_value = bronze_stats[
                "min_value"
            ]

            bronze_max_value = bronze_stats[
                "max_value"
            ]

            # ------------------------------------------------
            # MIN VALUE
            # ------------------------------------------------

            normalized_source_min = (
                normalize_source_value(
                    source_min_value
                )
            )

            normalized_bronze_min = (
                normalize_source_value(
                    bronze_min_value
                )
            )

            if (
                normalized_source_min
                ==
                normalized_bronze_min
            ):

                min_status = "PASS"

            else:

                min_status = "FAIL"

            add_result(
                table_name=table_name,
                check_type="MIN_VALUE",
                column_name=column,
                source_value=source_min_value,
                bronze_value=bronze_min_value,
                status=min_status,
                message=(
                    f"{source_type} min="
                    f"{source_min_value}; "
                    f"Bronze min="
                    f"{bronze_min_value}."
                ),
                source_type=source_type
            )

            # ------------------------------------------------
            # MAX VALUE
            # ------------------------------------------------

            normalized_source_max = (
                normalize_source_value(
                    source_max_value
                )
            )

            normalized_bronze_max = (
                normalize_source_value(
                    bronze_max_value
                )
            )

            if (
                normalized_source_max
                ==
                normalized_bronze_max
            ):

                max_status = "PASS"

            else:

                max_status = "FAIL"

            add_result(
                table_name=table_name,
                check_type="MAX_VALUE",
                column_name=column,
                source_value=source_max_value,
                bronze_value=bronze_max_value,
                status=max_status,
                message=(
                    f"{source_type} max="
                    f"{source_max_value}; "
                    f"Bronze max="
                    f"{bronze_max_value}."
                ),
                source_type=source_type
            )

    print(
        f"Finished validation: "
        f"{table_name} | {source_type}"
    )


# ============================================================
# PART A — API SOURCE VALIDATION
# ============================================================

print("\n")
print("#" * 80)
print("# API SOURCE COMPLETENESS VALIDATION")
print("#" * 80)


for row in control_rows:

    # --------------------------------------------------------
    # Read metadata from ingestion_control
    # --------------------------------------------------------

    table_name = row["BronzeTable"]
    relative_url = row["RelativeURL"]

    # Make sure we are working with the Bronze table name.
    #
    # Your ingestion_control already contains values such as:
    #
    # Bronze_Customers
    # Bronze_Orders
    #
    # Therefore we do NOT add another "Bronze_" prefix.
    # --------------------------------------------------------

    bronze_table_name = table_name

    source_type = "API"

    print("\n")
    print("-" * 80)
    print(
        f"Loading API source for {bronze_table_name}"
    )

    try:

        # ----------------------------------------------------
        # 1. Get actual API records
        # ----------------------------------------------------

        source_records = get_source_records(
            relative_url
        )

        # ----------------------------------------------------
        # 2. Load actual Bronze table
        # ----------------------------------------------------

        bronze_df = spark.table(
            bronze_table_name
        )

        # ----------------------------------------------------
        # 3. Convert Bronze table name to logical table name
        #
        # Example:
        # Bronze_Customers -> Customers
        # Bronze_Order_Details -> Order_Details
        # ----------------------------------------------------

        logical_table_name = bronze_table_name

        if logical_table_name.startswith("Bronze_"):

            logical_table_name = (
                logical_table_name[
                    len("Bronze_"):]
                )

        # ----------------------------------------------------
        # 4. Validate
        # ----------------------------------------------------

        validate_source_table(
            table_name=logical_table_name,
            source_type=source_type,
            source_records=source_records,
            bronze_df=bronze_df,
            source_batch_id=None
        )

    except Exception as e:

        print(
            f"ERROR validating "
            f"{bronze_table_name} | API: {e}"
        )

        logical_table_name = bronze_table_name

        if logical_table_name.startswith("Bronze_"):

            logical_table_name = (
                logical_table_name[
                    len("Bronze_"):]
                )

        add_result(
            table_name=logical_table_name,
            check_type="SOURCE_VALIDATION",
            column_name="*",
            source_value=None,
            bronze_value=None,
            status="ERROR",
            message=str(e),
            source_type="API"
        )


# ============================================================
# PART B — EXCEL SOURCE VALIDATION
# ============================================================

print("\n")
print("#" * 80)
print("# EXCEL SOURCE COMPLETENESS VALIDATION")
print("#" * 80)


for sheet_name, table_name in excel_sheet_map.items():

    source_type = "Excel"

    bronze_table_name = (
        f"Bronze_{table_name}"
    )

    print("\n")
    print("-" * 80)
    print(
        f"Loading Excel source: "
        f"{sheet_name} -> {bronze_table_name}"
    )

    try:

        # ----------------------------------------------------
        # 1. Get actual Excel records
        # ----------------------------------------------------

        source_records = get_excel_source_records(
            table_name
        )

        # ----------------------------------------------------
        # 2. Load actual Bronze table
        # ----------------------------------------------------

        bronze_df = spark.table(
            bronze_table_name
        )

        # ----------------------------------------------------
        # 3. Validate ONLY the current Excel batch
        # ----------------------------------------------------

        validate_source_table(
            table_name=table_name,
            source_type=source_type,
            source_records=source_records,
            bronze_df=bronze_df,
            source_batch_id=excel_batch_id
        )

    except Exception as e:

        print(
            f"ERROR validating "
            f"{table_name} | Excel: {e}"
        )

        add_result(
            table_name=table_name,
            check_type="SOURCE_VALIDATION",
            column_name="*",
            source_value=None,
            bronze_value=None,
            status="ERROR",
            message=str(e),
            source_type="Excel"
        )


# ============================================================
# PART C — CREATE RESULTS DATAFRAME
# ============================================================

result_schema = [
    "layer",
    "run_id",
    "table_name",
    "check_type",
    "column_name",
    "source_type",
    "source_value",
    "bronze_value",
    "status",
    "message",
    "validated_at"
]


results_df = spark.createDataFrame(
    results,
    result_schema
)


# ============================================================
# SAVE RESULTS
# ============================================================

(
    results_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "bronze_completeness_results"
    )
)


# ============================================================
# DISPLAY RESULTS
# ============================================================

display(
    results_df.orderBy(
        "table_name",
        "source_type",
        "check_type",
        "column_name"
    )
)


# ============================================================
# SUMMARY
# ============================================================

failed = (
    results_df
    .filter(
        F.col("status").isin(
            "FAIL",
            "ERROR"
        )
    )
    .count()
)

passed = (
    results_df
    .filter(
        F.col("status") == "PASS"
    )
    .count()
)

total = results_df.count()

print("=" * 80)
print("BRONZE SOURCE COMPLETENESS VALIDATION SUMMARY")
print("=" * 80)
print(f"Total checks       : {total}")
print(f"Passed checks      : {passed}")
print(f"Failed/Error checks: {failed}")
print(f"Run ID             : {run_id}")
print("=" * 80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
