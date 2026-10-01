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

import uuid

# If run_id was not supplied by the pipeline,
# generate one for manual notebook execution.
if not run_id:
    run_id = str(uuid.uuid4())

print("Layer:", layer)
print("Run ID:", run_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from datetime import datetime

control_df = (
    spark.table("validation_control")
    .filter(F.col("IsActive") == 1)
)

rules = control_df.collect()


def get_table(table_name, layer):
    return spark.table(f"{layer}_{table_name}")

results = []

def add_result(
    table_name,
    validation_type,
    column_name,
    referenced_table,
    referenced_column,
    status,
    failure_count,
    row_count,
    message
):

    results.append(
        (
            layer,
            table_name,
            validation_type,
            column_name,
            referenced_table,
            referenced_column,
            status,
            int(failure_count),
            int(row_count),
            message,
            datetime.now(),
            run_id
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_primary_key(table_name, column_name, layer):

    df = get_table(table_name, layer)

    row_count = df.count()

    null_count = (
        df.filter(F.col(column_name).isNull())
          .count()
    )

    duplicate_count = (
        df.groupBy(column_name)
          .count()
          .filter(F.col("count") > 1)
          .count()
    )

    failure_count = null_count + duplicate_count

    if failure_count == 0:

        status = "PASS"

        message = "Primary key is valid."

    else:

        status = "FAIL"

        message = (
            f"Primary key has {null_count} NULL values "
            f"and {duplicate_count} duplicate values."
        )

    add_result(
        table_name,
        "PRIMARY_KEY",
        column_name,
        None,
        None,
        status,
        failure_count,
        row_count,
        message
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_composite_primary_key(
    table_name,
    column_name,
    layer
):

    columns = [
        c.strip()
        for c in column_name.split(",")
    ]

    df = get_table(table_name, layer)

    row_count = df.count()

    null_condition = None

    for column in columns:

        condition = F.col(column).isNull()

        if null_condition is None:
            null_condition = condition
        else:
            null_condition = null_condition | condition

    null_count = (
        df.filter(null_condition)
          .count()
    )

    duplicate_count = (
        df.groupBy(*columns)
          .count()
          .filter(F.col("count") > 1)
          .count()
    )

    failure_count = null_count + duplicate_count

    if failure_count == 0:

        status = "PASS"

        message = "Composite primary key is valid."

    else:

        status = "FAIL"

        message = (
            f"Composite key has {null_count} NULL rows "
            f"and {duplicate_count} duplicate combinations."
        )

    add_result(
        table_name,
        "PRIMARY_KEY_COMPOSITE",
        column_name,
        None,
        None,
        status,
        failure_count,
        row_count,
        message
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_nulls(
    table_name,
    column_name,
    layer
):

    df = get_table(table_name, layer)

    row_count = df.count()

    null_count = (
        df.filter(F.col(column_name).isNull())
          .count()
    )

    if null_count == 0:

        status = "PASS"
        message = "No NULL values found."

    else:

        status = "FAIL"
        message = f"{null_count} NULL values found."

    add_result(
        table_name,
        "NULL_CHECK",
        column_name,
        None,
        None,
        status,
        null_count,
        row_count,
        message
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_duplicates(
    table_name,
    column_name,
    layer
):

    df = get_table(table_name, layer)

    row_count = df.count()

    duplicate_count = (
        df.groupBy(column_name)
          .count()
          .filter(F.col("count") > 1)
          .count()
    )

    if duplicate_count == 0:

        status = "PASS"
        message = "No duplicate values found."

    else:

        status = "FAIL"
        message = (
            f"{duplicate_count} duplicate values found."
        )

    add_result(
        table_name,
        "DUPLICATE_CHECK",
        column_name,
        None,
        None,
        status,
        duplicate_count,
        row_count,
        message
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_foreign_key(
    table_name,
    column_name,
    referenced_table,
    referenced_column,
    layer
):

    child_df = (
        get_table(table_name, layer)
        .alias("child")
    )

    parent_df = (
        get_table(referenced_table, layer)
        .select(
            F.col(referenced_column).alias("_parent_key")
        )
        .where(
            F.col(referenced_column).isNotNull()
        )
        .distinct()
        .alias("parent")
    )

    row_count = child_df.count()

    invalid_count = (
        child_df
        .where(
            F.col(f"child.{column_name}").isNotNull()
        )
        .join(
            parent_df,
            F.col(f"child.{column_name}") ==
            F.col("parent._parent_key"),
            "left_anti"
        )
        .count()
    )

    if invalid_count == 0:

        status = "PASS"

        message = (
            "All foreign-key values exist "
            "in the referenced table."
        )

    else:

        status = "FAIL"

        message = (
            f"{invalid_count} foreign-key values "
            "do not exist in the referenced table."
        )

    add_result(
        table_name,
        "FOREIGN_KEY",
        column_name,
        referenced_table,
        referenced_column,
        status,
        invalid_count,
        row_count,
        message
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_row_count(table_name, layer):

    df = get_table(table_name, layer)

    row_count = df.count()

    add_result(
        table_name,
        "ROW_COUNT",
        "*",
        None,
        None,
        "PASS",
        0,
        row_count,
        f"Table contains {row_count} rows."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for rule in rules:

    table_name = rule["TableName"]
    validation_type = rule["ValidationType"]
    column_name = rule["ColumnName"]

    referenced_table = rule["ReferencedTable"]
    referenced_column = rule["ReferencedColumn"]

    print(
        f"Validating {layer}_{table_name} "
        f"- {validation_type}"
    )

    try:

        if validation_type == "PRIMARY_KEY":

            validate_primary_key(
                table_name,
                column_name,
                layer
            )

        elif validation_type == "PRIMARY_KEY_COMPOSITE":

            validate_composite_primary_key(
                table_name,
                column_name,
                layer
            )

        elif validation_type == "NULL_CHECK":

            validate_nulls(
                table_name,
                column_name,
                layer
            )

        elif validation_type == "DUPLICATE_CHECK":

            validate_duplicates(
                table_name,
                column_name,
                layer
            )

        elif validation_type == "FOREIGN_KEY":

            validate_foreign_key(
                table_name,
                column_name,
                referenced_table,
                referenced_column,
                layer
            )

        elif validation_type == "ROW_COUNT":

            validate_row_count(
                table_name,
                layer
            )

    except Exception as e:

        add_result(
            table_name,
            validation_type,
            column_name,
            referenced_table,
            referenced_column,
            "ERROR",
            1,
            0,
            str(e)
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validation_df = spark.createDataFrame(
    results,
    [
        "layer",
        "table_name",
        "validation_type",
        "column_name",
        "referenced_table",
        "referenced_column",
        "status",
        "failure_count",
        "row_count",
        "message",
        "validation_time",
        "run_id"
    ]
)

validation_df = (
    validation_df
    .withColumn("validated_at", F.current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validation_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("validation_results")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.table("validation_results")
         .orderBy("layer", "table_name", "column_name")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
