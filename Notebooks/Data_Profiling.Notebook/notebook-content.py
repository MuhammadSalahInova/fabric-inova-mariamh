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
from pyspark.sql.types import (
    StringType,
    BooleanType,
    NumericType,
    DateType,
    TimestampType
)
from datetime import datetime
import json

tables = [
    "Categories",
    "Customers",
    "Employees",
    "Order_Details",
    "Orders",
    "Products",
    "Regions",
    "Shippers",
    "Suppliers",
    "Territories"
]

def get_table_name(table_name, layer):
    return f"{layer}_{table_name}"

# ============================================================
# Reusable data-quality rules
# ============================================================

NUMERIC_RULES = {
    "Order_Details": {
        "Quantity": {
            "rule": "> 0",
            "description": "Quantity must not be negative."
        },
        "UnitPrice": {
            "rule": ">= 0",
            "description": "UnitPrice must not be negative."
        },
        "Discount": {
            "rule": "between_0_1",
            "description": "Discount must be between 0 and 1."
        }
    },

    "Orders": {
        "Freight": {
            "rule": ">= 0",
            "description": "Freight must not be negative."
        }
    },

    "Products": {
        "UnitPrice": {
            "rule": ">= 0",
            "description": "UnitPrice must not be negative."
        },
        "UnitsInStock": {
            "rule": ">= 0",
            "description": "UnitsInStock must not be negative."
        },
        "UnitsOnOrder": {
            "rule": ">= 0",
            "description": "UnitsOnOrder must not be negative."
        },
        "ReorderLevel": {
            "rule": ">= 0",
            "description": "ReorderLevel must not be negative."
        }
    }
}

DATE_RULES = {
    "Orders": [
        ("OrderDate", "RequiredDate"),
        ("OrderDate", "ShippedDate")
    ]
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_numeric_rules(df, table_name):

    results = []

    rules = NUMERIC_RULES.get(table_name, {})

    for column_name, rule_info in rules.items():

        if column_name not in df.columns:
            continue

        rule = rule_info["rule"]

        if rule == ">= 0":

            invalid_count = (
                df.filter(
                    F.col(column_name).isNotNull() &
                    (F.col(column_name) < 0)
                )
                .count()
            )

        elif rule == "between_0_1":

            invalid_count = (
                df.filter(
                    F.col(column_name).isNotNull() &
                    (
                        (F.col(column_name) < 0) |
                        (F.col(column_name) > 1)
                    )
                )
                .count()
            )

        else:
            continue

        results.append({
            "table_name": table_name,
            "column_name": column_name,
            "rule": rule,
            "invalid_count": invalid_count,
            "status": "PASS" if invalid_count == 0 else "FAIL",
            "message": rule_info["description"]
        })

    return results


def validate_date_rules(df, table_name):

    results = []

    rules = DATE_RULES.get(table_name, [])

    for first_column, second_column in rules:

        if (
            first_column not in df.columns or
            second_column not in df.columns
        ):
            continue

        condition = (
            F.col(first_column).isNotNull() &
            F.col(second_column).isNotNull() &
            (F.col(first_column) > F.col(second_column))
        )

        invalid_count = df.filter(condition).count()

        results.append({
            "table_name": table_name,
            "column_name": f"{first_column} <= {second_column}",
            "rule": "DATE_ORDER",
            "invalid_count": invalid_count,
            "status": "PASS" if invalid_count == 0 else "FAIL",
            "message": (
                f"{first_column} must be earlier than or equal to "
                f"{second_column}."
            )
        })

    return results

def profile_table(table_name, layer, run_id):

    full_table_name = get_table_name(table_name, layer)

    df = spark.table(full_table_name)

    row_count = df.count()

    results = []

    for field in df.schema.fields:

        column_name = field.name
        data_type = field.dataType.simpleString()

        # -------------------------
        # General statistics
        # -------------------------

        distinct_count = (
            df.select(column_name)
              .distinct()
              .count()
        )

        distinct_percentage = (
            (distinct_count / row_count) * 100
            if row_count > 0 else 0
        )

        null_count = (
            df.filter(F.col(column_name).isNull())
              .count()
        )

        null_percentage = (
            (null_count / row_count) * 100
            if row_count > 0 else 0
        )

        # -------------------------
        # Min / Max
        # -------------------------

        min_value = None
        max_value = None

        if isinstance(
            field.dataType,
            (NumericType, DateType, TimestampType)
        ):

            stats = df.select(
                F.min(F.col(column_name)).alias("min_value"),
                F.max(F.col(column_name)).alias("max_value")
            ).first()

            min_value = (
                str(stats["min_value"])
                if stats["min_value"] is not None
                else None
            )

            max_value = (
                str(stats["max_value"])
                if stats["max_value"] is not None
                else None
            )

        # -------------------------
        # Categorical profiling
        # -------------------------

        top_values = None

        if isinstance(
            field.dataType,
            (StringType, BooleanType)
        ):

            category_counts = (
                df.filter(F.col(column_name).isNotNull())
                  .groupBy(column_name)
                  .count()
                  .orderBy(F.desc("count"))
                  .limit(10)
                  .collect()
            )

            category_list = []

            for row in category_counts:

                value = row[column_name]
                count = row["count"]

                percentage = (
                    (count / row_count) * 100
                    if row_count > 0 else 0
                )

                category_list.append({
                    "value": str(value),
                    "count": int(count),
                    "percentage": round(percentage, 2)
                })

            top_values = json.dumps(
                category_list,
                ensure_ascii=False
            )

        # -------------------------
        # Store result
        # -------------------------

        results.append(
            (
                layer,
                run_id,
                table_name,
                column_name,
                data_type,
                int(distinct_count),
                float(distinct_percentage),
                min_value,
                max_value,
                int(null_count),
                float(null_percentage),
                int(row_count),
                top_values,
                datetime.now()
            )
        )

    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_results = []
all_rule_results = []

for table_name in tables:

    print(f"Profiling {layer}_{table_name}...")

    table_results = profile_table(
        table_name,
        layer,
        run_id
    )

    all_results.extend(table_results)

    df = spark.table(
        get_table_name(table_name, layer)
    )

    # Numerical/business rules
    numeric_results = validate_numeric_rules(
        df,
        table_name
    )

    all_rule_results.extend(
        numeric_results
    )

    # Date/business rules
    date_results = validate_date_rules(
        df,
        table_name
    )

    all_rule_results.extend(
        date_results
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

profiling_df = spark.createDataFrame(
    all_results,
    [
        "layer",
        "run_id",
        "table_name",
        "column_name",
        "data_type",
        "distinct_count",
        "distinct_percentage",
        "min_value",
        "max_value",
        "null_count",
        "null_percentage",
        "row_count",
        "top_values",
        "profiled_at"
    ]
)

profiling_df = (
    profiling_df
    .withColumn("profiled_at", F.current_timestamp())
)

profiling_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("data_profiling_results")

display(
    spark.table("data_profiling_results")
         .orderBy("layer", "table_name", "column_name")
)

# ============================================================
# Save business-rule results separately
# ============================================================

if all_rule_results:

    rules_df = spark.createDataFrame(
        all_rule_results
    )

    rules_df = (
        rules_df
        .withColumn("layer", F.lit(layer))
        .withColumn("run_id", F.lit(run_id))
        .withColumn(
            "checked_at",
            F.current_timestamp()
        )
    )

    (
        rules_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(
            "profiling_rule_results"
        )
    )

    display(rules_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
