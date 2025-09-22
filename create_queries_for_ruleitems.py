import pandas as pd

def create_queries_for_ruleitems(self, ruleitems_df, column_name, prev_date):  # noqa: C901, PLR0912
    if ruleitems_df.empty:
        ruleitems_df["total_count_query"] = None
        ruleitems_df["dq_count_query"] = None
        return ruleitems_df

    column_mapping = {
        "TOTAL_COUNT_TEMPLATE": "total_count_query",
        "DQ_COUNT_TEMPLATE": "dq_count_query",
        "OTHER_TEMPLATE": "other_template_query",
    }

    if column_name in column_mapping:
        ruleitems_df[column_mapping[column_name]] = ruleitems_df[column_name]

    # Replacement mapping
    column_replacements = {
        "<JOIN_CONDITION>": "JOIN_CONDITION",
        "<LIST_OF_VALUE>": "RULE_EXPRESSION",
        "<RANGE_EXPRESSION>": "RULE_EXPRESSION",
        "<DIMENSION_KEY>": "DIMENSION_KEY",
        "<TABLE>": "ITEM_NAME",
        "<COLUMN>": "ATTRIBUTE_NAME",

        # ✅ Dynamic replacement: takes column name from self._si.location
        "<AND BASE.BUSINESS_GROUP_LOCATION_FILTER>": (
            self._si.location,
            f"AND BASE.{self._si.location} = '{{}}' "
        ),

        "<AND DATA_FILTER_EXPRESSION>": ("DATA_FILTER_EXPRESSION", "AND {}"),
        "<AND DQ_COUNT_FILTER_EXPRESSIONS>": ("DQ_COUNT_FILTER_EXPRESSION", "AND {}"),
        "<AND NOT FEEDBACK_LOOP_FILTER_EXPRESSION>": ("FILTER_OUT_CONDITION", "AND NOT ({} ) "),
    }

    # Apply replacements
    for key, value in column_replacements.items():
        if isinstance(value, tuple):
            column, template = value
            ruleitems_df[column_mapping[column_name]] = ruleitems_df.apply(
                lambda row: (
                    row[column_mapping[column_name]].replace(
                        key, template.format(row[column])
                    )
                    if pd.notnull(row.get(column)) and row[column] != ""
                    else row[column_mapping[column_name]].replace(key, "")
                ),
                axis=1,
            )
        else:
            ruleitems_df[column_mapping[column_name]] = ruleitems_df.apply(
                lambda row: (
                    row[column_mapping[column_name]].replace(key, row[value])
                    if pd.notnull(row.get(value)) and row[value] != ""
                    else row[column_mapping[column_name]].replace(key, "")
                ),
                axis=1,
            )

    return ruleitems_df
