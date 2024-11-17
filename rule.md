# Simplify Repeated withColumn Calls
 ### Currently, there are many consecutive withColumn calls, each transforming ruleitems_df_cp005. To make it more readable and efficient, we can define a reusable function to handle these transformations.

# Consolidate Repeated Logic
 ### Several expressions are repeated with slight variations, such as the checks for column nullability and replacements in other_template_query. Consolidating similar expressions in helper functions or a dictionary structure could make the code more modular and maintainable.


   ```Python
  from pyspark.sql import functions as F

def create_queries_for_ruleitems_df_cp005(ruleitems_df_cp005):
    """
    Updates 'other_template_query' column in the DataFrame by replacing placeholders
    with specific values based on column values or predefined expressions.
    
    Args:
        ruleitems_df_cp005: Input PySpark DataFrame with rule items.
    
    Returns:
        Updated PySpark DataFrame.
    """
    def replace_placeholder(df, col_name, placeholder):
        """
        Helper function to replace placeholder in 'other_template_query'
        if the column `col_name` is not null.
        """
        return df.withColumn(
            'other_template_query',
            F.when(F.col(col_name).isNotNull(),
                   F.expr(f"replace(other_template_query, '<{placeholder}>', {col_name})"))
            .otherwise(F.col('other_template_query'))
        )

    # Define a dictionary for column-to-placeholder mapping
    column_replacements = {
        'JOIN_CONDITION': 'JOIN_CONDITION',
        'RULE_EXPRESSION': 'RULE_EXPRESSION',
        'LIST_OF_VALUES': 'LIST_OF_VALUES',
        'RANGE_EXPRESSION': 'RANGE_EXPRESSION',
        'DIMENSION_KEY': 'DIMENSION_KEY',
        'BUSINESS_GROUP_LOCATION': 'BUSINESS_GROUP_LOCATION',
        'DATA_FILTER_EXPRESSION': 'DATA_FILTER_EXPRESSION',
        'DQ_COUNT_FILTER_EXPRESSION1': 'DQ_COUNT_FILTER_EXPRESSION1',
        'FILTER_OUT_CONDITION1': 'FILTER_OUT_CONDITION1'
    }

    # Apply placeholder replacements iteratively
    for col_name, placeholder in column_replacements.items():
        ruleitems_df_cp005 = replace_placeholder(ruleitems_df_cp005, col_name, placeholder)

    # Define static replacements for placeholders
    static_replacements = {
        '<BUSINESS_DATE_FILTER>': "BUSINESS_DATE = :",
        '<PARALLEL_FILTER>': "PARALLEL_ID = :",
        '<CURR_BUSINESS_DATE_FILTER>': "BUSINESS_DATE = :",
        '<TABLE_NAME>': "TABLE_NAME",
        '<ATTRIBUTE_NAME>': "ATTRIBUTE_NAME",
        '<PREVIOUS_DATE_FILTER>': "business_date = prev_date"
    }

    # Apply static replacements
    for placeholder, replacement in static_replacements.items():
        ruleitems_df_cp005 = ruleitems_df_cp005.withColumn(
            'other_template_query',
            F.expr(f"replace(other_template_query, '{placeholder}', '{replacement}')")
        )

    return ruleitems_df_cp005

```
#Refactored code
### Separated Logic for Placeholder Replacement
### Used Dictionaries for Mappings
### Consolidated Static Replacements
### Removed Hardcoded Logic for PREVIOUS_DATE_FILTER

