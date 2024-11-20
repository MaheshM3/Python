# Simplify Repeated withColumn Calls
 ### Currently, there are many consecutive withColumn calls, each transforming ruleitems_df_cp005. To make it more readable and efficient, we can define a reusable function to handle these transformations.

# Consolidate Repeated Logic
 ### Several expressions are repeated with slight variations, such as the checks for column nullability and replacements in other_template_query. Consolidating similar expressions in helper functions or a dictionary structure could make the code more modular and maintainable.


   ```Python
  import pandas as pd

def create_queries_for_ruleitems_df_cp005(ruleitems_df_cp005):
    """
    Updates 'other_template_query' column in the DataFrame by replacing placeholders
    with specific values based on column values or predefined expressions.

    Args:
        ruleitems_df_cp005: Input Pandas DataFrame with rule items.

    Returns:
        Updated Pandas DataFrame.
    """

    # Helper function to replace placeholder if the column `col_name` is not null.
    def replace_placeholder(df, col_name, placeholder):
        for index, row in df.iterrows():
            if pd.notnull(row[col_name]):
                df.at[index, 'other_template_query'] = row['other_template_query'].replace(
                    f"<{placeholder}>", str(row[col_name])
                )
        return df

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
        ruleitems_df_cp005['other_template_query'] = ruleitems_df_cp005['other_template_query'].str.replace(
            placeholder, replacement, regex=False
        )

    return ruleitems_df_cp005

# Example usage with a sample DataFrame
data = {
    'other_template_query': ['<JOIN_CONDITION> is applied', '<RULE_EXPRESSION> check', '<BUSINESS_DATE_FILTER>'],
    'JOIN_CONDITION': ['join_condition_value', None, None],
    'RULE_EXPRESSION': [None, 'rule_expression_value', None],
}
ruleitems_df_cp005 = pd.DataFrame(data)

# Call the function
updated_df = create_queries_for_ruleitems_df_cp005(ruleitems_df_cp005)
print(updated_df)


```
#Refactored code
### Separated Logic for Placeholder Replacement
### Used Dictionaries for Mappings
### Consolidated Static Replacements
### Removed Hardcoded Logic for PREVIOUS_DATE_FILTER

