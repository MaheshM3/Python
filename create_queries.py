import pandas as pd
from pyspark.sql import SparkSession

def create_queries_for_ruleitems_df_cp005(ruleitems_df_cp005, spark):
    # Step 1: Convert Spark DataFrame to Pandas DataFrame
    ruleitems_df_cp005_pd = ruleitems_df_cp005.toPandas()

    # Step 2: Helper function for conditional replacement
    def replace_conditionally(row, col_name, placeholder):
        """
        Replace placeholder in 'other_template_query' if col_name is not null in the row.
        """
        if pd.notnull(row[col_name]):
            return row['other_template_query'].replace(f'<{placeholder}>', str(row[col_name]))
        return row['other_template_query']
    
    # List of columns to be checked along with their placeholder names
    replacements = [
        ('JOIN_CONDITION', 'JOIN_CONDITION'),
        ('RULE_EXPRESSION', 'RULE_EXPRESSION'),
        ('LIST_OF_VALUES', 'LIST_OF_VALUES'),
        ('RANGE_EXPRESSION', 'RANGE_EXPRESSION'),
        ('DIMENSION_KEY', 'DIMENSION_KEY'),
        ('BUSINESS_GROUP_LOCATION', 'BUSINESS_GROUP_LOCATION'),
        ('DATA_FILTER_EXPRESSION', 'DATA_FILTER_EXPRESSION'),
        ('DQ_COUNT_FILTER_EXPRESSION1', 'DQ_COUNT_FILTER_EXPRESSION1'),
        ('FILTER_OUT_CONDITION1', 'FILTER_OUT_CONDITION1')
    ]
    
    # Step 3: Apply conditional replacements for each column
    for col_name, placeholder in replacements:
        ruleitems_df_cp005_pd['other_template_query'] = ruleitems_df_cp005_pd.apply(
            lambda row: replace_conditionally(row, col_name, placeholder), axis=1
        )
    
    # Step 4: Static replacements for placeholders without conditions
    ruleitems_df_cp005_pd['other_template_query'] = ruleitems_df_cp005_pd['other_template_query'].str.replace(
        '<BUSINESS_DATE_FILTER>', 'BUSINESS_DATE = :', regex=False
    ).str.replace(
        '<PARALLEL_FILTER>', 'PARALLEL_ID = :', regex=False
    ).str.replace(
        '<CURR_BUSINESS_DATE_FILTER>', 'BUSINESS_DATE = :', regex=False
    ).str.replace(
        '<TABLE_NAME>', 'TABLE_NAME', regex=False
    ).str.replace(
        '<ATTRIBUTE_NAME>', 'ATTRIBUTE_NAME', regex=False
    )

    # Step 5: Handle previous date expression by concatenating
    prev_date_exp = "business_date = prev_date"
    ruleitems_df_cp005_pd['other_template_query'] = ruleitems_df_cp005_pd['other_template_query'].str.replace(
        '<PREVIOUS_DATE_FILTER>', prev_date_exp, regex=False
    )

    # Optional: Export to CSV if needed
    # output_filename = f"Output_ruleitems_cp005_df_{str(self.rule_executor_manager_params.business_date)}_{str(self.rule_executor_manager_params.business_group_location)}.csv"
    # output_path = f"/mnt/export/{output_filename}"
    # ruleitems_df_cp005_pd.to_csv(output_path, index=False)

    # Step 6: Convert the Pandas DataFrame back to a Spark DataFrame
    ruleitems_df_cp005_spark = spark.createDataFrame(ruleitems_df_cp005_pd)

    return ruleitems_df_cp005_spark
