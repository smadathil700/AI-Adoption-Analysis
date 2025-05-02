import snowflake.snowpark.functions as F

#store industries seed data to a table
def model(dbt,session):

    #storing raw data to a dataframe
    industries_df = dbt.ref("raw_industries")

    #renaming of columns
    renamed_industries_df = industries_df.rename({'IndustryCode':'industry_code','Industry':'industry_name'})

    #return dataframe
    return renamed_industries_df