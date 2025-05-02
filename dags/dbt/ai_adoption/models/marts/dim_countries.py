import snowflake.snowpark.functions as F

#store countries seed data to a table
def model(dbt,session):

    #storing data to a dataframe
    countries_df = dbt.ref("raw_countries")

    #renaming of columns
    renamed_countries_df = countries_df.rename({'CountryCode':'country_code','Country':'country_name'})

    #return dataframe
    return renamed_countries_df