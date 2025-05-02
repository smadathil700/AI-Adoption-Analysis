import snowflake.snowpark.functions as F

#store industries seed data to a table
def model(dbt,session):
    #stores seed raw data in dataframe
    data_df = dbt.ref("raw_ai_adoption_data")

    #renaming of columns
    renamed_data_df = data_df.rename({'Industry_Code':'industry_code','Total_AI_Investment_USD_MM':'total_ai_investment_usd_mm',\
                                'AI_Adoption_Rate':'ai_adoption_rate','AI_Investment_USD_MM':'ai_investment_usd_mm','Company':'company'})
    #return
    return renamed_data_df
