import snowflake.snowpark.functions as F

def model(dbt,session):

   """
   data + CAGR + roi
   """

   df = dbt.ref("stg_ai_adoption_data")

   cagr_df = dbt.ref("inter_cagr").select("country_code","cagr_percent","global_avg")

   roi_df = dbt.ref("inter_roi").select('"country_code"','"company"','"year"','"roi"')

   df_cagr_joined = df.join(
       cagr_df, 
       df["country_code"] == cagr_df["country_code"]
       ).select(
              df["country_code"].alias("country_code"),
              df["year"],
              df["total_ai_investment_usd_mm"],
              df["gdp_usd_mm"],
              df["ai_adoption_rate"],
              df["company"].alias("company"),
              df["ai_investment_usd_mm"],
              df["return_from_ai_usd_mm"],
              df["industry_code"],
              cagr_df["cagr_percent"].alias("country_cagr_percent"),
              cagr_df["global_avg"]
       )
 
   result_df = df_cagr_joined.join(
              roi_df,
              (df_cagr_joined["country_code"] == roi_df['"country_code"']) &
              (df_cagr_joined["company"] == roi_df['"company"']) & 
              (df_cagr_joined["year"] == roi_df['"year"'])
       ).select(
       df_cagr_joined["country_code"].alias("country_code"),
       df_cagr_joined["year"].alias("year"),
       df_cagr_joined["total_ai_investment_usd_mm"], 
       df_cagr_joined["gdp_usd_mm"],
       df_cagr_joined["ai_adoption_rate"],
       df_cagr_joined["company"].alias("company"),
       df_cagr_joined["ai_investment_usd_mm"],
       df_cagr_joined["return_from_ai_usd_mm"],
       df_cagr_joined["industry_code"],
       df_cagr_joined["country_cagr_percent"],
       df_cagr_joined["global_avg"],
       roi_df['"roi"'].alias("ROI")
       )

   return result_df
