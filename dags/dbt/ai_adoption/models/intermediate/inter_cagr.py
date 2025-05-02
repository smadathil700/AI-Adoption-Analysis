#using snowpark
import snowflake.snowpark.functions as F

def model(dbt, session):

   df = dbt.ref('stg_ai_adoption_data')   
   
   """
   calculating CAGR
   """
 
   #calculating initial and final year
   years_df = df.group_by('country_code').\
            agg(F.min(F.col('year')).alias('initial_year'),F.max(F.col('year')).alias('final_year'))
    
   # Get initial and final values for each country
   country_values = df.join(
        years_df,
        df["country_code"] == years_df["country_code"]
    ).filter(\
        (df["year"] == years_df["initial_year"]) | (df["year"] == years_df["final_year"])
    ).select(
        df["country_code"].alias('country_code'),
        df["year"],
        years_df["initial_year"],
        years_df["final_year"],
        F.col('total_ai_investment_usd_mm')
    )
   
   
   # Pivot to get initial and final values in separate columns
   initial_df = country_values.filter(country_values["year"] == country_values["initial_year"]).select(
        F.col("country_code").alias("country_initial"),
        F.col("year").alias("initial_year"),
        F.col('total_ai_investment_usd_mm').alias("initial_value")
    )
   
   
   final_df = country_values.filter(country_values["year"] == country_values["final_year"]).select(
        F.col("country_code").alias("country_final"),
        F.col("year").alias("final_year"),
        F.col('total_ai_investment_usd_mm').alias("final_value")
    )
    
   # Join the initial and final values
   combined_df = initial_df.join(
        final_df,
        initial_df["country_initial"] == final_df["country_final"]
    )
   
   
   # Calculate CAGR using Snowflake SQL
   # CAGR = (final_value / initial_value)^(1 / (final_year - initial_year)) - 1
   cagr_df = combined_df.select(\
        F.col("country_initial").alias("country_code"),
        F.col("initial_year"),
        F.col("final_year"),
        F.col("initial_value"),
        F.col("final_value"),
        F.round(\
            (pow(F.col("final_value") / F.col("initial_value"),\
                 F.lit(1) / (F.col("final_year") - F.col("initial_year"))) - F.lit(1)) * F.lit(100),\
            F.lit(2)
        ).alias("cagr_percent")
    )
    
   # Sort by CAGR in descending order
   result_df_sorted = cagr_df.sort(F.col("cagr_percent").desc())

   # add global average value
   result_df = result_df_sorted.with_column('global_avg', F.round(F.mean(F.col('cagr_percent')).over(), 2))

   return result_df