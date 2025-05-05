#using pandas
import pandas as pd

def model(dbt, session):

   df = dbt.ref('stg_ai_adoption_data')   
   
   """
   calculating ROI for companies
   """
   #convert to pandas
   df_pandas = df.to_pandas()

   #changing column heading case to lower
   df_pandas.columns = df_pandas.columns.str.lower()
   
   #filtering for fewer columns
   df_pandas_reduced=df_pandas.filter(items=['country_code','year','company','ai_investment_usd_mm','return_from_ai_usd_mm','industry_code']).copy()

   #calculating roi
   df_pandas_reduced['roi_percent'] = ((df_pandas_reduced['return_from_ai_usd_mm']-df_pandas_reduced['ai_investment_usd_mm'])/df_pandas_reduced['ai_investment_usd_mm'])*100

   #rounding roi values to 2 decimals
   df_pandas_reduced['roi'] = df_pandas_reduced['roi_percent'].round(2)

   #dropping unnecessary columns
   result_df = df_pandas_reduced.drop(columns=['roi_percent'])

   #return dataframe
   return result_df
   



