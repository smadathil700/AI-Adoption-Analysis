<style>
theme {
    font-family: Calibri
}
</style>
# AI Adoption Data Analysis Using DBT (Python), Snowflake, Airflow and PowerBI
<u> </u>
<theme>
This dbt project focuses on dbt with Python. Does analysis on AI adoption data of various countries for years 2020-24.
</theme>
## Project overview
<theme>
Focus is on analysing AI adoption data for past 5 years for several countries. For this built an automated data pipeline using:
<br></br>

> 1.  **Source**: CSV files with real data. (seeds/raw_ai_adoption_data.csv, seeds/raw_countries.csv, seeds/raw_industries.csv)
>
> 2.  **ELT**: Dbt core has been used for ELT (Extract, Load and Transform) of source data. Since snowflake has been used as warehouse, dbt snowflake is used. Transformations are done using snowpark (intermediate/inter_cagr.py) and pandas (intermediate/inter_roi.py)
>
> 3. **Orchestration**: Apache airflow implemented using Astronomer and docker has been used for workflow scheduling. (https://www.youtube.com/watch?v=OLXkGB7krGo has been quite helpful for the same)
>
> 4. **Warhouse**: Used snowflake for data storage
>
> 5. **Reporting**: PowerBI along with Pandas charts used for creating visualizations
<br></br>

![alt text](images\Workflow.png)

## Data Modelling

There are three tables in data modelling:

>1.  Fact table with details about country adoption rate, AI investment USD MM, company investment USD MM, company return USD MM, ROI(%), CAGR(%)
>
>2. Dimension tables for countries and industries with data

![alt text](images\datamodel.png)

## Data Analysis

Analysis show countries are increasing their AI investment on an average by 25% from 2020-24 and shows positive correlation between AI investment and AI return/adoption rate/ROI in all countries.

Supporting visualizations:

>1. CAGR for all countries from 2020-24 showing as average of 25%

![alt text](images\cagr.png)

>2. Top 6 industries investment vs return showing positive relation

![alt text](<images\industries vs return.png>)

>3. Correlation between AI investment and adoption rate showing positive

![alt text](<images\adoption rate vs investment.png>)

>4. Correlation between AI investment and ROI by different industries also positive

![alt text](images\correlation.png)

</span>