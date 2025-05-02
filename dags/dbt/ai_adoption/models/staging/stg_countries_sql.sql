/*with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_countries') }}

),

renamed as (

    select
        CountryCode as country_code,
        Country as country_name

    from source

)

select * from renamed*/
