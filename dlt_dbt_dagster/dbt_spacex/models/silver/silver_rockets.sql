{{
  config(
    unique_key=['id', 'valid_from']
  )
}}

with bronze_rockets as (
  select * from {{ source('bronze_spacex', 'bronze_rockets') }}
),

cleaned_rockets as (
  select
    id,
    name,
    description,
    company,
    type,
    active,
    stages,
    boosters,
    cost_per_launch,
    success_rate_pct,
    country,
    case
      when first_flight is not null and first_flight != ''
      then cast(first_flight as date)
      else null
    end as first_flight_date,
    _dlt_valid_from as valid_from,
    _dlt_valid_to as valid_to,
    case
      when _dlt_valid_to is null then true
      else false
    end as is_current_record,
    _dlt_load_id as source_load_id,
    {{ current_timestamp() }} as silver_load_timestamp
  from bronze_rockets
)

select * from cleaned_rockets

{% if is_incremental() %}
  where (
    valid_from > (
      select coalesce(max(valid_from), '1900-01-01'::timestamp)
      from {{ this }}
    )
    or
    (valid_from is not null and valid_to is not null)
  )
{% endif %}
