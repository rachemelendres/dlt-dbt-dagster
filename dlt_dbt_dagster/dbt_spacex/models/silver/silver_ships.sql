{{
  config(
    unique_key=['id', 'valid_from']
  )
}}

with bronze_ships as (
  select * from {{ source('bronze_spacex', 'bronze_ships') }}
),

cleaned_ships as (
  select
    id,
    name,
    lower(type) as type,
    active,
    mass_kg,
    try_cast(year_built as integer) as year_built,
    home_port,
    model,
    launches,
    _dlt_valid_from as valid_from,
    _dlt_valid_to as valid_to,
    case
      when _dlt_valid_to is null then true
      else false
    end as is_current_record,
    _dlt_load_id as source_load_id,
    {{ current_timestamp() }} as silver_load_timestamp
  from bronze_ships
)

select * from cleaned_ships

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
