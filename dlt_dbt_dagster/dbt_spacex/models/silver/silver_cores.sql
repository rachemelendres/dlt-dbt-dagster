{{
  config(
    unique_key=['id', 'valid_from']
  )
}}

with bronze_cores as (
  select * from {{ source('bronze_spacex', 'bronze_cores') }}
),

cleaned_cores as (
  select
    id,
    lower(status) as status,
    serial,
    try_cast(reuse_count as integer) as reuse_count,
    launches,
    try_cast(last_update as timestamp) as last_update_timestamp,
    _dlt_valid_from as valid_from,
    _dlt_valid_to as valid_to,
    case
      when _dlt_valid_to is null then true
      else false
    end as is_current_record,
    _dlt_load_id as source_load_id,
    {{ current_timestamp() }} as silver_load_timestamp
  from bronze_cores
)

select * from cleaned_cores

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
