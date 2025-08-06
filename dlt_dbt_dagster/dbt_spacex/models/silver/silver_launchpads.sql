{{
  config(
    unique_key=['id', 'valid_from']
  )
}}

with bronze_launchpads as (
  select * from {{ source('bronze_spacex', 'bronze_launchpads') }}
),

cleaned_launchpads as (
  select
    id,
    name,
    full_name,
    locality,
    region,
    try_cast(latitude as numeric) as latitude,
    try_cast(longitude as numeric) as longitude,
    try_cast(launch_attempts as integer) as launch_attempts,
    try_cast(launch_successes as integer) as launch_successes,
    launches,
    rockets,
    lower(status) as status,
    details,
    timezone,
    _dlt_valid_from as valid_from,
    _dlt_valid_to as valid_to,
    case
      when _dlt_valid_to is null then true
      else false
    end as is_current_record,
    _dlt_load_id as source_load_id,
    {{ current_timestamp() }} as silver_load_timestamp
  from bronze_launchpads
)

select * from cleaned_launchpads

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
