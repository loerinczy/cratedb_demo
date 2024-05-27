{{
    config(
        materialized="incremental"
    )
}}


select
    purchases.event_id,
    platform,
    sales,
    cast((cast(purchase_date as float) / 1000000) as timestamp)
        as purchase_date_ts,
    items,
    traffic_source,
    user_id,
    review,
    cast((cast(review_date as float) / 1000000) as timestamp) as review_date_ts,
    cast([longitude, latitude] as geo_point) as coord
from {{ source('sales', 'purchases') }} as purchases
inner join
    {{ source('sales', 'reviews') }} as reviews
    on purchases.event_id = reviews.event_id

{% if is_incremental() %}
    where
        review_date
        >= (
            select
                coalesce(
                    max(review_date),
                    1559154170
                )
            from {{ this }}
        )

{% endif %}