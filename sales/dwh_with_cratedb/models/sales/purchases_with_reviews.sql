select
    purchases.event_id,
    platform,
    sales,
    purchase_date::timestamp,
    items,
    traffic_source,
    user_id,
    review,
    review_date::timestamp,
    [longitude, latitude]::geo_point as coord
from {{ source('sales', 'purchases') }} as purchases
inner join
    {{ source('sales', 'reviews') }} as reviews
    on purchases.event_id = reviews.event_id