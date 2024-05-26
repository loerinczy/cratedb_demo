select
    city,
    (select count(*) from {{ ref('slow_delivery') }} where within(coord, shape))
        as within_area,
    (
        select count(*)
        from {{ ref('slow_delivery') }}
        where distance(coord, distribution_hubs.coords) < 30000
    ) as within_distance
from {{ source('sales', 'distribution_hubs') }} as distribution_hubs