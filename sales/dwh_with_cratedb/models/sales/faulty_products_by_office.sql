select
    city,
    (
        select count(*)
        from {{ ref('faulty_products') }}
        where within(coord, shape)
    ) as within_area,
    (
        select count(*)
        from {{ ref('faulty_products') }}
        where distance(coord, offices.coords) < 30000
    ) as within_distance
from {{ source('sales', 'offices') }} as offices
