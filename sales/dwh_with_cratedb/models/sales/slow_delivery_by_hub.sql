select
    city,
    (select count(*) from {{ ref('slow_delivery') }} where within(coord, shape))
        as within_shape,
    (
        select count(*)
        from {{ ref('slow_delivery') }}
        where distance(coord, distribution_hubs.coords) < 30000
    ) as within_distance
from {{ source('sales', 'distribution_hubs') }} as distribution_hubs


-- insert into sales.offices values (
--     'Vienna',
--     [16.372778, 48.209206],
--     {
--         type='Polygon', 
--         coordinates=[[
--             [16.310016, 48.255302], 
--             [16.414540, 48.306153], 
--             [16.495410, 48.189269], 
--             [16.366504, 48.115341], 
--             [16.256203, 48.161690], 
--             [16.310016, 48.255302]
--         ]]
--     }
-- )