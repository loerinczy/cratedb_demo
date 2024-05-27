with slow_delivery as (
    select
        review,
        coord,
        _score
    from {{ ref('purchases_with_reviews') }}
    where match(review, 'slow delivery')
)

select
    review,
    coord
from slow_delivery where _score >= 1