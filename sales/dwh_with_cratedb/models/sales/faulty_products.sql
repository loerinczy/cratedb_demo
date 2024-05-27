with faulty_products as (
    select
        review,
        coord,
        _score
    from {{ ref('purchases_with_reviews') }}
    where match(review, 'faulty not working wrong')
)

select
    review,
    coord
from faulty_products where _score >= 1