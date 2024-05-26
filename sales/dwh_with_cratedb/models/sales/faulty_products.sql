with faulty_products as (
    select
        review,
        _score
    from ref('purchases_with_reviews')
    where match(review, 'faulty not working wrong')
)

select * from faulty_products where _score > 1