with slow_delivery as (
    select
        review,
        coord
    from ref('purchases_with_reviews') where match(review, 'slow delivery')
)

select * from slow_delivery where _score > 1