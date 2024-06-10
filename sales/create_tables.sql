create table dome.test (rating double, review text index using fulltext);
create table reviews.delivery as select review from dome.test where match(review, 'slow delivery') using best_fields order by _score desc limit 10000;
create table reviews.faulty as select review from dome.test where match(review, 'faulty not working wrong') using best_fields order by _score desc limit 10000;
create table reviews.random as select review from dome.test limit 10000;
create table maps.random_reviews (lat double, lng double, reviews text index using fulltext)
create table sales.distribution_hubs (city text, coords geo_point, shape geo_shape);
create table sales_dev.purchases_with_reviews (event_id bigint, platform text, sales text, purchase_date_ts timestamp, items text, traffic_source text, user_id text, review text index using fulltext, review_date_ts timestamp, coord geo_point);

-- populate offices
insert into sales.offices values ('Vienna', [16.372778, 48.209206], {type='Polygon', coordinates=[[[16.310016, 48.255302], [16.414540, 48.306153], [16.495410, 48.189269], [16.366504, 48.115341], [16.256203, 48.161690], [16.310016, 48.255302]]]});
insert into sales.offices values ('Linz', [14.291626, 48.297052], {type='Polygon', coordinates=[[[14.280377, 48.336109], [14.343151, 48.323615], [14.343113, 48.262829], [14.275833, 48.261456], [14.252443, 48.302580], [14.280377, 48.336109]]]});
insert into sales.offices values ('Graz', [15.438914, 47.062267], {type='Polygon', coordinates=[[[15.484044, 47.097248], [15.513320, 47.045236], [15.450947, 46.989463], [15.384385, 47.040614], [15.388978, 47.103646], [15.484044, 47.097248]]]});

-- populate distribution hubs
insert into sales.distribution_hubs values ('Graz', [15.438914, 47.062267], {type='Polygon', coordinates=[[[16.050626, 47.366189], [15.896594, 46.730609], [14.844154, 46.932320], [15.281793, 47.411517], [16.050626, 47.366189]]]});
insert into sales.distribution_hubs values ('Salzburg', [13.041514, 47.805990], {type='Polygon', coordinates=[[[12.925350, 48.000131], [13.263535, 47.853108], [13.086746, 47.739421], [12.952713, 47.829995], [12.925350, 48.000131]]]});
insert into sales.distribution_hubs values ('Innsbruck', [11.408526, 47.265203], {type='Polygon', coordinates=[[[11.832892, 47.408344], [11.088454, 46.887962], [10.315724, 47.133737], [11.304621, 47.424782], [11.832892, 47.408344]]]});


create table sales_dev.reviews as select event_id, review, review_date from sales_dev.purchases_with_reviews where review is not null;
create table sales_dev.purchases as select event_id, platform, sales, purchase_date, items, traffic_source, user_id, longitude, latitude from temp.purchases_with_reviews;

-- testing
create table sales_dev.delivery as select * from (select [lng, lat]::geo_point as coord, reviews as review, _score from maps.random_reviews where match(reviews, 'slow delivery')) as delivery where _score >= 1;
create table sales_dev.faulty as select * from (select [lng, lat]::geo_point as coord, reviews as review, _score from maps.random_reviews where match(reviews, 'faulty not working wrong')) as delivery where _score >= 1;

create table sales.slow_delivery_by_hub as select city, (select count(*) from sales_dev.delivery where within(coord, shape)) as within_area, (select count(*) from sales_dev.delivery where distance(coord, distribution_hubs.coords) < 30000) as within_distance from sales_dev.distribution_hubs limit 100;
create table sales.faulty_product_by_office as select city, (select count(*) from sales_dev.faulty where within(coord, shape)) as within_area, (select count(*) from sales_dev.faulty where distance(coord, offices.coords) < 30000) as within_distance from sales_dev.offices limit 100;