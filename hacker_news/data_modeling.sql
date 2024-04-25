-- creata raw story table
create table bronze.raw (
    deleted boolean,
    dead boolean,
    parent int,
    poll int,
    parts array (int),
    author text,
    descendants int,
    id int,
    kids array (int),
    score int,
    time bigint,
    title text,
    type text,
    url text
);