-- Export schema metadata for the current database.
-- Run each query in SQLTools and export the result grid to CSV if needed.

-- 1) Table overview: estimated row counts and storage size
SELECT
    table_schema,
    table_name,
    table_rows,
    ROUND((data_length + index_length) / 1024 / 1024, 2) AS size_mb,
    engine,
    table_collation,
    create_time,
    update_time
FROM information_schema.tables
WHERE table_schema = DATABASE()
ORDER BY size_mb DESC, table_name;

-- 2) Column dictionary for all tables in the current database
SELECT
    c.table_schema,
    c.table_name,
    c.ordinal_position,
    c.column_name,
    c.column_type,
    c.data_type,
    c.is_nullable,
    c.column_key,
    c.column_default,
    c.extra,
    c.character_maximum_length,
    c.numeric_precision,
    c.numeric_scale,
    c.datetime_precision,
    c.column_comment
FROM information_schema.columns c
WHERE c.table_schema = DATABASE()
ORDER BY c.table_name, c.ordinal_position;

-- 3) Index dictionary
SELECT
    s.table_schema,
    s.table_name,
    s.index_name,
    s.non_unique,
    s.seq_in_index,
    s.column_name,
    s.collation,
    s.cardinality,
    s.sub_part,
    s.nullable,
    s.index_type
FROM information_schema.statistics s
WHERE s.table_schema = DATABASE()
ORDER BY s.table_name, s.index_name, s.seq_in_index;

-- 4) Foreign key dictionary (may be empty if FK constraints are not defined)
SELECT
    kcu.constraint_name,
    kcu.table_name,
    kcu.column_name,
    kcu.referenced_table_name,
    kcu.referenced_column_name
FROM information_schema.key_column_usage kcu
WHERE kcu.table_schema = DATABASE()
  AND kcu.referenced_table_name IS NOT NULL
ORDER BY kcu.table_name, kcu.constraint_name, kcu.ordinal_position;
