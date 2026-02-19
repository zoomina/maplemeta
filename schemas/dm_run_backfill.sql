-- Backfill run query (2025-12 Wednesdays, 5 weeks)
select dm.refresh_marts(
    array[
        date '2025-12-03',
        date '2025-12-10',
        date '2025-12-17',
        date '2025-12-24',
        date '2025-12-31'
    ]
);
