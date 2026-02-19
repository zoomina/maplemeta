-- DAG run query (single aggregate date)
-- Replace {{ ds }} with execution date when running manually.
select dm.refresh_marts(array[date '{{ ds }}']);
