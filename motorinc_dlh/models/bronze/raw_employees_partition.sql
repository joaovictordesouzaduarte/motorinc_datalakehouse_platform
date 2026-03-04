{{ config(
    materialized='view',
    schema='raw',
    pre_hook="{{ add_raw_partition('employees') }}"
) }}
-- Activates current partition for raw.employees (pre-hook runs ADD PARTITION).
select 1 as partition_activated
