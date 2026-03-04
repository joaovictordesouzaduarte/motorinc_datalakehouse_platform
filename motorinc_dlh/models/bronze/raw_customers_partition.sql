{{ config(
    materialized='view',
    schema='raw',
    pre_hook="{{ add_raw_partition('customers') }}"
) }}
-- Activates current partition for raw.customers (pre-hook runs ADD PARTITION).
select 1 as partition_activated
