{{ config(
    materialized='view',
    schema='raw',
    pre_hook="{{ add_raw_partition('orderdetails') }}"
) }}
-- Activates current partition for raw.orderdetails (pre-hook runs ADD PARTITION).
select 1 as partition_activated
