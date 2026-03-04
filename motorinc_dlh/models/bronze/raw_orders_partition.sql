{{ config(
    materialized='view',
    schema='raw',
    pre_hook="{{ add_raw_partition('orders') }}"
) }}
-- Activates current partition for raw.orders (pre-hook runs ADD PARTITION).
select 1 as partition_activated
