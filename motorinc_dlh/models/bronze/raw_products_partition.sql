{{ config(
    materialized='view',
    schema='raw',
    pre_hook="{{ add_raw_partition('products') }}"
) }}
-- Activates current partition for raw.products (pre-hook runs ADD PARTITION).
select 1 as partition_activated
