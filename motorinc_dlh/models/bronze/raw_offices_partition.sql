{{ config(
    materialized='view',
    schema='raw',
    pre_hook="{{ add_raw_partition('offices') }}"
) }}
-- Activates current partition for raw.offices (pre-hook runs ADD PARTITION).
select 1 as partition_activated
