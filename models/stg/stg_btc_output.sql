{{ config(materialized='incremental', incremental_strategy='append')}}

with flattened_outputs as (

select

tx.HASH_KEY,
tx.block_number,
tx.BLOCK_TIMESTAMP,
tx.is_coinbase,
f.value:address::STRING as output_address,
f.value:value::FLOAT as output_value

from {{ ref("stg_btc")}} tx,
lateral flatten(input => outputs) f

where f.value:address is not null

{% if is_incremental() %}
and  tx.BLOCK_TIMESTAMP >= (select max(tx.BLOCK_TIMESTAMP) from {{ this }})
{% endif %}

)
select
HASH_KEY,
block_number,
BLOCK_TIMESTAMP,
is_coinbase,
output_address,
output_value
from flattened_outputs