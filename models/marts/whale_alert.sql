with whales as (

select output_address, sum(output_value) as total_sent, count(*) as tx_count from {{ ref('stg_btc_trans')}}
where output_value > 10
group by output_address
order by total_sent desc

),
latest_price as (
    select price from {{ ref('btc_usd_max')}}
    where to_date(replace(snapped_at, ' UTC',''))=current_date()
)

select w.output_address,w.total_sent,
w.tx_count,(p.price*w.total_sent) as total_sent_usd
from whales w
cross join latest_price p
order by total_sent desc