
-- category report
with cte as 
(
    SELECT 
        c.name as catg_name
        , i.name as item_name
        , (SUM(moi.quantity) * i.sell_price) AS gross_sales
        , sum(moi.quantity) as qty
        , max(50) as seq
    FROM mh_orders o
    JOIN mh_order_items moi ON moi.order_id = o.id
    join mh_item_category ic on ic.item_id = moi.item_id
    join mh_categories c on c.id = ic.category_id
    join mh_items i on i.id = moi.item_id
    JOIN mh_order_types t ON t.id = o.order_type_id
    JOIN mh_location mhl ON o.location_id = mhl.id
    WHERE mhl.id = {{location_id}}
    and (
        (t.type_group IN ('D') AND o.status_id IN (15, 16, 17)) OR
        (t.type_group IN ('I') AND o.status_id IN (11, 12, 13, 14, 15, 16, 17, 43)) OR
        (t.type_group IN ('P', 'S') AND o.status_id IN (15, 16, 17, 11, 19, 43, 12, 13, 14))
    ) 
    and c.category_type = 'R'
    AND date(CONVERT_TZ(cast(concat(o.order_date, ' ', o.order_time) as datetime), 'UTC', case when mhl.timezone_cd = 'IST' then '+05:30' when mhl.timezone_cd = 'America/Chicago' then '-05:00' else '-04:00' END)) between {{start_date}} AND  {{end_date}}
    group by i.name,c.name,i.sell_price
) ,

cte1 as (
    select 
        sum(gross_sales) as totalSales
    from cte
) ,

cte2 as (
    select 
        catg_name
        , max('Category Total') as item_name
        , sum(gross_sales) as catgSales 
        , sum(qty) as catgQty
        , max(100) as seq
    from cte
    group by catg_name
    order by catg_name
) ,

cte3 as (
    select 
        max('Total') as catg_name
        , max('Total') as item_name
        , sum(gross_sales) as catgSales 
        , sum(qty) as catgQty
        , max(1000) as seq
    from cte
)

select 
    a. catg_name as 'Category Name'
    , a.item_name as 'Item Name'
    , a. gross_sales as 'Gross Sales'
    , a.qty as 'Sold'
    , a.netSales as "%Net Sales"
from 
(
    SELECT 
        cte.catg_name
        , cte.item_name
        , cte.gross_sales
        , cte.qty
        , concat(round(((cte.gross_sales/cte1.totalSales) * 100),2),'%') as netSales 
        , seq
    from cte,cte1
    
    union all
    
    SELECT 
        cte2.catg_name
        , cte2.item_name
        , cte2.CatgSales as gross_sales
        , cte2.catgQty
        , concat(round(((cte2.CatgSales/cte1.totalSales) * 100),2),'%') as netSales
        , seq
    from cte2, cte1

    union all
    
    SELECT 
        cte3.catg_name
        , cte3.item_name
        , cte3.CatgSales as gross_sales
        , cte3.catgQty
        , concat(round(((cte3.CatgSales/cte1.totalSales) * 100),2),'%') as netSales
        , seq
    from cte3, cte1
) a
where 1=1
[[and ((a.catg_name LIKE CONCAT ('%',{{SearchInput}},'%')) OR (a.item_name LIKE CONCAT ('%',{{SearchInput}},'%')))]]
order by catg_name, seq, gross_sales desc


-- Top 20 Popular Items
SELECT 
    `mh_order_items`.`item_name` as 'Product name'
    , sum(`mh_order_items`.quantity ) AS `Quantity`
FROM `mh_orders` 
INNER  JOIN `mh_location` mhl ON `mh_orders`.`location_id` = mhl.`id` 
inner join `mh_order_items` on `mh_orders`.`id` = `mh_order_items`.`order_id`
inner join `mh_order_types` t on t.id = `mh_orders`.order_type_id
where  
date( CONVERT_TZ (cast(concat (`mh_orders`.`order_date`, ' ', `mh_orders`.`order_time`) as datetime), 'UTC',mhl.timezone_cd)) between {{orderDate}} and {{endDate}} and 
(
    (t.type_group in ('D') and `mh_orders`.status_id in (15,16,17)) or 
    (t.type_group in ('I') and `mh_orders`.status_id in (15,16,17)) or 
    (t.type_group in ('P','S') and `mh_orders`.status_id in (11,12,13,14,19,15,16,17))
)
and `mh_orders`.`location_id` = {{locationId}}
GROUP BY `mh_order_items`.`item_name`
order by sum(`mh_order_items`.quantity) desc, `mh_order_items`.`item_name` LIMIT 20

-- Top 20 Revenue Making Productsl 3

SELECT 
    `mh_order_items`.`item_name` as 'Product name', 
--    sum(`mh_order_items`.quantity ) AS `Quantity` , 
    sum(`mh_order_items`.quantity * `mh_order_items`.price) as total_product_sales
FROM `mh_orders` 
INNER  JOIN `mh_location` mhl ON `mh_orders`.`location_id` = mhl.`id` 
inner join `mh_order_items` on `mh_orders`.`id` = `mh_order_items`.`order_id`
inner join `mh_order_types` t on t.id = `mh_orders`.order_type_id
where  
    date( CONVERT_TZ (cast(concat (`mh_orders`.`order_date`, ' ', `mh_orders`.`order_time`) as datetime), 'UTC',mhl.timezone_cd)) between {{orderDate}} and {{endDate}}
    and 
    (
    (t.type_group in ('D') and `mh_orders`.status_id in (15,16,17)) or 
    (t.type_group in ('I') and `mh_orders`.status_id in (15,16,17)) or 
    (t.type_group in ('P','S') and `mh_orders`.status_id in (11,12,13,14,19,15,16,17))
    )
    and `mh_orders`.`location_id` = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
GROUP BY `mh_order_items`.`item_name`
order by total_product_sales  desc, `mh_order_items`.`item_name` LIMIT 20

-- Least 20 Popular Items

SELECT 
    `mh_order_items`.`item_name` as 'Product name'
    , sum(`mh_order_items`.quantity ) AS `Quantity`
FROM `mh_orders` 
INNER  JOIN `mh_location` mhl ON `mh_orders`.`location_id` = mhl.`id` 
inner join `mh_order_items` on `mh_orders`.`id` = `mh_order_items`.`order_id`
inner join `mh_order_types` t on t.id = `mh_orders`.order_type_id
where  
date( CONVERT_TZ (cast(concat (`mh_orders`.`order_date`, ' ', `mh_orders`.`order_time`) as datetime), 'UTC',mhl.timezone_cd)) between '2022-06-01' and '' and 
(
    (t.type_group in ('D') and `mh_orders`.status_id in (15,16,17)) or 
    (t.type_group in ('I') and `mh_orders`.status_id in (15,16,17)) or 
    (t.type_group in ('P','S') and `mh_orders`.status_id in (11,12,13,14,19,15,16,17))
)
and `mh_orders`.`location_id` = {{locationId}}
GROUP BY `mh_order_items`.`item_name`
order by sum(`mh_order_items`.quantity), `mh_order_items`.`item_name` LIMIT 20


-- Least 20 Revenue Making Products


SELECT 
`mh_order_items`.`item_name` as 'Product name', 
-- sum(`mh_order_items`.quantity ) AS `Quantity` , 
-- `mh_order_items`.price as 'Price',
sum(`mh_order_items`.quantity * `mh_order_items`.price) as total_product_sales
FROM `mh_orders` 
INNER  JOIN `mh_location` mhl ON `mh_orders`.`location_id` = mhl.`id` 
inner join `mh_order_items` on `mh_orders`.`id` = `mh_order_items`.`order_id`
inner join `mh_order_types` t on t.id = `mh_orders`.order_type_id
where  
date( CONVERT_TZ (cast(concat (`mh_orders`.`order_date`, ' ', `mh_orders`.`order_time`) as datetime), 'UTC',mhl.timezone_cd)) 
between {{orderDate}} and {{endDate}}
and (
(t.type_group in ('D') and `mh_orders`.status_id in (15,16,17)) or 
(t.type_group in ('I') and `mh_orders`.status_id in (15,16,17)) or 
(t.type_group in ('P','S') and `mh_orders`.status_id in (11,12,13,14,19,15,16,17))
)
 and `mh_orders`.`location_id` = {{locationId}}
GROUP BY `mh_order_items`.`item_name`
 order by total_product_sales, `mh_order_items`.`item_name`  LIMIT 20
 
 
 -- Top Categories

select 
    name as 'Category', 
    sum(total_sales) as ttl_sales 
from (
    select  
        mc.name
    --    , sum(moi.quantity) as 'Items sold'
        , sum(moi.subtotal) as total_sales 
    --    , mio.category_id as category_id
    from mh_orders mo 
    join mh_order_items moi on moi.order_id = mo.id
    join mh_item_category mio on moi.item_id = mio.item_id 
    join mh_order_types t on t.id = mo.order_type_id
    join mh_location mhl on mhl.id = mo.location_id
    join mh_categories mc on mc.id = mio.category_id 
    where 
        mc.parent_id is null 
        and mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8' 
        and date( CONVERT_TZ (cast(concat (mo.order_date, ' ', mo.order_time) as datetime), 'UTC',mhl.timezone_cd)) between '2022-06-01' and '2024-12-28' 
        and 
        (
        (t.type_group in ('D') and mo.status_id in (15,16,17)) or 
        (t.type_group in ('I') and mo.status_id in (15,16,17)) or 
        (t.type_group in ('P','S') and mo.status_id in (11,12,13,14,15,16,17,43))
        )
        and mc.category_type='R'
    GROUP BY  mc.name
    union all
    select  
        (select name from mh_categories where id=mc.parent_id)   as name
    --    , sum(moi.quantity) 'Items sold'
        , sum(moi.subtotal) as total_sales
    --    , mc.parent_id as category_id
    from mh_orders mo 
    join mh_order_items moi on moi.order_id = mo.id
    join mh_item_category mio on moi.item_id = mio.item_id 
    join mh_order_types t on t.id = mo.order_type_id 
    join mh_location mhl on mhl.id = mo.location_id
    join mh_categories mc on mc.id = mio.category_id 
    where 
        mc.parent_id is not null 
        and mo.location_id ='0abe0c6e-88b0-45a3-921c-bb9662eac0b8' 
        and date( CONVERT_TZ (cast(concat (mo.order_date, ' ', mo.order_time) as datetime), 'UTC',mhl.timezone_cd))  between '2022-06-01' and '2024-12-28'
        and 
        (
            (t.type_group in ('D') and mo.status_id in (15,16,17)) or 
            (t.type_group in ('I') and mo.status_id in (11,12,15,16,17)) or 
            (t.type_group in ('P','S') and mo.status_id in (19,11,12,13,14,15,16,17))
        ) 
        and mc.category_type='R'
    GROUP BY name
) a 
group by name
order by ttl_sales desc, name limit 40

-- Top Revenue Streams

select  
    mht.tag_name as 'Revenue Class'
    , sum(moi.quantity) as 'Items sold'
    , sum(moi.subtotal) as 'Total Sales'
from mh_orders mo 
join mh_location mhl on mhl.id = mo.location_id
join mh_order_items moi on moi.order_id = mo.id
join mh_item_tags mhit on moi.item_id=mhit.item_id
join mh_tags mht on mht.id=mhit.tag_id 
where mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'  
    and mo.status_id in (11,12,13,14,43,15,16,17)
    and date( CONVERT_TZ (cast(concat (mo.order_date, ' ', mo.order_time) as datetime), 'UTC',mhl.timezone_cd))  between '2022-06-01' and '2024-12-28'
    and mht.is_revenue=1
GROUP BY mht.tag_name
order by sum(moi.subtotal) desc, mht.tag_name

-- Top Voided Items

select 
    i.name
    , sum(oih.updated_quantity) as void_items
from mh_order_items_history oih 
join mh_orders o on o.id = oih.order_id
join mh_items i on i.id= oih.item_id
-- join mh_order_types ot on ot.id = o.order_type_id
join mh_location l on l.id=o.location_id
where  o.location_id={{locationId}} 
    and date( CONVERT_TZ (cast(concat ( o.order_date, ' ',  o.order_time) as datetime), 'UTC', l.timezone_cd)) between {{startDate}} and {{endDate}} 
    and oih.action in('SUBTRACT' ,'REMOVED')
group by i.name
order by void_items desc, i.name
limit 100

-- Top Cancel Reasons
select 
    case when (oi.cancel_reason is null or length(trim(oi.cancel_reason))=0) then 'UNKNOWN' 
        else Upper(oi.cancel_reason)
    end as cancel_rsn
    , sum(oih.updated_quantity) as void_items
from mh_order_items_history oih
join mh_orders o on o.id = oih.order_id
join mh_order_items oi on oi.id = oih.order_item_id
join mh_location l on l.id=o.location_id
where o.location_id={{locationId}} 
    and  oih.action in('SUBTRACT' ,'REMOVED')
    and date( CONVERT_TZ (cast(concat ( o.order_date, ' ',  o.order_time) as datetime), 'UTC', l.timezone_cd)) between {{startDate}} and {{endDate}}
group by cancel_rsn
order by void_items desc limit 100


-- Top Item Cancel Reasons
select 
    name
    , cancel_rsn
    , void_items
from (
    select 
        oih.item_name as name
        , case when (oi.cancel_reason is null or length(trim(oi.cancel_reason))=0) then 'UNKNOWN' 
            else Upper(oi.cancel_reason)
        end as cancel_rsn
        , sum(oih.updated_quantity) as void_items
    from mh_order_items_history oih
    join mh_orders o on o.id = oih.order_id
--    join mh_items i on i.id= oih.item_id
    join mh_order_items oi on oi.id = oih.order_item_id
    -- join mh_order_kot_items ki on ki.order_id = o.id
    join mh_location l on l.id=o.location_id
    where o.location_id={{locationId}} 
        -- and oi.quantity = 0
        and  oih.action in('SUBTRACT' ,'REMOVED')
        and date( CONVERT_TZ (cast(concat ( o.order_date, ' ',  o.order_time) as datetime), 'UTC', l.timezone_cd)) between {{startDate}} and {{endDate}}
    group by oih.item_name, cancel_rsn
) a
order by void_items desc limit 100

-- Today's Check In

SELECT count(*) AS `count`
FROM `mh_reservation`
LEFT JOIN `mh_location` `Location` ON `mh_reservation`.`location_id` = `Location`.`id` LEFT JOIN `mh_merchant` `Merchant` ON `Location`.`merchant_id` = `Merchant`.`id`
WHERE (`mh_reservation`.`reservation_status` in ('C','A')
   AND `Location`.`id` = {{locid}}
   AND date(CONVERT_TZ(`mh_reservation`.`reservation_time`,'UTC',case when `Location`.`timezone_cd` = 'IST' then '+05:30' else '-04:00' END )) = date(CONVERT_TZ(current_time(),'UTC',case when `Location`.`timezone_cd` = 'IST' then '+05:30' else '-04:00' END ))
  )
  
  
  
