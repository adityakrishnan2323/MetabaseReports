-- Total Order Processed - US
select count(o.id) from mh_orders o join mh_order_types t on t.id = o.order_type_id
join mh_location mhl on mhl.id = o.location_id
where  (
(t.type_group in ('D') and o.status_id in (15,16,17)) or 
(t.type_group in ('I') and o.status_id in (11,12,13,14,15,16,17)) or 
(t.type_group in ('P','S') and o.status_id in (11,12,13,14,43,15,16,17))
)
and date( CONVERT_TZ (cast(concat (o.order_date, '06-09-2024 ', o.order_time) as datetime), 'UTC',mhl.timezone_cd))  between '2023-06-01' and '2023-12-28'
and o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'

-- Total Sales - US
WITH third_party_orders AS (
    SELECT 
        o.id AS orderId,
        t.rate AS taxRate
    FROM 
        mh_orders o 
    INNER JOIN 
        mh_location l ON o.location_id = l.id
    LEFT JOIN 
        mh_taxfees t ON l.id = t.location_id
    WHERE  
        DATE(CONVERT_TZ(CAST(CONCAT(o.order_date, ' ', o.order_time) AS DATETIME), 'UTC', CASE WHEN 'America/Chicago' = 'IST' THEN '+05:30' WHEN l.timezone_cd = 'America/Chicago' THEN '-05:00' ELSE '-04:00' END)) BETWEEN '2023-06-01' AND '2023-12-28'
        AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
        AND o.order_source_name IS NOT NULL
        AND o.status_id IN (15, 16, 17, 11, 19, 43, 12, 13, 14)
        AND t.is_enabled = 1 
        AND t.is_default = 1
),
order_totals AS (
    SELECT 
        ot.order_id AS orderId,
        SUM(CASE WHEN ot.code = 3 THEN ot.value ELSE 0 END) AS tip,
        SUM(CASE WHEN ot.code = 4 THEN ot.value ELSE 0 END) AS additionalCharges,
        SUM(CASE WHEN ot.code = 6 THEN ot.value ELSE 0 END) AS discount,
        SUM(CASE WHEN ot.code = 7 THEN ot.value ELSE 0 END) AS convenienceFee
    FROM 
        mh_order_totals ot
    WHERE 
        ot.order_id IN (SELECT orderId FROM third_party_orders)
    GROUP BY ot.order_id
),
third_party_order_totals AS (
    SELECT 
        tpo.orderId,
        ot.tip,
        ot.additionalCharges,
        ot.discount,
        ot.convenienceFee,
        SUM(oi.quantity * i.sell_price) AS itemTotal,
        SUM(oi.quantity * i.sell_price * (tpo.taxRate / 100)) AS tax
    FROM 
        third_party_orders tpo
    INNER JOIN 
        mh_order_items oi ON tpo.orderId = oi.order_id
    INNER JOIN 
        mh_items i ON oi.item_id = i.id 
    LEFT JOIN 
        order_totals ot ON tpo.orderId = ot.orderId
    GROUP BY tpo.orderId, ot.tip, ot.additionalCharges, ot.discount, ot.convenienceFee
),
magil_order_totals AS (
    SELECT 
        o.id AS orderId,
        SUM(CASE WHEN ot.code = 1 THEN ot.value ELSE 0 END) AS itemTotal,
        SUM(CASE WHEN ot.code = 2 THEN ot.value ELSE 0 END) AS tax,
        SUM(CASE WHEN ot.code = 3 THEN ot.value ELSE 0 END) AS tip,
        SUM(CASE WHEN ot.code = 4 THEN ot.value ELSE 0 END) AS additionalCharges,
        SUM(CASE WHEN ot.code = 6 THEN ot.value ELSE 0 END) AS discount,
        SUM(CASE WHEN ot.code = 7 THEN ot.value ELSE 0 END) AS convenienceFee
    FROM 
        mh_orders o 
    INNER JOIN 
        mh_location l ON o.location_id = l.id
    INNER JOIN 
        mh_order_totals ot ON o.id = ot.order_id
    WHERE  
        DATE(CONVERT_TZ(CAST(CONCAT(o.order_date, ' ', o.order_time) AS DATETIME), 'UTC', CASE WHEN 'America/Chicago' = 'IST' THEN '+05:30' WHEN l.timezone_cd = 'America/Chicago' THEN '-05:00' ELSE '-04:00' END)) BETWEEN '2023-06-01' AND '2023-12-28'
        AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
        AND o.status_id IN (15, 16, 17, 11, 19, 43, 12, 13, 14)
        AND (o.order_source_name IS NULL OR o.order_source_name = '')
    GROUP BY o.id
)

SELECT 
    SUM(itemTotal + tax + tip + additionalCharges + convenienceFee - discount) AS GrossSales
FROM (
    SELECT 
        itemTotal,
        tax,
        tip,
        additionalCharges,
        discount,
        convenienceFee
    FROM 
        third_party_order_totals
    UNION ALL
    SELECT 
        itemTotal,
        tax,
        tip,
        additionalCharges,
        discount,
        convenienceFee
    FROM 
        magil_order_totals
) AS combined_orders;



-- Net Sales - US
WITH third_party_orders AS (
    SELECT 
        o.id AS orderId,
        t.rate AS taxRate
    FROM 
        mh_orders o 
    INNER JOIN 
        mh_location l 
    LEFT JOIN 
        mh_taxfees t 
    ON l.id = t.location_id
    ON o.location_id = l.id
    WHERE  
        date(CONVERT_TZ(cast(concat(o.order_date, ' ', o.order_time) as datetime), 'UTC', case when 'America/Chicago' = 'IST' then '+05:30' when 'mhl.timezone_cd' = 'America/Chicago' then '-05:00' else '-04:00' END)) BETWEEN '2023-06-01' AND '2023-12-28'
        AND o.location_id= '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
        AND o.order_source_name IS NOT NULL
        and o.status_id in (15,16,17,11,19,43,12,13,14)
        AND t.is_enabled = 1 
        AND t.is_default = 1
),
order_totals AS (
    SELECT 
        order_id AS orderId,
        SUM(CASE WHEN code = 6 THEN value ELSE 0 END) AS discount
    FROM 
        mh_order_totals
        where order_id in (SELECT orderId from third_party_orders)
    GROUP BY 1
),
third_party_order_totals AS (
    SELECT 
        tpo.orderId,
        ot.discount,
        SUM(oi.quantity * i.sell_price) AS itemTotal
    FROM 
        third_party_orders tpo
    INNER JOIN 
        mh_order_items oi
    ON tpo.orderId = oi.order_id
    INNER JOIN 
        mh_items i 
    ON oi.item_id = i.id 
    LEFT JOIN
        order_totals ot
    ON tpo.orderId = ot.orderId
    GROUP BY 1, 2
),
magil_order_totals AS (
    SELECT 
        o.id AS orderId,
        SUM(CASE WHEN ot.code = 1 THEN ot.value ELSE 0 END) AS itemTotal,
        SUM(CASE WHEN ot.code = 6 THEN ot.value ELSE 0 END) AS discount
    FROM 
        mh_orders o 
    INNER JOIN 
        mh_location l ON o.location_id = l.id
    INNER JOIN 
        mh_order_totals ot ON o.id = ot.order_id
    WHERE  
        date(CONVERT_TZ(cast(concat(o.order_date, ' ', o.order_time) as datetime), 'UTC', case when 'America/Chicago' = 'IST' then '+05:30' when 'mhl.timezone_cd' = 'America/Chicago' then '-05:00' else '-04:00' END)) BETWEEN '2023-06-01' AND '2023-12-28'
        AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
        AND (o.order_source_name IS NULL OR o.order_source_name = '')
        and o.status_id in (15,16,17,11,19,43,12,13,14)
    GROUP BY o.id
)

SELECT 
    SUM(itemTotal - discount) AS 'Net Sales'
FROM (
    SELECT 
        itemTotal,
        discount
    FROM 
        third_party_order_totals
    UNION ALL
    SELECT 
        itemTotal,
        discount
    FROM 
        magil_order_totals
) AS combined_orders;


-- Tip - US

select case when sum(value)>0 then sum(value) else 0 end as 'Tips' from mh_order_totals where order_id in(select o.id from mh_orders o
 join mh_order_types t on t.id = o.order_type_id 
 join mh_location mhl on mhl.id = o.location_id
 where  (
(t.type_group in ('D') and o.status_id in (15,16,17)) or 
(t.type_group in ('I') and o.status_id in (11,12,13,14,15,16,17)) or 
(t.type_group in ('P','S') and o.status_id in (15,16,17,11,12,13,14,43))
) and 
o.location_id= '0abe0c6e-88b0-45a3-921c-bb9662eac0b8' and  date( CONVERT_TZ (cast(concat (o.order_date, ' ', o.order_time) as datetime), 'UTC',mhl.timezone_cd))  between '2023-06-01'and  '2023-12-28' ) and code=3

-- Tax - US
WITH third_party_orders AS (
    SELECT 
        o.id AS orderId,
        t.rate AS taxRate
    FROM 
        mh_orders o 
    INNER JOIN 
        mh_location l 
    LEFT JOIN 
        mh_taxfees t 
    ON l.id = t.location_id
    ON o.location_id = l.id
    WHERE  
        DATE(CONVERT_TZ(CAST(CONCAT(o.order_date, ' ', o.order_time) AS DATETIME), 'UTC', l.timezone_cd)) BETWEEN {{startDate}} AND '2024-12-28'
        AND o.location_id= '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
        and o.status_id in (15,16,17,11,19,43,12,13,14)
        AND o.order_source_name IS NOT NULL
        AND t.is_enabled = 1 
        AND t.is_default = 1
),
third_party_order_totals AS (
    SELECT 
        tpo.orderId,
        SUM(oi.quantity * i.sell_price * (tpo.taxRate/100)) AS tax
    FROM 
        third_party_orders tpo
    INNER JOIN 
        mh_order_items oi
    ON tpo.orderId = oi.order_id
    INNER JOIN 
        mh_items i 
    ON oi.item_id = i.id 
    GROUP BY 1
),
magil_order_totals AS (
    SELECT 
        o.id AS orderId,
        SUM(CASE WHEN ot.code = 2 THEN ot.value ELSE 0 END) AS tax
    FROM 
        mh_orders o 
    INNER JOIN 
        mh_location l ON o.location_id = l.id
    INNER JOIN 
        mh_order_totals ot ON o.id = ot.order_id
    WHERE  
        DATE(CONVERT_TZ(CAST(CONCAT(o.order_date, ' ', o.order_time) AS DATETIME), 'UTC', l.timezone_cd)) BETWEEN '2024-06-01' AND '2024-12-28'
        AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
        and o.status_id in (15,16,17,11,19,43,12,13,14)
        AND (o.order_source_name IS NULL OR o.order_source_name = '')
    GROUP BY o.id
)

SELECT 
    SUM(tax) AS 'Tax'
FROM (
    SELECT 
        tax
    FROM 
        third_party_order_totals
    UNION ALL
    SELECT 
        tax
    FROM 
        magil_order_totals
) AS combined_orders;


-- Live Orders

select
    CONVERT_TZ (cast(concat (mo.order_date, ' ', mo.order_time) as datetime), 'UTC',l.timezone_cd) 'Order Date'
    , tt.table_name as 'Table Name'
    , case when round(TIME_TO_SEC (TIMEDIFF(CONVERT_TZ (now(), '-05:00',l.timezone_cd),CONVERT_TZ (mr.actual_dinein_time, 'UTC',l.timezone_cd)))/60) > 500 then '-' 
        else concat(round(TIME_TO_SEC (TIMEDIFF(CONVERT_TZ (now(), '-05:00',l.timezone_cd),CONVERT_TZ (mr.actual_dinein_time, 'UTC',l.timezone_cd)))/60) ,' mins')
    end as 'Table Occupancy Duration'
    , mo.order_total as '$EST Order Amount'
from mh_orders mo 
inner join mh_table_orders t on t.order_id = mo.id
inner join mh_location l on l.id = mo.location_id
inner join mh_reservation mr on mr.id = t.reservation_id
inner join mh_table tt on tt.id = t.table_id
where mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8' 
    and order_date between '2023-01-01' and '2023-12-30' 
    and mo.status_id not in (15,16,17,9,62) 
order by tt.table_name 


-- Payment mode - US


with combined_orders as (
select o.id,o.order_source_name from mh_orders o 
INNER  JOIN `mh_location` mhl ON o.`location_id` = mhl.`id` 
join `mh_order_types` t on t.`id` = o.`order_type_id` 
where date( CONVERT_TZ (cast(concat ( o.order_date, ' ',  o.order_time) as datetime), 'UTC',mhl.timezone_cd))  between '2023-06-01' and '2023-12-28'
AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8' and (
(t.type_group in ('D') and o.status_id in (15,16,17)) or 
(t.type_group in ('I') and o.status_id in (11,12,13,14,15,16,17,43)) or 
(t.type_group in ('P','S') and o.status_id in (11,12,13,43,15,16,17,6,19))
)),

maghil_orders as (
SELECT 
case when `Mh Transactions`.`tender_type` = 'CNP' then 'Online/Key-In'
    when `Mh Transactions`.`tender_type` = 'CP' then 'Card Swipe'
else `Mh Transactions`.`tender_type` end as 'Payment Mode',
count(distinct c.id) as '#Total Orders',
SUM(`Mh Transactions`.`tender_amt`) AS '#Amount Paid' 
FROM combined_orders c
INNER  JOIN `mh_transactions` `Mh Transactions` ON `Mh Transactions`.`order_id` = c.id
AND c.order_source_name is null
AND `Mh Transactions`.`status_cd` in (19,24) 
 GROUP BY 
 `Mh Transactions`.`tender_type`
ORDER BY  `#Amount Paid`  desc
LIMIT 1048576
),

third_party_orders as (
SELECT 
case when `Mh Transactions`.`tender_type` = 'CNP' then 'Online/Key-In'
    when `Mh Transactions`.`tender_type` = 'CP' then 'Card Swipe'
else `Mh Transactions`.`tender_type` end as 'Payment Mode',
count(distinct c.id) as '#Total Orders',
SUM((mo.quantity * i.sell_price) + (mo.quantity * i.sell_price * (tpo.rate/100))) AS '#Amount Paid' 
FROM combined_orders c
join mh_order_items mo on mo.order_id = c.id
join mh_items i on i.id = mo.item_id
join mh_taxfees tpo on tpo.location_id = i.`location_id`
INNER  JOIN `mh_transactions` `Mh Transactions` ON `Mh Transactions`.`order_id` = c.id
AND `Mh Transactions`.`status_cd` in (19,24) 
AND c.order_source_name is not null
and tpo.is_default = 1
and tpo.is_enabled = 1
 GROUP BY 
 `Mh Transactions`.`tender_type`
ORDER BY  `#Amount Paid`  desc
LIMIT 1048576
)

select * from maghil_orders 
union all 
select * from third_party_orders
order by `#Amount Paid`  desc


-- Order Summary - US

SELECT
    CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd) AS `Order`,
    GROUP_CONCAT(DISTINCT CONCAT(tt.`tender_type`, ' ',
            CASE WHEN tt.tender_amt > 0.0 THEN TRUNCATE(tt.tender_amt, 2) ELSE TRUNCATE(tt.transaction_amt, 2) END)
            ORDER BY tt.tender_type SEPARATOR '\n') AS `Payment Mode`,
    ROUND(TIME_TO_SEC(TIMEDIFF(mr.actual_dineout_time, mr.actual_dinein_time)) / 60) AS `Table Occupancy Duration`,
    GROUP_CONCAT(DISTINCT CONCAT(s.section_name, ' - ', t.table_name) ORDER BY s.section_name SEPARATOR '\n') AS `Section Name`,
    CASE
        WHEN ot.type_group = 'S' THEN CASE WHEN mo.order_source_name IS NOT NULL THEN CONCAT(mo.order_source_name, ' Delivery') ELSE ot.type_name END
        ELSE ot.type_name
    END AS `Order Type`,
    CASE WHEN (mo.customer_id <> '') THEN 'ONLINE' ELSE 'MERCHANT' END AS `Channel`,
    SUM(mhto.no_of_guests) * COUNT(DISTINCT mhto.table_id) / COUNT(*) AS `Guest`,
    mo.order_total AS `Order Total`,
    CASE
        WHEN mo.order_no = '' THEN SUBSTRING(JSON_UNQUOTE(JSON_EXTRACT(mo.order_source_detail, '$.orderNo')), 2, LENGTH(JSON_UNQUOTE(JSON_EXTRACT(mo.order_source_detail, '$.orderNo'))) - 2)
        ELSE mo.order_no
    END AS `Order No`,
    mo.full_name AS `Customer Name`,
    mo.order_date AS `Order Date`,
    mo.location_id AS `Location_id`,
    CASE
        WHEN (mo.phone <> '' AND mo.phone IS NOT NULL)
            THEN CASE
                    WHEN mhl.country_cd = 'IN' THEN CASE
                            WHEN SUBSTRING(mo.phone, 1, 4) = '+91-' THEN mo.phone ELSE CONCAT('+91-', mo.phone)
                         END
                    ELSE CASE
                            WHEN SUBSTRING(mo.phone, 1, 3) = '+1-' THEN mo.phone ELSE CONCAT('+1-', mo.phone)
                         END
                 END
            ELSE mo.phone
    END AS `Contact Number`,
    GROUP_CONCAT(DISTINCT CONCAT(moi.`item_name`, ' (Qty-', moi.`quantity`, ')') ORDER BY moi.`item_name` SEPARATOR '\n') AS `Item Details`
FROM
    mh_orders mo
INNER JOIN mh_order_items moi ON moi.order_id = mo.id
LEFT OUTER JOIN mh_items mi ON moi.item_id = mi.id
LEFT JOIN mh_table_orders mhto ON mo.id = mhto.order_id
LEFT JOIN mh_reservation mr ON mr.id = mhto.reservation_id
LEFT JOIN mh_transactions tt ON tt.order_id = mo.id AND tt.tender_type NOT IN ('POS', 'POD')
LEFT JOIN mh_table t ON t.id = mhto.table_id
LEFT JOIN mh_section s ON s.id = t.section_id
INNER JOIN mh_order_types ot ON ot.id = mo.order_type_id
INNER JOIN mh_location mhl ON mo.location_id = mhl.id
INNER JOIN `mh_status` `Mh Status` ON mo.`status_id` = `Mh Status`.`status_id`
WHERE
    mo.status_id IN (11, 12, 13, 14, 15, 16, 17, 25, 59, 19, 60, 43)
    AND mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
    AND DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) BETWEEN '2023-06-01' AND '2023-12-28'
GROUP BY `Order`, `Table Occupancy Duration`, `Order Type`, `Channel`, `Order Total`, `Order No`, `Customer Name`, `Order Date`, `Location_id`, `Contact Number`;



-- Category - US
SELECT
    mc.name AS 'Category',
    mo.order_date AS 'Order Date',
    mo.location_id AS 'Location ID',
    SUM(moi.quantity) AS 'Items sold',
    SUM(CASE 
        WHEN ANY_VALUE(mo.order_source_name) IS NOT NULL THEN (moi.quantity * mi.sell_price)
        ELSE moi.subtotal
    END) AS 'Total Sales'
FROM
    mh_orders mo
JOIN
    mh_order_items moi ON moi.order_id = mo.id
JOIN
    mh_item_category mio ON moi.item_id = mio.item_id
JOIN
    mh_order_types t ON t.id = mo.order_type_id
JOIN
    mh_location mhl ON mhl.id = mo.location_id
JOIN
    mh_categories mc ON mc.id = mio.category_id
JOIN
    mh_items mi ON mi.id = moi.item_id
WHERE
    mc.parent_id IS NULL
    AND mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
    AND DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) BETWEEN '2023-06-01' AND '2023-12-28'
    AND (
        (t.type_group IN ('D') AND mo.status_id IN (15,16,17)) OR
        (t.type_group IN ('I') AND mo.status_id IN (15,16,17)) OR
        (t.type_group IN ('P','S') AND mo.status_id IN (11,12,13,14,15,16,17,43))
    )
    AND mc.category_type = 'R'
GROUP BY
    mio.category_id, mo.order_date, mo.location_id

UNION ALL

SELECT
    (SELECT name FROM mh_categories WHERE id = mc.parent_id) AS 'Category',
    mo.order_date AS 'Order Date',
    mo.location_id AS 'Location ID',
    SUM(moi.quantity) AS 'Items sold',
    SUM(CASE 
        WHEN ANY_VALUE(mo.order_source_name) IS NOT NULL THEN (moi.quantity * mi.sell_price)
        ELSE moi.subtotal
    END) AS 'Total Sales'
FROM
    mh_orders mo
JOIN
    mh_order_items moi ON moi.order_id = mo.id
JOIN
    mh_item_category mio ON moi.item_id = mio.item_id
JOIN
    mh_order_types t ON t.id = mo.order_type_id
JOIN
    mh_location mhl ON mhl.id = mo.location_id
JOIN
    mh_categories mc ON mc.id = mio.category_id
JOIN 
    mh_items mi ON mi.id = moi.item_id
WHERE
    (
        (t.type_group IN ('D') AND mo.status_id IN (15,16,17)) OR
        (t.type_group IN ('I') AND mo.status_id IN (11,12,15,16,17)) OR
        (t.type_group IN ('P','S') AND mo.status_id IN (19,11,12,13,14,15,16,17))
    )
    AND mc.parent_id IS NOT NULL
    AND mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
    AND DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) BETWEEN '2023-06-01' AND '2023-12-28'
    AND mc.category_type = 'R'
GROUP BY
    mc.parent_id, mo.order_date, mo.location_id

    
    
-- $Revenue Class

SELECT  
    mht.tag_name AS 'Revenue Class',
    SUM(moi.quantity) AS 'Items sold',
    CASE
        WHEN max(mo.order_source_name) IS NOT NULL THEN
            SUM(moi.quantity*mi.sell_price)
        ELSE
            SUM(moi.subtotal)
    END AS 'Total Sales'
FROM 
    mh_orders mo 
JOIN 
    mh_location mhl ON mhl.id = mo.location_id
JOIN 
    mh_order_items moi ON moi.order_id = mo.id
JOIN 
    mh_item_tags mhit ON moi.item_id = mhit.item_id
JOIN 
    mh_tags mht ON mht.id = mhit.tag_id
JOIN
    mh_items mi ON mi.id = moi.item_id
WHERE 
    mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'  
    AND mo.status_id IN (11,12,13,14,43,15,16,17)
    AND DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) BETWEEN '2023-06-01' AND '2023-12-28'
    AND mht.is_revenue = 1
GROUP BY 
    mht.tag_name
ORDER BY 
    mht.tag_name
    
    -- Print Summary
    
select mo.order_no as 'Order no', ki.sort_order 'KOT no' , i.name as 'Item name',  ki.quantity 'Quantity',
CONVERT_TZ (ki.time_in, 'UTC',l.timezone_cd) 'Time In'
from mh_order_kot_items ki
join mh_orders mo on mo.id = ki.order_id
join mh_items i on i.id = ki.item_id
join mh_location l on l.id = mo.location_id
where mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8' and  date( CONVERT_TZ (cast(concat (mo.order_date, ' ', mo.order_time) as datetime), 'UTC',l.timezone_cd))  between '2023-06-01' and '2023-12-28'  and mo.status_id !='9'
order by CONVERT_TZ (ki.time_in, 'UTC',l.timezone_cd) desc


-- Discount summary

select 
 o.order_no as 'Order no', t.table_name as 'Table name', ordt.value as '#Discount amount'
 from mh_order_totals ordt
inner join mh_orders o on  o.id = ordt.order_id 
inner join mh_table_orders tos on tos.order_id = o.id
inner join mh_table t on t.id = tos.table_id
where
 o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8' and o.status_id in (15,16,17)
and o.order_date between '2023-06-06' and '2023-12-28' and  ordt.value >0
and  ordt.title = 'Discount' 
order by o.order_no desc

-- Cancel Item Tracker

select o.order_no as 'Order no',
s.full_name  as 'Steward',
case when ot.type_group in( 'P','I') then ot.type_name 
when ot.type_group = 'D' then ot.type_name
when ot.type_group = 'S' then ot.type_name
else '-' end as 'Order type',
i.name 'Item name', oih.updated_quantity 'Refunded Quantity', oih.subtotal 'Amount',
 oih.cancel_reason as 'Reason',
oih.created_time as 'Time'
from mh_order_items_history oih 
join mh_staff s on s.id = oih.staff_id
join mh_orders o on o.id = oih.order_id
join mh_items i on i.id= oih.item_id
join mh_order_types ot on ot.id = o.order_type_id
join mh_location l on l.id=o.location_id
where  o.location_id='0abe0c6e-88b0-45a3-921c-bb9662eac0b8' and
o.order_date between '2023-06-01' and '2023-12-28' 
 and oih.action in ('SUBTRACT' ,'REMOVED')
order by o.order_no desc, CONVERT_TZ (oih.created_time, 'UTC',l.timezone_cd)  

-- Product summary - US

SELECT 
    `mh_items`.`name` AS 'Product name', 
    SUM(`mh_order_items`.quantity) AS `Quantity`,
    `mh_items`.sell_price AS 'Price',
    CASE 
        WHEN max(`mh_orders`.`order_source_name`) IS NOT NULL THEN
            SUM(`mh_order_items`.quantity * mi.sell_price)
        ELSE
            SUM(`mh_order_items`.subtotal)
    END AS 'Total Product Sales'
FROM 
    `mh_orders`
INNER JOIN 
    `mh_location` mhl ON `mh_orders`.`location_id` = mhl.`id`
INNER JOIN 
    `mh_order_items` ON `mh_orders`.`id` = `mh_order_items`.`order_id`
inner join 
    `mh_items` on  `mh_items`.id =  `mh_order_items`.item_id
INNER JOIN 
    `mh_order_types` t ON t.id = `mh_orders`.order_type_id
JOIN
    mh_items mi ON mi.id = `mh_order_items`.item_id
WHERE  
    DATE(CONVERT_TZ(CAST(CONCAT(`mh_orders`.`order_date`, ' ', `mh_orders`.`order_time`) AS DATETIME), 'UTC', mhl.timezone_cd)) 
    BETWEEN '2023-06-01' AND '2023-12-28'
    AND (
        (t.type_group IN ('D') AND `mh_orders`.status_id IN (15,16,17)) OR 
        (t.type_group IN ('I') AND `mh_orders`.status_id IN (15,16,17)) OR 
        (t.type_group IN ('P','S') AND `mh_orders`.status_id IN (11,12,13,14,19,15,16,17))
    )
    AND `mh_orders`.`location_id` = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
GROUP BY 
    `mh_items`.`name`, 
    `mh_items`.sell_price
ORDER BY 
    SUM(`mh_order_items`.quantity * `mh_order_items`.price) DESC 
LIMIT 1048576
