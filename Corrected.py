from flask import Flask, Response
import mysql.connector
from collections import OrderedDict
from datetime import datetime
import json
from decimal import Decimal
from datetime import date, datetime


app = Flask(__name__)

# Database connection details
config = {
    'host': 'dbd.magilhub.com',
    'user': 'mhdsvc',
    'port': 3306,
    'password': 'H65nGYc7',
    'database': 'mhd',
    'connection_timeout': 1000,
    'ssl_ca': 'ca.pem',
    'ssl_cert': 'server-cert.pem',
    'ssl_key': 'server-key.pem'
          }
# Queries
queries = [
    {
        'query': """
            -- Net Sales
            WITH third_party_orders AS (
                SELECT 
                    o.id AS orderId,
                    t.rate AS taxRate
                FROM 
                    mh_orders o 
                INNER JOIN 
                    mh_location l ON o.location_id = l.id
                LEFT JOIN 
                    mh_taxfees t ON l.id = t.location_id AND t.is_enabled = 1 AND t.is_default = 1
                WHERE  
                    DATE(CONVERT_TZ(CAST(CONCAT(o.order_date, ' ', o.order_time) AS DATETIME), 'UTC', 
                        CASE 
                            WHEN 'America/Chicago' = 'IST' THEN '+05:30' 
                            WHEN 'mhl.timezone_cd' = 'America/Chicago' THEN '-05:00' 
                            ELSE '-04:00' 
                        END)) BETWEEN '2024-06-01' AND '2024-12-28'
                    AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
                    AND o.order_source_name IS NOT NULL
                    AND o.status_id IN (15,16,17,11,19,43,12,13,14)
            ),
            order_totals AS (
                SELECT 
                    order_id AS orderId,
                    SUM(CASE WHEN code = 6 THEN value ELSE 0 END) AS discount
                FROM 
                    mh_order_totals
                WHERE 
                    order_id IN (SELECT orderId FROM third_party_orders)
                GROUP BY order_id
            ),
            third_party_order_totals AS (
                SELECT 
                    tpo.orderId,
                    COALESCE(ot.discount, 0) AS discount,
                    SUM(oi.quantity * i.sell_price) AS itemTotal
                FROM 
                    third_party_orders tpo
                INNER JOIN 
                    mh_order_items oi ON tpo.orderId = oi.order_id
                INNER JOIN 
                    mh_items i ON oi.item_id = i.id 
                LEFT JOIN 
                    order_totals ot ON tpo.orderId = ot.orderId
                GROUP BY tpo.orderId, ot.discount
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
                    DATE(CONVERT_TZ(CAST(CONCAT(o.order_date, ' ', o.order_time) AS DATETIME), 'UTC', 
                        CASE 
                            WHEN 'America/Chicago' = 'IST' THEN '+05:30' 
                            WHEN 'mhl.timezone_cd' = 'America/Chicago' THEN '-05:00' 
                            ELSE '-04:00' 
                        END)) BETWEEN '2024-06-01' AND '2024-12-28'
                    AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
                    AND (o.order_source_name IS NULL OR o.order_source_name = '')
                    AND o.status_id IN (15,16,17,11,19,43,12,13,14)
                GROUP BY o.id
            )
            SELECT 
                SUM(itemTotal - discount) AS NetSales
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
        """,
        'title': 'Net Sales'
    },
    {
      'query': """
      select case when sum(value)>0 then sum(value) else 0 end as 'Tips' from mh_order_totals where order_id in(select o.id from mh_orders o
 join mh_order_types t on t.id = o.order_type_id 
 join mh_location mhl on mhl.id = o.location_id
 where  (
(t.type_group in ('D') and o.status_id in (15,16,17)) or 
(t.type_group in ('I') and o.status_id in (11,12,13,14,15,16,17)) or 
(t.type_group in ('P','S') and o.status_id in (15,16,17,11,12,13,14,43))
) and 
o.location_id= '0abe0c6e-88b0-45a3-921c-bb9662eac0b8' and  date( CONVERT_TZ (cast(concat (o.order_date, ' ', o.order_time) as datetime), 'UTC',mhl.timezone_cd))  between '2023-06-01'and  '2023-12-28' ) and code=3
      """,
      'title':'Tip-Us'
    },
    {
        'query': """
            -- Live Orders
            SELECT 
                o.order_date AS 'Order Date',
                t.table_name AS 'Table Name',
                TIMESTAMPDIFF(MINUTE, o.order_date, NOW()) AS 'Table Occupancy Duration (mins)',
                SUM(oi.quantity * i.sell_price) AS 'Estimated Order Amount'
            FROM 
                mh_orders o 
            JOIN 
                mh_order_items oi ON o.id = oi.order_id
            JOIN 
                mh_items i ON oi.item_id = i.id
            JOIN 
                mh_table t ON o.location_id = t.location_id
            WHERE 
                o.status_id IN (11, 12, 13, 14)
                AND DATE(CONVERT_TZ(CAST(CONCAT(o.order_date, ' ', o.order_time) AS DATETIME), 'UTC', 'America/Chicago')) BETWEEN '2024-06-01' AND '2024-12-28'
                AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
            GROUP BY 
                o.order_date, o.id, t.table_name;
        """,
        'title': 'Live Orders'
    },
    {
        'query': """
            -- Payment Mode
            WITH combined_orders AS (
                SELECT 
                    o.id,
                    o.order_source_name
                FROM 
                    mh_orders o 
                INNER JOIN 
                    mh_location mhl ON o.location_id = mhl.id 
                JOIN 
                    mh_order_types t ON t.id = o.order_type_id 
                WHERE 
                    CONVERT_TZ(CONCAT(o.order_date, ' ', o.order_time), 'UTC', mhl.timezone_cd) BETWEEN '2024-06-01' AND '2024-12-28'
                    AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
                    AND (
                        (t.type_group = 'D' AND o.status_id IN (15, 16, 17)) OR 
                        (t.type_group = 'I' AND o.status_id IN (11, 12, 13, 14, 15, 16, 17, 43)) OR 
                        (t.type_group IN ('P', 'S') AND o.status_id IN (11, 12, 13, 43, 15, 16, 17, 6, 19))
                    )
            ),
            maghil_orders AS (
                SELECT 
                    CASE 
                        WHEN mt.tender_type = 'CNP' THEN 'Online/Key-In'
                        WHEN mt.tender_type = 'CP' THEN 'Card Swipe'
                        ELSE mt.tender_type
                    END AS Payment_Mode,
                    COUNT(DISTINCT c.id) AS Total_Orders,
                    SUM(mt.tender_amt) AS Amount_Paid 
                FROM 
                    combined_orders c
                INNER JOIN 
                    mh_transactions mt ON mt.order_id = c.id
                WHERE 
                    c.order_source_name IS NULL
                    AND mt.status_cd IN (19, 24) 
                GROUP BY 
                    mt.tender_type
                ORDER BY  
                    Amount_Paid DESC
                LIMIT 1048576
            ),
            third_party_orders AS (
                SELECT 
                    CASE 
                        WHEN mt.tender_type = 'CNP' THEN 'Online/Key-In'
                        WHEN mt.tender_type = 'CP' THEN 'Card Swipe'
                        ELSE mt.tender_type
                    END AS Payment_Mode,
                    COUNT(DISTINCT c.id) AS Total_Orders,
                    SUM((mo.quantity * i.sell_price) + (mo.quantity * i.sell_price * (tpo.rate / 100))) AS Amount_Paid 
                FROM 
                    combined_orders c
                JOIN 
                    mh_order_items mo ON mo.order_id = c.id
                JOIN 
                    mh_items i ON i.id = mo.item_id
                JOIN 
                    mh_taxfees tpo ON tpo.location_id = i.location_id
                JOIN 
                    mh_transactions mt ON mt.order_id = c.id
                WHERE 
                    mt.status_cd IN (19, 24) 
                    AND c.order_source_name IS NOT NULL
                    AND tpo.is_default = 1
                    AND tpo.is_enabled = 1
                GROUP BY 
                    mt.tender_type
                ORDER BY  
                    Amount_Paid DESC
                LIMIT 1048576
            )

            SELECT 
                'Maghil' AS Source,
                Payment_Mode,
                Total_Orders,
                Amount_Paid
            FROM 
                maghil_orders
            UNION ALL
            SELECT 
                'Third Party' AS Source,
                Payment_Mode,
                Total_Orders,
                Amount_Paid
            FROM 
                third_party_orders
            ORDER BY  
                Amount_Paid DESC
            LIMIT 1048576;
        """,
        'title': 'Payment Mode'
    },
    {
        'query': """
            -- Sales Data
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
                    DATE(CONVERT_TZ(CAST(CONCAT(o.order_date, ' ', o.order_time) AS DATETIME), 'UTC', 
                        CASE 
                            WHEN 'America/Chicago' = 'IST' THEN '+05:30' 
                            WHEN 'mhl.timezone_cd' = 'America/Chicago' THEN '-05:00' 
                            ELSE '-04:00' 
                        END)) BETWEEN '2024-06-01' AND '2024-12-28'
                    AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
                    AND o.order_source_name IS NOT NULL
                    AND o.status_id IN (15,16,17,11,19,43,12,13,14)
            ),
            order_totals AS (
                SELECT 
                    order_id AS orderId,
                    SUM(CASE WHEN code = 6 THEN value ELSE 0 END) AS discount
                FROM 
                    mh_order_totals
                WHERE 
                    order_id IN (SELECT orderId FROM third_party_orders)
                GROUP BY order_id
            ),
            third_party_order_totals AS (
                SELECT 
                    tpo.orderId,
                    COALESCE(ot.discount, 0) AS discount,
                    SUM(oi.quantity * i.sell_price) AS itemTotal
                FROM 
                    third_party_orders tpo
                INNER JOIN 
                    mh_order_items oi ON tpo.orderId = oi.order_id
                INNER JOIN 
                    mh_items i ON oi.item_id = i.id 
                LEFT JOIN 
                    order_totals ot ON tpo.orderId = ot.orderId
                GROUP BY tpo.orderId, ot.discount
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
                    DATE(CONVERT_TZ(CAST(CONCAT(o.order_date, ' ', o.order_time) AS DATETIME), 'UTC', 
                        CASE 
                            WHEN 'America/Chicago' = 'IST' THEN '+05:30' 
                            WHEN 'mhl.timezone_cd' = 'America/Chicago' THEN '-05:00' 
                            ELSE '-04:00' 
                        END)) BETWEEN '2024-06-01' AND '2024-12-28'
                    AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
                    AND (o.order_source_name IS NULL OR o.order_source_name = '')
                    AND o.status_id IN (15,16,17,11,19,43,12,13,14)
                GROUP BY o.id
            )
            SELECT 
                COALESCE(SUM(itemTotal - discount), 0) AS SalesData
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
        """,
        'title': 'Sales Data'
        
    },
    {
        'query': """
        -- Total Orders Count query 
            select count(o.id) from mh_orders o join mh_order_types t on t.id = o.order_type_id
join mh_location mhl on mhl.id = o.location_id
where  (
(t.type_group in ('D') and o.status_id in (15,16,17)) or 
(t.type_group in ('I') and o.status_id in (11,12,13,14,15,16,17)) or 
(t.type_group in ('P','S') and o.status_id in (11,12,13,14,43,15,16,17))
)
and date( CONVERT_TZ (cast(concat (o.order_date, ' ', o.order_time) as datetime), 'UTC',mhl.timezone_cd))  between '2023-06-01' and '2023-12-28'
and o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
        """,
        'title': 'Total Orders Count'
        
    },
    {
      'query': """
      SELECT
    mc.name AS 'Category',
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
    AND DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) BETWEEN '2024-06-01' AND '2024-12-28'
    AND (
        (t.type_group IN ('D') AND mo.status_id IN (15,16,17)) OR
        (t.type_group IN ('I') AND mo.status_id IN (15,16,17)) OR
        (t.type_group IN ('P','S') AND mo.status_id IN (11,12,13,14,15,16,17,43))
    )
    AND mc.category_type = 'R'
GROUP BY
    mio.category_id
UNION ALL
SELECT
    (SELECT name FROM mh_categories WHERE id = mc.parent_id) AS 'Category',
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
    AND DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) BETWEEN '2024-06-01' AND '2024-12-28'
    AND mc.category_type = 'R'
GROUP BY
    mc.parent_id
      """,
      'title': 'Category Summary'
    },
    {
      'query': """
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
        DATE(CONVERT_TZ(CAST(CONCAT(o.order_date, ' ', o.order_time) AS DATETIME), 'UTC', l.timezone_cd)) BETWEEN '2024-06-01' AND '2024-12-28'
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
      """,
      'title':'Tax-Us'
    },
    {
      'query':"""
       -- Order Info
                select 
CONVERT_TZ (cast(concat (mo.order_date, ' ', mo.order_time) as datetime), 'UTC',mhl.timezone_cd) 'Order',
group_concat( distinct(tt.`tender_type`) ,' ' , case when tt.tender_amt > 0.0 then TRUNCATE(tt.tender_amt,2) else TRUNCATE(tt.transaction_amt,2) end , ' ' SEPARATOR '\n') as 'Payment Mode',
round(TIME_TO_SEC (TIMEDIFF(mr.actual_dineout_time,mr.actual_dinein_time))/60) 'Table Occupancy Duration',
group_concat( distinct(s.section_name), ' - ', t.table_name ) as'Section Name',
case when ot.type_group = 'S' then case when mo.order_source_name is not null then CONCAT(mo.order_source_name,' Delivery') else ot.type_name end else ot.type_name end as 'Order Type',
case when (mo.customer_id<>'') then 'ONLINE' else 'MERCHANT' end as 'Channel',
sum(mhto.no_of_guests) * count(distinct mhto.table_id)/count(*) as 'Guest' ,
mo.order_total as 'Order Total', case when mo.order_no = '' then substring(json_extract(order_source_detail, '$.orderNo'),2, length(json_extract(order_source_detail, '$.orderNo'))-2) else mo.order_no end as 'Order No'-- ,mo.order_no as 'Order No'
,mo.full_name as 'Customer Name',
case when (mo.phone<>'' and mo.phone is not null) then case when mhl.country_cd = 'IN' then case when substring(mo.phone,1,4) = '+91-' then mo.phone else Concat('+91-',mo.phone) end else 
 case when substring(mo.phone,1,3) = '+1-' then mo.phone else Concat('+1-',mo.phone) end end else mo.phone end as 'Contact Number',
group_concat( distinct( moi.`item_name`) ,' (Qty-' , moi.`quantity`,') ' SEPARATOR '\n') AS `Item Details`
from mh_orders mo
inner join mh_order_items moi on moi.order_id = mo.id
left outer join mh_items mi on moi.item_id = mi.id
left join mh_table_orders mhto on mo.id = mhto.order_id
left join mh_reservation mr on mr.id = mhto.reservation_id
left join mh_transactions tt on tt.order_id = mo.id and tt.tender_type not in ('POS','POD')
left join mh_table t on t.id = mhto.table_id
left join mh_section s on s.id = t.section_id
inner join mh_order_types ot on ot.id = mo.order_type_id
inner join mh_location mhl on mo.location_id = mhl.id
INNER JOIN `mh_status` `Mh Status` ON mo.`status_id` = `Mh Status`.`status_id`
where mo.status_id in (11,12,13,14,15,16,17,25,59,19,60,43) and
mo.location_id ='0abe0c6e-88b0-45a3-921c-bb9662eac0b8' and
 date( CONVERT_TZ (cast(concat (mo.order_date, ' ', mo.order_time) as datetime), 'UTC',mhl.timezone_cd)) 
between '2023-06-01' and  '2023-12-28'
group by 1,3,5,6,8,9,10,11 
            """,
            'title':'Order Info'
    },
    {
      'query':
        """               
        -- Product Summary
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
    BETWEEN '2024-06-01' AND '2024-12-28'
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

            """,
            'title': 'Product Summary'
    },
    {
      'query': """"
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
      """,
      'title':"Revenue Class"
    }
    
    
]
def get_data(query):
    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor(dictionary=True)
        
        # Print the query for debugging purposes
        print(f"Executing query:\n{query}")
        
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        connection.close()
        return result
    except mysql.connector.Error as e:
        print(f"Error fetching data: {e}")
        return []

def process_query():
    data = {}
    for query in queries:
        result = get_data(query)
        if result:
            if query['title'] == 'Live Orders':
                data['Live Orders'] = []
                for row in result:
                    order = OrderedDict()
                    order["Order Date"] = row["Order Date"]
                    order["Table Name"] = row["Table Name"]
                    order["Table Occupancy Duration (mins)"] = row["Table Occupancy Duration (mins)"]
                    order["Estimated Order Amount"] = float(row['Estimated Order Amount'])
                    data['Live Orders'].append(order)
            elif query['title'] == 'Net Sales':
                data['Net Sales'] = float(result[0]['NetSales']) if 'NetSales' in result[0] else 0
            elif query['title'] == 'Payment Mode':
                data['Payment Mode'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Payment_Mode"] = row["Payment_Mode"]
                    mode["Total_Orders"] = row["Total_Orders"]
                    mode["Amount_Paid"] = float(row["Amount_Paid"])
                    mode["Source"] = row['Source']
                    data['Payment Mode'].append(mode)
            elif query['title'] == 'Sales Data':
                data['Sales Data'] = float(result[0]['SalesData']) if 'SalesData' in result[0] else 0
            elif query['title'] == 'Total Orders Count':
                data['Total Orders Count'] = float(result[0]['count(o.id)']) if 'count(o.id)' in result[0] else 0
            elif query['title'] == 'Category Summary':
                data['Category Summary'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Category"] = row["Category"]
                    mode["Items sold"] = row["Items sold"]
                    mode["Total Sales"] = float(row["Total Sales"])  # Convert Decimal to float
                    data['Category Summary'].append(mode)
            elif query['title']== 'Tip-Us':
              data['Tip-Us']=float(result[0]['Tips']) if 'Tips' in result[0] else 0
            elif query['title']== 'Tax-Us':
              data['Tax-Us']=float(result[0]['Tax']) if 'Tax' in result[0] else 0
            elif query['title']== 'Order Info':
              data['Order Info'] =[]
              for row in result:
                mode = OrderedDict()
                mode["Order"] = row["Order"]
                mode["Payment Mode"] = row["Payment Mode"]
                mode["Table Occupancy Duration"] = row["Table Occupancy Duration"]
                mode["Section Name"] = row["Section Name"]
                mode["Order Type"] =row["Order Type"]
                mode["Channel"] =row["Channel"]
                mode["Guest"] =row["Guest"]
                mode["Order Total"] =row["Order Total"]
                mode["Order No"]=row["Order No"]
                mode["Customer Name"]= row ["Customer Name"]
                mode["Contact Number"]=row["Contact Number"]
                mode["Item Details"]=row["Item Details"]
                data['Order Info'].append(mode)
            elif query['title'] == 'Product Summary':
                data['Product Summary'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Product name"] = row["Product name"]
                    mode["Quantity"] = row["Quantity"]  # Convert Decimal to float
                    mode["Price"]=float(row["Price"])
                    mode["Total Product Sales"]=float(row["Total Product Sales"])
                    data['Product Summary'].append(mode)
            elif query['title'] == 'Revenue Class':
                data['Revenue Class'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Revenue Class"] = row["Revenue Class"]
                    mode["Items sold"] = row["Items sold"]  # Convert Decimal to float
                    mode["Total Sales"]=float(row["Total Sales"])
                    data['Revenue Class'].append(mode)
                            
    return data

class CustomEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        elif isinstance(o, set):
            return list(o)
        return super().default(o)

@app.route('/api/all_data', methods=['GET'])
def get_all_data():
    all_data = process_query()
    json_response = json.dumps(all_data, indent=4, cls=CustomEncoder)
    return Response(json_response, mimetype='application/json')

if __name__ == '__main__':
    app.run(debug=True)