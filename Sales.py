import mysql.connector
from mysql.connector import Error
from flask import Flask, jsonify,Response
from datetime import date, datetime
from decimal import Decimal
from collections import OrderedDict
import json

app = Flask(__name__)

# Function to connect to MySQL database and execute query
def execute_mysql_query(query):
    try:
        # Connect to MySQL database
        connection = mysql.connector.connect(
            host='dbd.magilhub.com',
            database='mhd',
            user='mhdsvc',
            password='H65nGYc7'
        )

        if connection.is_connected():
            print('Connected to MySQL database')

            # Create cursor to execute queries
            cursor = connection.cursor(dictionary=True)

            # Print the query for debugging purposes
            print(f"Executing query:\n{query['query']}")

            # Execute query
            cursor.execute(query['query'])

            # Fetch all rows
            result = cursor.fetchall()

            cursor.close()
            connection.close()
            print('MySQL connection is closed')

            return result

    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

@app.route('/data', methods=['GET'])
def get_data():
    try:
        data = {}

        queries = [
            {
                'query': """
                 SELECT  
                    mht.tag_name AS 'Revenue Class',
                    SUM(moi.quantity) AS 'Items sold',
                    SUM(moi.subtotal) AS 'Total Sales',
                    DATE(mht.created_time) AS 'Order Date',
                    mo.location_id AS 'Location ID'
                FROM mh_orders mo 
                JOIN mh_location mhl ON mhl.id = mo.location_id
                JOIN mh_order_items moi ON moi.order_id = mo.id
                JOIN mh_item_tags mhit ON moi.item_id = mhit.item_id
                JOIN mh_tags mht ON mht.id = mhit.tag_id 
                WHERE mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'  
                    AND mo.status_id IN (11, 12, 13, 14, 43, 15, 16, 17)
                    AND DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) BETWEEN '2022-06-01' AND '2024-12-28'
                    AND mht.is_revenue = 1
                GROUP BY mht.tag_name, DATE(mht.created_time), mo.location_id
                ORDER BY SUM(moi.subtotal) DESC, mht.tag_name;

                """,
                'title': 'Revenue Class'
            },
            {
                'query': """
                -- Live Orders
                SELECT 
    o.location_id AS 'Location ID',
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
    AND DATE(CONVERT_TZ(CAST(CONCAT(o.order_date, ' ', o.order_time) AS DATETIME), 'UTC', 'America/Chicago')) BETWEEN '2022-06-01' AND '2024-12-28'
    AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
GROUP BY 
    o.location_id, o.order_date, o.id, t.table_name;

                """,
                'title': 'Live Orders'
            },
            {
                 
                 'query':"""
                 
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
                 """,
                 'title': "Category Summary"
                
                
            },
            {
                'query': """
                -- Payment Mode
                WITH combined_orders AS (
    SELECT 
        o.id,
        o.order_source_name,
        o.location_id,
        o.order_date
    FROM 
        mh_orders o 
    INNER JOIN 
        mh_location mhl ON o.location_id = mhl.id 
    JOIN 
        mh_order_types t ON t.id = o.order_type_id 
    WHERE 
        CONVERT_TZ(CONCAT(o.order_date, ' ', o.order_time), 'UTC', mhl.timezone_cd) BETWEEN '2022-06-01' AND '2024-12-28'
        AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
        AND (
            (t.type_group = 'D' AND o.status_id IN (15, 16, 17)) OR 
            (t.type_group = 'I' AND o.status_id IN (11, 12, 13, 14, 15, 16, 17, 43)) OR 
            (t.type_group IN ('P', 'S') AND o.status_id IN (11, 12, 13, 43, 15, 16, 17, 6, 19))
        )
),
maghil_orders AS (
    SELECT 
        c.location_id,
        c.order_date,
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
        c.location_id, c.order_date, mt.tender_type
),
third_party_orders AS (
    SELECT 
        c.location_id,
        c.order_date,
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
        c.location_id, c.order_date, mt.tender_type
)

SELECT 
    location_id,
    order_date,
    Payment_Mode,
    Total_Orders,
    Amount_Paid
FROM 
    maghil_orders
UNION ALL
SELECT 
    location_id,
    order_date,
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
                """,
                'title': 'Order Info'
            },
            {
                'query': """
                -- Product Summary
                SELECT 
    mi.name AS 'Product name',
    mo.order_date AS 'Order Date',
    mo.location_id AS 'Location ID',
    SUM(moi.quantity) AS 'Quantity',
    ROUND(SUM(moi.quantity * mi.sell_price), 2) AS 'Price',
    ROUND(SUM(moi.quantity * mi.sell_price), 2) AS 'Total Product Sales'
FROM 
    mh_orders mo 
JOIN 
    mh_order_items moi ON moi.order_id = mo.id
JOIN 
    mh_items mi ON mi.id = moi.item_id
WHERE 
    mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
    AND mo.status_id IN (11, 12, 13, 14, 15, 16, 17, 19, 43)
    AND DATE(CONVERT_TZ(CONCAT(mo.order_date, ' ', mo.order_time), 'UTC', 'America/Chicago')) BETWEEN '2022-06-01' AND '2024-12-28'
GROUP BY 
    mi.name, mo.order_date, mo.location_id;
                """,
                'title': 'Product Summary'
            },
            {
              'query': """
              -- Discount Summary
SELECT 
    o.order_no AS `Order no`, 
    mo.order_date AS `Order Date`,
    t.table_name AS `Table name`, 
    ordt.value AS `#Discount amount`,
    mo.location_id AS `Location ID`
FROM 
    mh_order_totals ordt
INNER JOIN 
    mh_orders o ON o.id = ordt.order_id 
INNER JOIN 
    mh_table_orders tos ON tos.order_id = o.id
INNER JOIN 
    mh_table t ON t.id = tos.table_id
INNER JOIN 
    mh_orders mo ON mo.id = o.id
WHERE
    o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8' 
    AND o.status_id IN (15, 16, 17)
    AND mo.order_date BETWEEN '2022-06-01' AND '2024-12-28' 
    AND ordt.value > 0
    AND ordt.title = 'Discount' 
ORDER BY 
    o.order_no DESC;
              """,
              'title': 'Discount Summary'
            },
            {
                'query':"""
               SELECT 
    mo.order_no AS 'Order no', 
    mo.order_date AS 'Order Date',
    ki.sort_order AS 'KOT no',
    i.name AS 'Item name',
    ki.quantity AS 'Quantity',
    CONVERT_TZ(ki.time_in, 'UTC', l.timezone_cd) AS 'Time In',
    mo.location_id AS 'Location ID'
FROM 
    mh_order_kot_items ki
JOIN 
    mh_orders mo ON mo.id = ki.order_id
JOIN 
    mh_items i ON i.id = ki.item_id
JOIN 
    mh_location l ON l.id = mo.location_id
WHERE 
    mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
    AND DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', l.timezone_cd)) BETWEEN '2022-06-01' AND '2024-12-28'
    AND mo.status_id != '9'
ORDER BY 
    CONVERT_TZ(ki.time_in, 'UTC', l.timezone_cd) DESC;
                """,
                'title':'Print Summary'
            },
            {
              'query':"""
                    SELECT 
    o.order_no AS `Order no`,
    s.full_name AS `Steward`,
    CASE 
        WHEN ot.type_group IN ('P', 'I', 'D', 'S') THEN ot.type_name
        ELSE '-'
    END AS `Order type`,
    i.name AS `Item name`,
    oih.updated_quantity AS `Refunded Quantity`,
    oih.subtotal AS `Amount`,
    oih.cancel_reason AS `Reason`,
    CONVERT_TZ(oih.created_time, '+00:00', l.timezone_cd) AS `Time`,
    mo.order_date AS `Order Date`,
    mo.location_id AS `Location ID`
FROM 
    mh_order_items_history oih 
JOIN 
    mh_staff s ON s.id = oih.staff_id
JOIN 
    mh_orders o ON o.id = oih.order_id
JOIN 
    mh_items i ON i.id = oih.item_id
JOIN 
    mh_order_types ot ON ot.id = o.order_type_id
JOIN 
    mh_location l ON l.id = o.location_id
JOIN
    mh_orders mo ON mo.id = oih.order_id 
WHERE  
    o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
    AND o.order_date BETWEEN '2022-06-01' AND '2024-12-28'
    AND oih.action IN ('SUBTRACT', 'REMOVED')
ORDER BY 
    o.order_no DESC, 
    CONVERT_TZ(oih.created_time, '+00:00', l.timezone_cd) DESC;
  
              """,
              'title': 'Cancel Item Tracker'
                
            },
            {
                'query': """
                -- Net Sales
                SELECT 
    ROUND(SUM(moi.quantity * mi.sell_price), 2) AS 'NetSales',
    mo.order_date AS 'Order Date',
    mo.location_id AS 'Location ID'
FROM 
    mh_orders mo 
JOIN 
    mh_order_items moi ON moi.order_id = mo.id
JOIN 
    mh_items mi ON mi.id = moi.item_id
WHERE 
    mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
    AND mo.status_id IN (11, 12, 13, 14, 15, 16, 17, 19, 43)
    AND DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', 'America/Chicago')) BETWEEN '2022-06-01' AND '2024-12-28'
GROUP BY
    mo.order_date,
    mo.location_id;
                """,
                'title': 'Net Sales'
            },
            {
                'query': """
                -- Tip
                 SELECT 
    mo.order_date AS "Order_date",
    mo.location_id AS "location_id",
    CASE 
        WHEN SUM(ot.value) > 0 THEN SUM(ot.value)
        ELSE 0
    END AS "Tips"
FROM 
    mh_orders mo
JOIN 
    mh_order_types t ON t.id = mo.order_type_id 
JOIN 
    mh_location mhl ON mhl.id = mo.location_id
LEFT JOIN 
    mh_order_totals ot ON ot.order_id = mo.id
WHERE  
    (
        (t.type_group IN ('D') AND mo.status_id IN (15,16,17)) OR 
        (t.type_group IN ('I') AND mo.status_id IN (11,12,13,14,15,16,17)) OR 
        (t.type_group IN ('P','S') AND mo.status_id IN (15,16,17,11,12,13,14,43))
    ) 
    AND mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8' 
    AND DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) BETWEEN '2022-06-01' AND '2024-12-28'
GROUP BY 
    mo.order_date, 
    mo.location_id;

                """,
                'title': 'Tip-Us'
            },
            {
                'query': """
                WITH third_party_orders AS (
    SELECT 
        o.id AS orderId,
        o.order_date AS Order_date,
        o.location_id AS Location_id,
        t.rate AS taxRate
    FROM 
        mh_orders o 
    INNER JOIN 
        mh_location l ON o.location_id = l.id
    LEFT JOIN 
        mh_taxfees t ON l.id = t.location_id AND t.is_enabled = 1 AND t.is_default = 1
    WHERE  
        DATE(CONVERT_TZ(CAST(CONCAT(o.order_date, ' ', o.order_time) AS DATETIME), 'UTC', l.timezone_cd)) BETWEEN '2022-06-01' AND '2024-12-28'
        AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
        AND o.status_id IN (15,16,17,11,19,43,12,13,14)
        AND o.order_source_name IS NOT NULL
),
third_party_order_totals AS (
    SELECT 
        tpo.orderId,
        tpo.Order_date,
        tpo.Location_id,
        SUM(oi.quantity * i.sell_price * (tpo.taxRate / 100)) AS Tax
    FROM 
        third_party_orders tpo
    INNER JOIN 
        mh_order_items oi ON tpo.orderId = oi.order_id
    INNER JOIN 
        mh_items i ON oi.item_id = i.id 
    GROUP BY 
        tpo.orderId
),
magil_order_totals AS (
    SELECT 
        o.id AS orderId,
        o.order_date AS Order_date,
        o.location_id AS Location_id,
        SUM(CASE WHEN ot.code = 2 THEN ot.value ELSE 0 END) AS Tax
    FROM 
        mh_orders o 
    INNER JOIN 
        mh_location l ON o.location_id = l.id
    INNER JOIN 
        mh_order_totals ot ON o.id = ot.order_id
    WHERE  
        DATE(CONVERT_TZ(CAST(CONCAT(o.order_date, ' ', o.order_time) AS DATETIME), 'UTC', l.timezone_cd)) BETWEEN '2022-06-01' AND '2024-12-28'
        AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
        AND o.status_id IN (15,16,17,11,19,43,12,13,14)
        AND (o.order_source_name IS NULL OR o.order_source_name = '')
    GROUP BY 
        o.id
)

SELECT 
    Order_date,
    Location_id,
    SUM(Tax) AS Tax
FROM (
    SELECT 
        Order_date,
        Location_id,
        Tax
    FROM 
        third_party_order_totals
    UNION ALL
    SELECT 
        Order_date,
        Location_id,
        Tax
    FROM 
        magil_order_totals
) AS combined_orders
GROUP BY 
    Order_date,
    Location_id;

                """,
                'title': 'Tax-Us'
            },
            {
                'query': """
                -- Sales Data
                SELECT 
    mo.order_date AS Order_date,
    mo.location_id AS Location_id,
    ROUND(SUM(moi.quantity * mi.sell_price), 2) AS SalesData
FROM 
    mh_orders mo 
JOIN 
    mh_order_items moi ON moi.order_id = mo.id
JOIN 
    mh_items mi ON mi.id = moi.item_id
WHERE 
    mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
    AND mo.status_id IN (11, 12, 13, 14, 15, 16, 17, 19, 43)
    AND DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', 'America/Chicago')) BETWEEN '2022-06-01' AND '2024-12-28'
GROUP BY 
    mo.order_date, mo.location_id;
                """,
                'title': 'Sales Data'
            },
            {
                'query': """
                -- Total Orders Count
                SELECT 
    o.order_date AS Order_date,
    o.location_id AS Location_id,
    COUNT(o.id) AS TotalOrdersCount
FROM 
    mh_orders o 
JOIN 
    mh_location mhl ON mhl.id = o.location_id
JOIN 
    mh_order_types t ON t.id = o.order_type_id
WHERE 
    o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
    AND (
        (t.type_group IN ('D') AND o.status_id IN (15,16,17)) OR
        (t.type_group IN ('I') AND o.status_id IN (11,12,13,14,15,16,17,43)) OR
        (t.type_group IN ('P','S') AND o.status_id IN (11,12,13,14,15,16,17,19,43))
    )
    AND DATE(CONVERT_TZ(CAST(CONCAT(o.order_date, ' ', o.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) BETWEEN '2022-06-01' AND '2024-12-28'
GROUP BY 
    o.order_date, o.location_id;

                """,
                'title': 'Total Orders Count'
            }
        ]

        for query in queries:
            result = execute_mysql_query(query)
            if result is None:
                return jsonify({"error": "Error executing MySQL query"}), 500

            if query['title'] == 'Revenue Class':
                data['Revenue Class'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["date"] = row["Order Date"]
                    mode["data"] = {
                        "Revenue Class": row["Revenue Class"],
                        "Items sold": row["Items sold"],  
                        "Total Sales": float(row["Total Sales"])  
                    }
                    data['Revenue Class'].append(mode)

            elif query['title'] == 'Live Orders':
                data['Live Orders'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["date"] = row["Order Date"]
                    mode["data"] = {
                        "Order Date": row["Order Date"],
                        "Table Name": row["Table Name"],  
                        "Table Occupancy Duration (mins)": float(row["Table Occupancy Duration (mins)"]),
                        "Estimated Order Amount":row["Table Occupancy Duration (mins)"]  
                    }
                    data['Live Orders'].append(mode)
            elif query['title'] == 'Payment Mode':
                data['Payment Mode'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["location_id"] = row["location_id"]
                    mode["date"] = row["order_date"]
                    mode["data"] = {
                        "Payment_Mode":row["Payment_Mode"],
                        "Total_Orders": row["Total_Orders"],
                        "Amount_Paid": row["Amount_Paid"] 
                    }
                    data['Payment Mode'].append(mode)

            elif query['title'] == 'Category Summary':
                data['Category Summary'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["date"] = row["Order Date"]
                    mode["data"] = {
                        "Category":row["Category"],
                        "Items sold": row["Items sold"],
                        "Total Sales":float(row["Total Sales"])
                    }
                    data['Category Summary'].append(mode)
                    
            elif query['title'] == 'Order Info':
                data['Order Info'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location_id"] = row["Location_id"]
                    mode["date"] = row["Order Date"]
                    mode["data"] = {
                        "Order":row["Order"],
                        "Payment Mode": row["Payment Mode"],
                        "Table Occupancy Duration": row["Table Occupancy Duration"],
                        "Section Name": row["Section Name"],
                        "Order Type": row["Order Type"],
                        "Channel": row["Channel"],
                        "Guest": row["Guest"],
                        "Order Total": row["Order Total"],
                        "Order No": row["Order No"],
                        "Customer Name": row["Customer Name"],
                        "Contact Number": row["Contact Number"],
                        "Item Details": row["Item Details"]
                        }
                    data['Order Info'].append(mode)

            elif query['title'] == 'Product Summary':
                data['Product Summary'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["date"] = row["Order Date"]
                    mode["data"] = {
                        "Product name":row["Product name"],
                        "Quantity": row["Quantity"],
                        "Price":float(row["Price"]),
                        "Total Product Sales":float(row["Total Product Sales"])
                        }
                    data['Product Summary'].append(mode)
                    
            elif query['title'] == 'Print Summary':
                data['Print Summary'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["date"] = row["Order Date"]
                    mode["data"] = {
                        "Order no":row["Order no"],
                        "KOT no": row["KOT no"],
                        "Item name":row["Item name"],
                        "Quantity": row["Quantity"],
                        "Time In": row["Time In"]
                        }
                    data['Print Summary'].append(mode)
                    
            elif query['title'] == 'Discount Summary':
                data['Discount Summary'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["date"] = row["Order Date"]
                    mode["data"] = {
                        "Order no":row["Order no"],
                        "Table name": row["Table name"],
                        "#Discount amount":float(row["#Discount amount"])
                    }
                    data['Discount Summary'].append(mode)
                    
            elif query['title'] == 'Cancel Item Tracker':
                data['Cancel Item Tracker'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["date"] = row["Order Date"]
                    mode["data"] = {
                        "Order no":row["Order no"],
                        "Steward": row["Steward"],
                        "Order type":row["Order type"],
                        "Item name":row["Item name"],
                        "Refunded Quantity": row["Refunded Quantity"],
                        "Amount": row["Amount"],
                        "Reason": row["Reason"]
                    }
                    data['Cancel Item Tracker'].append(mode)        

            elif query['title'] == 'Net Sales':
             data['Net Sales'] = []
             for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["date"] = row["Order Date"]
                    mode["data"] = {
                "NetSales": float(row["NetSales"]) if "NetSales" in row else 0
                   }
                    data['Net Sales'].append(mode)
                    
            elif query['title'] == 'Tip-Us':
                data['Tip-Us'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["location_id"] = row["location_id"]
                    mode["date"] = row["Order_date"]
                    mode["data"] = {
                "Tips": float(row['Tips']) if "Tips" in row else 0
                   }
                    data['Tip-Us'].append(mode)
 
            elif query['title'] == 'Tax-Us':
                data['Tax-Us'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location_id"] = row["Location_id"]
                    mode["date"] = row["Order_date"]
                    mode["data"] = {
                "Tax": float(row['Tax']) if "Tax" in row else 0
                }
                    data['Tax-Us'].append(mode)
                    
            elif query['title'] == 'Sales Data':
                data['Sales Data'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location_id"] = row["Location_id"]
                    mode["date"] = row["Order_date"]
                    mode["data"] = {
                "SalesData": float(row['SalesData']) if "SalesData" in row else 0
                }
                    data['Sales Data'].append(mode)
                    
            elif query['title'] == 'Total Orders Count':
                data['Total Orders Count'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location_id"] = row["Location_id"]
                    mode["date"] = row["Order_date"]
                    mode["data"] = {
                "TotalOrdersCount": float(row['TotalOrdersCount']) if "TotalOrdersCount" in row else 0
                }
                    data['Total Orders Count'].append(mode)
        json_response = json.dumps(data, indent=4, cls=CustomEncoder)
        return Response(json_response, mimetype='application/json')
    except Exception as e:
        print(f"Exception in get_data: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

class CustomEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        elif isinstance(o, (date, datetime)):
            return o.isoformat()
        elif isinstance(o, list):
            return [self.default(item) for item in o]  
        elif isinstance(o, OrderedDict):
            return {key: self.default(value) for key, value in o.items()}  
        elif isinstance(o, dict):
            return {key: self.default(value) for key, value in o.items()}  
        return super().default(o)

# Set custom JSON encoder globally
app.json_encoder = CustomEncoder

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
