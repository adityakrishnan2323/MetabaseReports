from flask import Flask, jsonify
import mysql.connector
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# MySQL connection using mysql.connector
db_config = {
    'host': 'dbd.magilhub.com',
    'user': 'mhdsvc',
    'password': 'H65nGYc7',
    'database': 'mhd'
}

# Function to execute MySQL queries
def execute_query(query):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)  # Fetch results as dictionaries
        cursor.execute(query)
        result = cursor.fetchall()
        conn.close()
        return result  # Return the fetched results

    except mysql.connector.Error as e:
        print(f"Error executing query: {e}")
        return None

# Define routes for API endpoints

@app.route('/api/sales', methods=['GET'])
def get_sales_data():
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
                -- Live Orders
SELECT 
    o.order_date AS 'Order Date',
    t.table_name AS 'Table Name',
    TIMESTAMPDIFF(MINUTE, o.order_date, NOW()) AS 'Table Occupancy Duration (mins)',
    FORMAT(SUM(oi.quantity * i.sell_price), 2) AS 'Estimated Order Amount'
FROM 
    mh_orders o, mh_order_items oi, mh_items i, mh_table t
WHERE 
    o.id = oi.order_id
    AND oi.item_id = i.id
    AND o.location_id = t.location_id
    AND o.status_id IN (11, 12, 13, 14)
    AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
    AND o.order_date BETWEEN '2024-06-01' AND '2024-12-28'
GROUP BY 
    o.order_date, t.table_name WITH ROLLUP;


            """,
            'title': 'Live Orders'
        },
        {
            'query': """
                -- Payment Mode
                WITH maghil_orders AS (
    SELECT 
        CASE 
            WHEN t.type_group = 'D' AND o.status_id IN (15, 16, 17) THEN 'Online/Key-In'
            WHEN t.type_group = 'I' AND o.status_id IN (11, 12, 13, 14, 15, 16, 17, 43) THEN 'Card Swipe'
            WHEN t.type_group IN ('P', 'S') AND o.status_id IN (11, 12, 13, 43, 15, 16, 17, 6, 19) THEN 'Other'
        END AS Payment_Mode,
        COUNT(DISTINCT o.id) AS Total_Orders,
        SUM(
            CASE 
                WHEN t.type_group = 'D' AND o.status_id IN (15, 16, 17) THEN mt.tender_amt
                ELSE 0
            END
        ) AS Amount_Paid
    FROM 
        mh_orders o 
    JOIN 
        mh_order_types t ON t.id = o.order_type_id 
    LEFT JOIN 
        mh_transactions mt ON mt.order_id = o.id AND mt.status_cd IN (19, 24)
    WHERE 
        CONVERT_TZ(CONCAT(o.order_date, ' ', o.order_time), 'UTC', mhl.timezone_cd) BETWEEN '2024-06-01' AND '2024-12-28'
        AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
    GROUP BY 
        Payment_Mode
),
third_party_orders AS (
    SELECT 
        CASE 
            WHEN t.type_group = 'D' AND o.status_id IN (15, 16, 17) THEN 'Online/Key-In'
            WHEN t.type_group = 'I' AND o.status_id IN (11, 12, 13, 14, 15, 16, 17, 43) THEN 'Card Swipe'
            WHEN t.type_group IN ('P', 'S') AND o.status_id IN (11, 12, 13, 43, 15, 16, 17, 6, 19) THEN 'Other'
        END AS Payment_Mode,
        COUNT(DISTINCT o.id) AS Total_Orders,
        SUM(
            CASE 
                WHEN tpo.is_default = 1 AND tpo.is_enabled = 1 THEN (mo.quantity * i.sell_price) + (mo.quantity * i.sell_price * (tpo.rate / 100))
                ELSE 0
            END
        ) AS Amount_Paid
    FROM 
        mh_orders o 
    JOIN 
        mh_order_types t ON t.id = o.order_type_id 
    LEFT JOIN 
        mh_order_items mo ON mo.order_id = o.id
    LEFT JOIN 
        mh_items i ON i.id = mo.item_id
    LEFT JOIN 
        mh_taxfees tpo ON tpo.location_id = i.location_id
    WHERE 
        CONVERT_TZ(CONCAT(o.order_date, ' ', o.order_time), 'UTC', mhl.timezone_cd) BETWEEN '2024-06-01' AND '2024-12-28'
        AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
        AND tpo.is_default = 1
        AND tpo.is_enabled = 1
    GROUP BY 
        Payment_Mode
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

            """,
            'title': 'Sales Data'
        },
        {
            'query': """
                -- Order Info
                SELECT 
                    o.id AS 'Order Number',
                    DATE_FORMAT(o.order_date, '%m-%d-%Y') AS 'Order Date',
                    o.status_id AS 'Order Status',
                    o.order_source_name AS 'Order Source',
                    o.location_id AS 'Location ID',
                    o.order_type_id AS 'Order Type ID',
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
                    o.id, o.order_date, o.status_id, o.order_source_name, o.location_id, o.order_type_id, t.table_name;
            """,
            'title': 'Order Info'
        },
        {
            'query': """
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
            'query': """
                -- Category Summary
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
        }
    ]
    results = []
    for query_obj in queries:
        query = query_obj['query']
        title = query_obj['title']
        result = execute_query(query)
        results.append({title: result})

    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)
