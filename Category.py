import mysql.connector
from mysql.connector import Error
from flask import Flask, jsonify, Response
import json
from datetime import date, datetime
from decimal import Decimal
from collections import OrderedDict

app = Flask(__name__)

def execute_mysql_query(query):
    try:
        connection = mysql.connector.connect(
            host='dbd.magilhub.com',
            database='mhd',
            user='mhdsvc',
            password='H65nGYc7'
        )

        if connection.is_connected():
            print('Connected to MySQL database')
            cursor = connection.cursor(dictionary=True)

            # Print the query for debugging purposes
            print(f"Executing query:\n{query}")
            cursor.execute(query)
            result = cursor.fetchall()

            cursor.close()
            connection.close()
            print('MySQL connection is closed')

            return result

    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

@app.route('/Category_data', methods=['GET'])
def get_data():
    try:
        data = {}

        queries = [
            {
                'query': """
                WITH cte AS (
                    SELECT 
                        c.name AS catg_name,
                        i.name AS item_name,
                        (SUM(moi.quantity) * i.sell_price) AS gross_sales,
                        SUM(moi.quantity) AS qty,
                        MAX(50) AS seq,
                        o.location_id,
                        CONVERT_TZ(CONCAT(o.order_date, ' ', o.order_time), 'UTC', 
                            CASE 
                                WHEN mhl.timezone_cd = 'IST' THEN '+05:30' 
                                WHEN mhl.timezone_cd = 'America/Chicago' THEN '-05:00' 
                                ELSE '-04:00' 
                            END) AS converted_order_datetime,
                        DATE(CONVERT_TZ(CONCAT(o.order_date, ' ', o.order_time), 'UTC', 
                            CASE 
                                WHEN mhl.timezone_cd = 'IST' THEN '+05:30' 
                                WHEN mhl.timezone_cd = 'America/Chicago' THEN '-05:00' 
                                ELSE '-04:00' 
                            END)) AS order_date
                    FROM mh_orders o
                    JOIN mh_order_items moi ON moi.order_id = o.id
                    JOIN mh_item_category ic ON ic.item_id = moi.item_id
                    JOIN mh_categories c ON c.id = ic.category_id
                    JOIN mh_items i ON i.id = moi.item_id
                    JOIN mh_order_types t ON t.id = o.order_type_id
                    JOIN mh_location mhl ON o.location_id = mhl.id
                    WHERE mhl.id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
                    AND (
                        (t.type_group IN ('D') AND o.status_id IN (15, 16, 17)) OR
                        (t.type_group IN ('I') AND o.status_id IN (11, 12, 13, 14, 15, 16, 17, 43)) OR
                        (t.type_group IN ('P', 'S') AND o.status_id IN (15, 16, 17, 11, 19, 43, 12, 13, 14))
                    ) 
                    AND c.category_type = 'R'
                    AND DATE(CONVERT_TZ(CONCAT(o.order_date, ' ', o.order_time), 'UTC', 
                            CASE 
                                WHEN mhl.timezone_cd = 'IST' THEN '+05:30' 
                                WHEN mhl.timezone_cd = 'America/Chicago' THEN '-05:00' 
                                ELSE '-04:00' 
                            END)) BETWEEN '2022-06-01' AND '2024-12-28'
                    GROUP BY c.name, i.name, i.sell_price, o.location_id, converted_order_datetime, order_date, o.order_time
                ),

                cte1 AS (
                    SELECT 
                        SUM(gross_sales) AS totalSales
                    FROM cte
                ),

                cte2 AS (
                    SELECT 
                        catg_name,
                        'Category Total' AS item_name,
                        SUM(gross_sales) AS catgSales,
                        SUM(qty) AS catgQty,
                        MAX(100) AS seq,
                        location_id,
                        order_date
                    FROM cte
                    GROUP BY catg_name, location_id, order_date
                ),

                cte3 AS (
                    SELECT 
                        'Total' AS catg_name,
                        'Total' AS item_name,
                        SUM(gross_sales) AS catgSales,
                        SUM(qty) AS catgQty,
                        MAX(1000) AS seq,
                        NULL AS location_id,
                        NULL AS order_date
                    FROM cte
                )

                SELECT 
                    a.catg_name AS 'Category Name',
                    a.item_name AS 'Item Name',
                    a.gross_sales AS 'Gross Sales',
                    a.qty AS 'Sold',
                    a.netSales AS '%Net Sales',
                    a.location_id AS 'Location ID',
                    a.order_date AS 'Order Date'
                FROM (
                    SELECT 
                        cte.catg_name,
                        cte.item_name,
                        cte.gross_sales,
                        cte.qty,
                        CONCAT(ROUND(((cte.gross_sales / cte1.totalSales) * 100), 2), '%') AS netSales,
                        cte.seq,
                        cte.location_id,
                        cte.order_date
                    FROM cte
                    CROSS JOIN cte1

                    UNION ALL

                    SELECT 
                        cte2.catg_name,
                        cte2.item_name,
                        cte2.catgSales AS gross_sales,
                        cte2.catgQty AS qty,
                        CONCAT(ROUND(((cte2.catgSales / cte1.totalSales) * 100), 2), '%') AS netSales,
                        cte2.seq,
                        cte2.location_id,
                        cte2.order_date
                    FROM cte2
                    CROSS JOIN cte1

                    UNION ALL 

                    SELECT 
                        cte3.catg_name,
                        cte3.item_name,
                        cte3.catgSales AS gross_sales,
                        cte3.catgQty AS qty,
                        CONCAT(ROUND(((cte3.catgSales / cte1.totalSales) * 100), 2), '%') AS netSales,
                        cte3.seq,
                        cte3.location_id,
                        cte3.order_date
                    FROM cte3
                    CROSS JOIN cte1
                ) a
                WHERE 1=1
                -- You can uncomment and adjust the following line to filter specific categories or items
                -- AND ((a.catg_name LIKE '%Accompaniments%') OR (a.item_name LIKE '%Accompaniments%'))
                ORDER BY catg_name, seq, gross_sales DESC;
                """,
                'title': 'Category Table'
            }
        ]

        for query_info in queries:
            result = execute_mysql_query(query_info['query'])
            if result is None:
                return jsonify({"error": "Error executing MySQL query"}), 500

            if query_info['title'] == 'Category Table':
                data['Category Table'] = []  # Initialize as a list
                for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["Order Date"] = row["Order Date"]
                    mode["data"] = {
                        "Category Name": row["Category Name"],
                        "Item Name": row["Item Name"],
                        "Gross Sales": row["Gross Sales"],
                        "Sold": row["Sold"],
                        "%Net Sales": row["%Net Sales"]
                    }
                    data['Category Table'].append(mode)

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
        elif isinstance(o, OrderedDict):
            return {key: self.default(value) for key, value in o.items()}
        else:
            return super().default(o)
 
app.json_encoder = CustomEncoder

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
