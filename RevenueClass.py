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
            print(f"Executing query:\n{query['query']}")
            cursor.execute(query['query'])
            result = cursor.fetchall()

            cursor.close()
            connection.close()
            print('MySQL connection is closed')

            return result

    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

@app.route('/RevenueClass', methods=['GET'])
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
                    DATE(mht.created_time) AS 'Tag Created Date',
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
                'title': 'Revenue Classes'
            }
        ]

        for query_dict in queries:
            result = execute_mysql_query(query_dict)
            if result is None:
                return jsonify({"error": "Error executing MySQL query"}), 500

            if query_dict['title'] == 'Revenue Classes':
                data['Revenue Class'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["date"] = row["Tag Created Date"]
                    mode["data"] = {
                        "Revenue Class": row["Revenue Class"],
                        "Items sold": row["Items sold"],  
                        "Total Sales": float(row["Total Sales"])  
                    }
                    data['Revenue Class'].append(mode)

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
            return o.strftime("%m-%d-%Y")  # Format date as "MM-DD-YYYY"
        elif isinstance(o, list):
            return [self.default(item) for item in o]
        elif isinstance(o, OrderedDict):
            return {key: self.default(value) for key, value in o.items()}
        elif isinstance(o, dict):
            return {key: self.default(value) for key, value in o.items()}
        return super().default(o)

app.json_encoder = CustomEncoder

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
