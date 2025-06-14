from flask import Flask, jsonify #to jasonfy the databse code
import mysql.connector # mysql connector

app = Flask(__name__)

# Database connection configuration
db_config = {
    'host': 'dbd.magilhub.com',
    'user': 'mhdsvc',
    'password': 'H65nGYc7',
    'database': 'mhd'
}

# Example route to fetch total order count processed for US
@app.route('/api/total_orders_processed_us', methods=['GET'])
def get_total_orders_processed_us():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # SQL query to fetch total order count processed for US
        query = """
            SELECT COUNT(o.id) #selection of the content of the connection for the database
            FROM mh_orders o #the data is fetched from the data
            JOIN mh_order_types t ON t.id = o.order_type_id # kathai le 
            JOIN mh_location mhl ON mhl.id = o.location_id
            WHERE (
                (t.type_group IN ('D') AND o.status_id IN (15,16,17)) OR
                (t.type_group IN ('I') AND o.status_id IN (11,12,13,14,15,16,17)) OR
                (t.type_group IN ('P','S') AND o.status_id IN (11,12,13,14,43,15,16,17))
            )
            AND DATE(CONVERT_TZ(CAST(CONCAT(o.order_date, ' ', o.order_time) AS DATETIME), 'UTC', mhl.timezone_cd))
                BETWEEN '2023-06-01' AND '2023-12-28'
            AND o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
        """
        cursor.execute(query)
        total_orders = cursor.fetchone()[0]  # Fetch the first column value from the result

        conn.close()

        # Format fetched data as JSON
        json_data = {'total_orders_processed_us': total_orders}

        return jsonify(json_data), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
