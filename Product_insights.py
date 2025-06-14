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

@app.route('/Product_data', methods=['GET'])
def get_data():
    try:
        data = {}

        queries = [
            {
                'query': """
                -- Top 20 Popular Items
                SELECT  
    `mh_order_items`.`item_name` AS 'Product name',
    SUM(`mh_order_items`.`quantity`) AS `Quantity`,
    mo.location_id AS 'Location ID',
    DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) AS 'Order Date'
FROM `mh_orders` mo
INNER JOIN `mh_location` mhl ON mo.location_id = mhl.id
INNER JOIN `mh_order_items` ON mo.id = `mh_order_items`.`order_id`
INNER JOIN `mh_order_types` mot ON mo.order_type_id = mot.id
WHERE 
    DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) BETWEEN '2022-06-01' AND '2024-12-28'
    AND (
        (mot.type_group IN ('D') AND mo.status_id IN (15, 16, 17)) OR
        (mot.type_group IN ('I') AND mo.status_id IN (15, 16, 17)) OR
        (mot.type_group IN ('P','S') AND mo.status_id IN (11, 12, 13, 14, 19, 15, 16, 17))
    )
    AND mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
GROUP BY `mh_order_items`.`item_name`, mo.location_id, DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd))
ORDER BY SUM(`mh_order_items`.`quantity`) DESC, `mh_order_items`.`item_name`
LIMIT 20;
                """,
                'title': 'Top 20 Popular Items'
            },
            {
              'query': """
              -- Top 20 Revenue Making Products 3
              SELECT 
    Category,
    location_id,
    order_date,
    SUM(ttl_sales) AS ttl_sales
FROM (
    SELECT  
        cat.name AS Category,
        mo.location_id,
        mo.order_date,
        SUM(moi.subtotal) AS ttl_sales 
    FROM mh_orders mo 
    JOIN mh_order_items moi ON moi.order_id = mo.id
    JOIN mh_item_category mc ON moi.item_id = mc.item_id 
    JOIN mh_order_types t ON t.id = mo.order_type_id
    JOIN mh_location mhl ON mhl.id = mo.location_id
    JOIN mh_categories cat ON cat.id = mc.category_id 
    WHERE 
        cat.parent_id IS NULL 
        AND mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8' 
        AND DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) BETWEEN '2022-06-01' AND '2024-12-28' 
        AND (
            (t.type_group IN ('D') AND mo.status_id IN (15,16,17)) OR 
            (t.type_group IN ('I') AND mo.status_id IN (15,16,17)) OR 
            (t.type_group IN ('P','S') AND mo.status_id IN (11,12,13,14,15,16,17,43))
        )
        AND cat.category_type = 'R'
    GROUP BY Category, mo.location_id, mo.order_date
    
    UNION ALL
    
    SELECT  
        cat_parent.name AS Category,
        mo.location_id,
        mo.order_date,
        SUM(moi.subtotal) AS ttl_sales 
    FROM mh_orders mo 
    JOIN mh_order_items moi ON moi.order_id = mo.id
    JOIN mh_item_category mc ON moi.item_id = mc.item_id 
    JOIN mh_order_types t ON t.id = mo.order_type_id
    JOIN mh_location mhl ON mhl.id = mo.location_id
    JOIN mh_categories cat ON cat.id = mc.category_id 
    JOIN mh_categories cat_parent ON cat_parent.id = cat.parent_id
    WHERE 
        cat.parent_id IS NOT NULL 
        AND mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8' 
        AND DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) BETWEEN '2022-06-01' AND '2024-12-28' 
        AND (
            (t.type_group IN ('D') AND mo.status_id IN (15,16,17)) OR 
            (t.type_group IN ('I') AND mo.status_id IN (11,12,15,16,17)) OR 
            (t.type_group IN ('P','S') AND mo.status_id IN (19,11,12,13,14,15,16,17))
        )
        AND cat.category_type = 'R'
    GROUP BY Category, mo.location_id, mo.order_date
) a 
GROUP BY Category, location_id, order_date
ORDER BY ttl_sales DESC, Category
LIMIT 20;
              """,
              'title': 'Top 20 Revenue Making Products'
              
            },
            { 
              'query': """
             SELECT  
    `mh_order_items`.`item_name` AS 'Product name',
    SUM(`mh_order_items`.`quantity`) AS `Quantity`,
    mo.location_id AS 'Location ID',
    DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) AS 'Order Date'
FROM `mh_orders` mo
INNER JOIN `mh_location` mhl ON mo.location_id = mhl.id
INNER JOIN `mh_order_items` ON mo.id = `mh_order_items`.`order_id`
INNER JOIN `mh_order_types` mot ON mo.order_type_id = mot.id
WHERE  
    DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) BETWEEN '2022-06-01' AND '2024-12-28'
    AND (
        (mot.type_group IN ('D') AND mo.status_id IN (15, 16, 17)) OR 
        (mot.type_group IN ('I') AND mo.status_id IN (15, 16, 17)) OR 
        (mot.type_group IN ('P', 'S') AND mo.status_id IN (11, 12, 13, 14, 19, 15, 16, 17))
    )
    AND mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
GROUP BY `mh_order_items`.`item_name`, mo.location_id, DATE(mot.created_time), DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd))
ORDER BY SUM(`mh_order_items`.`quantity`), `mh_order_items`.`item_name`
LIMIT 20;
 """,'title': 'Least 20 Popular Items'
  },
  {
              'query': """
              select 
    name as 'Category', 
    sum(total_sales) as ttl_sales,
    location_id as 'Location ID',
    order_date as 'Order Date'
from (
    select  
        mc.name,
        mo.location_id,
        mo.order_date,
        sum(moi.subtotal) as total_sales
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
    group by mc.name, mo.location_id, mo.order_date
    
    union all
    
    select  
        (select name from mh_categories where id=mc.parent_id) as name,
        mo.location_id,
        mo.order_date,
        sum(moi.subtotal) as total_sales
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
    group by name, mo.location_id, mo.order_date
) a 
group by name, location_id, order_date
order by ttl_sales desc, name 
limit 40;
              """,
            'title':'Top Categories'
  },
  {
    'query':
      """
      SELECT
mht.tag_name AS 'Revenue Class',
    SUM(moi.quantity) AS 'Items sold',
    SUM(moi.subtotal) AS 'Total Sales',
    DATE(mo.order_date) AS 'Order Date',
    mo.location_id AS 'Location ID'  -- Adding location_id as a new column
FROM mh_orders mo 
JOIN mh_location mhl ON mhl.id = mo.location_id
JOIN mh_order_items moi ON moi.order_id = mo.id
JOIN mh_item_tags mhit ON moi.item_id = mhit.item_id
JOIN mh_tags mht ON mht.id = mhit.tag_id 
WHERE mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'  
    AND mo.status_id IN (11, 12, 13, 14, 43, 15, 16, 17)
    AND DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) BETWEEN '2022-06-01' AND '2024-12-28'
    AND mht.is_revenue = 1
GROUP BY mht.tag_name, DATE(mo.order_date), mo.location_id  -- Include location_id in GROUP BY
ORDER BY SUM(moi.subtotal) DESC, mht.tag_name;
""",
'title': 'Revenue Classes'
  },
  {
      'query':"""
      SELECT 
    i.name,
    mo.location_id AS 'Location ID',
    DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', l.timezone_cd)) AS 'Order Date',
    SUM(oih.updated_quantity) AS void_items
FROM mh_order_items_history oih 
JOIN mh_orders mo ON mo.id = oih.order_id
JOIN mh_items i ON i.id = oih.item_id
JOIN mh_location l ON l.id = mo.location_id
WHERE  
    mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8' 
    AND DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', l.timezone_cd)) BETWEEN '2022-06-01' AND '2024-12-28' 
    AND oih.action IN ('SUBTRACT', 'REMOVED')
GROUP BY i.name, mo.location_id, DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', l.timezone_cd))
ORDER BY void_items DESC, i.name
LIMIT 100;
      """,
      'title': 'Top Voided Items'
  },
  {
    'query':"""
    SELECT 
    CASE 
        WHEN (oi.cancel_reason IS NULL OR LENGTH(TRIM(oi.cancel_reason)) = 0) THEN 'UNKNOWN' 
        ELSE UPPER(oi.cancel_reason)
    END AS cancel_rsn,
    mo.location_id AS 'Location ID',
    DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', l.timezone_cd)) AS 'Order Date',
    SUM(oih.updated_quantity) AS void_items
FROM mh_order_items_history oih
JOIN mh_orders mo ON mo.id = oih.order_id
JOIN mh_order_items oi ON oi.id = oih.order_item_id
JOIN mh_location l ON l.id = mo.location_id
WHERE 
    mo.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
    AND oih.action IN ('SUBTRACT', 'REMOVED')
    AND DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', l.timezone_cd)) BETWEEN '2022-06-01' AND '2024-12-28'
GROUP BY cancel_rsn, mo.location_id, DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', l.timezone_cd))
ORDER BY void_items DESC
LIMIT 100;
    """,
    'title': 'Top Cancel Reasons'
      
  },
  {
      'query':"""
      select 
    a.name,
    a.cancel_rsn,
    a.void_items,
    o.location_id,
    o.order_date
from (
    select 
        oih.item_name as name,
        case 
            when (oi.cancel_reason is null or length(trim(oi.cancel_reason))=0) then 'UNKNOWN' 
            else Upper(oi.cancel_reason)
        end as cancel_rsn,
        sum(oih.updated_quantity) as void_items,
        o.id as order_id  -- Added order_id for grouping by order_id
    from mh_order_items_history oih
    join mh_orders o on o.id = oih.order_id
    join mh_order_items oi on oi.id = oih.order_item_id
    join mh_location l on l.id = o.location_id
    where 
        o.location_id = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8' 
        and oih.action in ('SUBTRACT', 'REMOVED')
        and date(CONVERT_TZ(cast(concat(o.order_date, ' ', o.order_time) as datetime), 'UTC', l.timezone_cd)) between '2022-06-01' and '2024-12-28'
    group by 
        oih.item_name, 
        cancel_rsn,
        order_id  -- Grouping by order_id to get unique orders
) a
join mh_orders o on o.id = a.order_id  -- Joining with mh_orders to get location_id and order_date
order by 
    a.void_items desc
limit 100;
      """,
      'title': 'Top Item Cancel Reasons'
  },
  {
      'query':"""
      SELECT 
    `mh_order_items`.`item_name` AS 'Product name', 
    SUM(`mh_order_items`.`quantity` * `mh_order_items`.`price`) AS total_product_sales,
    mo.location_id AS 'Location ID',
    DATE(CONVERT_TZ(CAST(CONCAT(mo.order_date, ' ', mo.order_time) AS DATETIME), 'UTC', mhl.timezone_cd)) AS 'Order Date'
FROM `mh_orders` mo
INNER JOIN `mh_location` mhl ON mo.`location_id` = mhl.`id` 
INNER JOIN `mh_order_items` ON mo.`id` = `mh_order_items`.`order_id`
INNER JOIN `mh_order_types` t ON t.`id` = mo.`order_type_id`
WHERE  
    DATE(CONVERT_TZ(CAST(CONCAT(mo.`order_date`, ' ', mo.`order_time`) AS DATETIME), 'UTC', mhl.`timezone_cd`)) 
    BETWEEN '2022-06-01' AND '2024-12-28'
    AND (
        (t.`type_group` IN ('D') AND mo.`status_id` IN (15, 16, 17)) OR 
        (t.`type_group` IN ('I') AND mo.`status_id` IN (15, 16, 17)) OR 
        (t.`type_group` IN ('P', 'S') AND mo.`status_id` IN (11, 12, 13, 14, 19, 15, 16, 17))
    )
    AND mo.`location_id` = '0abe0c6e-88b0-45a3-921c-bb9662eac0b8'
GROUP BY `mh_order_items`.`item_name`, mo.location_id, mo.order_date, mo.order_time, mhl.timezone_cd
ORDER BY total_product_sales DESC, `mh_order_items`.`item_name`
LIMIT 20;
      """,
      'title': 'Least 20 Revenue Making Products'
      
  }
        ]

        for query_dict in queries:
            result = execute_mysql_query(query_dict)
            if result is None:
                return jsonify({"error": "Error executing MySQL query"}), 500
            if query_dict['title'] == 'Top 20 Popular Items':
                data['Top 20 Popular Items'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["date"] = row["Order Date"]
                    mode["data"] = {
                        "Product name": row["Product name"],
                        "Quantity": row["Quantity"]  
                    }
                    data['Top 20 Popular Items'].append(mode)
            elif query_dict['title'] == 'Top 20 Revenue Making Products':
                data['Top 20 Revenue Making Products'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["location_id"] = row["location_id"]
                    mode["order date"] = row["order_date"]
                    mode["data"] = {
                        "Category": row["Category"],
                        "ttl_sales": row["ttl_sales"]  
                    }
                    data['Top 20 Revenue Making Products'].append(mode)
            elif query_dict['title'] == 'Least 20 Popular Items':
                data['Least 20 Popular Items'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["date"] = row["Order Date"]
                    mode["data"] = {
                        "Product name": row["Product name"],
                        "Quantity": row["Quantity"]  
                    }
                    data['Least 20 Popular Items'].append(mode)
            elif query_dict['title'] == 'Top Categories':
                data['Top Categories'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["date"] = row["Order Date"]
                    mode["data"] = {
                        "Category": row["Category"],
                        "ttl_sales": row["ttl_sales"]
                        }
                    data['Top Categories'].append(mode)
            elif query_dict['title'] == 'Revenue Classes':
                data['Revenue Classes'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["date"] = row["Order Date"]
                    mode["data"] = {
                        "Revenue Class": row["Revenue Class"],
                        "Items sold": row["Items sold"],  
                        "Total Sales": float(row["Total Sales"])  
                    }
                    data['Revenue Classes'].append(mode)
            elif query_dict['title'] == 'Top Voided Items':
                data['Top Voided Items'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["date"] = row["Order Date"]
                    mode["data"] = {
                        "name": row["name"],
                        "void_items": row["void_items"]  
                    }
                    data['Top Voided Items'].append(mode)
            elif query_dict['title'] == 'Top Cancel Reasons':
                data['Top Cancel Reasons'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["date"] = row["Order Date"]
                    mode["data"] = {
                        "cancel_rsn": row["cancel_rsn"],
                        "void_items": row["void_items"]  
                    }
                    data['Top Cancel Reasons'].append(mode)
            elif query_dict['title'] == 'Top Item Cancel Reasons':
                data['Top Item Cancel Reasons'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["location_id"] = row["location_id"]
                    mode["date"] = row["order_date"]
                    mode["data"] = {
                        "name":row["name"],
                        "cancel_rsn": row["cancel_rsn"],
                        "void_items": row["void_items"]  
                    }
                    data['Top Item Cancel Reasons'].append(mode)
            elif query_dict['title'] == 'Least 20 Revenue Making Products':
                data['Least 20 Revenue Making Products'] = []
                for row in result:
                    mode = OrderedDict()
                    mode["Location ID"] = row["Location ID"]
                    mode["date"] = row["Order Date"]
                    mode["data"] = {
                        "Product name":row["Product name"],
                        "total_product_sales": row["total_product_sales"]  
                    }
                    data['Least 20 Revenue Making Products'].append(mode)                                                
                    
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

app.json_encoder = CustomEncoder

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
