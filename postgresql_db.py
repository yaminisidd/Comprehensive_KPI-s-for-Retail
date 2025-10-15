import logging
import json
import psycopg2
import os


class PostgreSQL:

    def __init__(self) -> None:
        template_file = open(os.path.join(os.getcwd(), "blueprints", "postgresql_template.json"))
        self.db_template = json.load(template_file)

    def connect_to_db(self):
        self.conn = psycopg2.connect(
            database=self.db_template["db_name"],
            user=self.db_template["user"],
            password=self.db_template["password"],
            host=self.db_template["host"]
        )
        cursor = self.conn.cursor()
        return cursor

    def execute_query(self, cursor, query):
        cursor.execute(query)
        result = cursor.fetchall()
        self.conn.commit()
        self.conn.close()
        return result
