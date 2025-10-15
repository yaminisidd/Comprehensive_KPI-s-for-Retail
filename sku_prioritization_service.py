from utils.postgresql_db import PostgreSQL
import logging
import json

class SkuPrioritizationService:

    def __init__(self) -> None:
        pass

    def get_top_performing_skus(self, blueprint):
        try:
            logging.info("sku prioritization - top performing skus invoked...")
            db = PostgreSQL()
            sql_query = blueprint["sku_prioritization"]["top_performing"]["query"]
            cursor = db.connect_to_db()
            result = db.execute_query(cursor, sql_query)
            response_data = []
            for row in result:
                output = {
                    "product_id": row[0],
                    "demand_qty": json.loads(row[1])['total_demand_qty'],
                    "score": row[2],
                    "category": row[3]
                }
                response_data.append(output)
            return response_data
        except Exception as e:
            logging.error(e)
            raise

    def get_top_performing_skus_count(self, blueprint):
        try:
            logging.info("sku prioritization - top performing skus count invoked...")
            db = PostgreSQL()
            sql_query = blueprint["sku_prioritization"]["top_performing_sku_count"]["query"]
            cursor = db.connect_to_db()
            result = db.execute_query(cursor, sql_query)
            return {"total_top_performing_skus": result[0][0]}
        except Exception as e:
            logging.error(e)
            raise

    def all_prioritization_scores(self, blueprint):
        try:
            logging.info("sku prioritization - all scores invoked...")
            db = PostgreSQL()
            sql_query = blueprint["sku_prioritization"]["all_priority_score"]["query"]
            cursor = db.connect_to_db()
            result = db.execute_query(cursor, sql_query)
            response_data = []
            for row in result:
                output = {
                    "product_id": row[0],
                    "demand_qty": json.loads(row[1])['total_demand_qty'],
                    "score": row[2],
                    "category": row[3]
                }
                response_data.append(output)
            return response_data
        except Exception as e:
            raise