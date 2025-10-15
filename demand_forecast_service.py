from utils.postgresql_db import PostgreSQL
import logging


class DemandForecastService:

    def __init__(self) -> None:
        pass

    def demand_shortfall(self, blueprint, entrytype, offset, limit):

        if entrytype == "Combined":
            logging.info("Top 10 - Demand Shortfall Analysis (In Store & Online) invoked...")
        elif entrytype == "InStore":
            logging.info("POS - Bottom 10 - Demand Shortfall Analysis invoked...")
        elif entrytype == "Online":
            logging.info("RTC & DTC - Bottom 10 Products - Demand Shortfall Analysis invoked...")
        try:
            db = PostgreSQL()

            sql_query = blueprint["demand"]["shortfall"][entrytype]["query"]
            sql_query = sql_query.replace("offset_placeholder", str(offset))
            sql_query = sql_query.replace("limit_placeholder", str(limit))

            cursor = db.connect_to_db()

            result = db.execute_query(cursor, sql_query)

            response_data = []
            for data in result:
                response_data.append({
                    blueprint["demand"]["shortfall"][entrytype]["response_template_order"][0]:
                        data[0],
                    blueprint["demand"]["shortfall"][entrytype]["response_template_order"][1]:
                        data[1],
                    blueprint["demand"]["shortfall"][entrytype]["response_template_order"][2]:
                        data[2],
                    blueprint["demand"]["shortfall"][entrytype]["response_template_order"][3]:
                        data[3]
                })

            return response_data
        except Exception as e:
            raise

    def demand_surge(self, blueprint, entrytype, offset, limit):

        if entrytype == "Combined":
            logging.info("Top 10 - Demand Surge Analysis invoked...")
        elif entrytype == "InStore":
            logging.info("POS Top 10 - Demand Surge Analysis invoked...")
        elif entrytype == "Online":
            logging.info("RTC & DTC - Top 10 - Demand Surge Analysis invoked...")
        try:
            db = PostgreSQL()

            sql_query = blueprint["demand"]["surge"][entrytype]["query"]
            sql_query = sql_query.replace("offset_placeholder", str(offset))
            sql_query = sql_query.replace("limit_placeholder", str(limit))

            cursor = db.connect_to_db()

            result = db.execute_query(cursor, sql_query)

            response_data = []
            for data in result:
                response_data.append({
                    blueprint["demand"]["surge"][entrytype]["response_template_order"][0]:
                        data[0],
                    blueprint["demand"]["surge"][entrytype]["response_template_order"][1]:
                        data[1],
                    blueprint["demand"]["surge"][entrytype]["response_template_order"][2]:
                        data[2],
                    blueprint["demand"]["surge"][entrytype]["response_template_order"][3]:
                        data[3]
                })

            return response_data
        except Exception as e:
            raise


    def actual_demand_forecast(self, blueprint, req_body):

        logging.info("Actual Demand invoked...")

        try:
            db = PostgreSQL()

            sql_query = blueprint["demand"]["actual-forecast-latest"]["query"]
            req_body["productIds"] = [str(x) for x in req_body["productIds"]]
            sql_query = sql_query.replace("productIds", str(tuple(req_body["productIds"])).replace(",)", ")"))
            sql_query = sql_query.replace("actual_weeks", str(req_body["actual_weeks"]))
            sql_query = sql_query.replace("forecasted_weeks", str(req_body["forecasted_weeks"]))

            cursor = db.connect_to_db()
            result = db.execute_query(cursor, sql_query)

            response_data = []
            for product_id in req_body["productIds"]:
                temp = {"product_id": product_id, "values": []}
                for record in result:
                    if record[0] == product_id:
                        temp["values"].append(
                            {"date": record[1].strftime('%Y-%m-%d'), "week": record[2], "value": record[3],
                             "isForecast": record[4]})
                response_data.append(temp)

            return response_data
        except Exception as e:
            raise


    def demand_forecast_compare(self, blueprint, req_body):

        logging.info("Actual Demand invoked...")

        try:
            db = PostgreSQL()

            sql_query = blueprint["demand"]["demand_forecast_compare"]["query"]
            req_body["productIds"] = [str(x) for x in req_body["productIds"]]

            # Formatting the placeholders with actual data
            formatted_query = sql_query.format(
                productIds=",".join(f"'{id}'" for id in req_body["productIds"]),
                actual_weeks=req_body["actual_weeks"],
                forecasted_weeks=req_body["forecasted_weeks"]
            )

            print("Formatted Query: \n", formatted_query)

            cursor = db.connect_to_db()
            result = db.execute_query(cursor, formatted_query)

            print("Results", result)

            response_data = []
            for product_id in req_body["productIds"]:
                temp = {"product_id": product_id, "values": []}
                for record in result:
                    if record[0] == product_id:
                        temp["values"].append(
                            {"date": record[1].strftime('%m/%d/%Y'), "week": record[4], "actual": record[2],
                             "forecast": record[3]})
                response_data.append(temp)
            return response_data
        except Exception as e:
            raise



