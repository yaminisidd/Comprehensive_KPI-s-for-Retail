import azure.functions as func
import logging
import json
from collections import defaultdict
from datetime import datetime
from utils.postgresql_db import PostgreSQL
from utils.calculate_level import calculate_level_adjusted
from utils.separate_year_with_root import build_separate_year_with_root
import pandas as pd

class KpiService:

    def __init__(self) -> None:
        pass
    def revenue_progression_ytd(self,blueprint,req):
        logging.info("revenue progression financialyear invoked...")
        try:
            db = PostgreSQL()
            input_date_range=req.get_json().get("input")
            input_date_range=', '.join(f"'{date}'" for date in input_date_range)
            sql_query =blueprint["revenue"]["progression"]["ytd"]["query"]
            sql_query=sql_query.replace("date_range",input_date_range)
            print(sql_query)
        
            cursor = db.connect_to_db()
            result = db.execute_query(cursor, sql_query)
            output = {
                "Year": [row[0] for row in result],
                "Month": [row[1] for row in result],
                "revenue": [row[2] for row in result]
            }

            return output
        except Exception as e:
            logging.error(f"An error occurred: {e}")
        raise


    def revenue_growth_ytd(self,blueprint,req):
        logging.info("revenue growth financialyear invoked...")

        try:
            db = PostgreSQL()
            input_date_range=req.get_json().get("input")
            input_date_range=', '.join(f"'{date}'" for date in input_date_range)
            sql_query = blueprint["revenue"]["growth"]["ytd"]["query"]
            sql_query=sql_query.replace("date_range",input_date_range)
            print(sql_query)
        
            cursor = db.connect_to_db()
            result = db.execute_query(cursor, sql_query)

            output1 = {
                "Year": [row[0] for row in result],
                "totalRevenue": [row[1] for row in result],
                "previousRevenue": [row[2] for row in result],
                "revenue diff" :[row[3] for row in result]
                  }

            return output1
        except Exception as e:
            logging.error(f"An error occurred: {e}")
        raise


    def revenue_growth_yoy(self,blueprint,req) :
        logging.info("revenue growth financialyear invoked...")

        try:
            db = PostgreSQL()
            input_date_range=req.get_json().get("input")
            input_date_range=', '.join(f"'{date}'" for date in input_date_range)
            sql_query = blueprint["revenue"]["growth"]["yoy"]["query"]
            sql_query=sql_query.replace("date_range",input_date_range)
            print(sql_query)

            cursor = db.connect_to_db()
            result = db.execute_query(cursor, sql_query)
            output2 = {
                "Year": [row[0] for row in result],
                "totalRevenue": [row[1] for row in result],
                "previousRevenue": [row[2] for row in result],
                "revenue diff" :[row[3] for row in result]
                  }
            return output2
        except Exception as e:
            raise

    def revenue_progression_yoy(self,blueprint,req):
        logging.info("revenue progression yoy invoked...")
        try:
            db = PostgreSQL()
            input_date_range=req.get_json().get("input")
            input_date_range=', '.join(f"'{date}'" for date in input_date_range)
            sql_query = blueprint["revenue"]["progression"]["yoy"]["query"]
            sql_query=sql_query.replace("date_range",input_date_range)
            print(sql_query)

            cursor = db.connect_to_db()
            result = db.execute_query(cursor, sql_query)
            output = {
                "Year": [row[0] for row in result],
                "Month_no": [row[1] for row in result],
                "Month": [row[2] for row in result],
                "revenue" :[row[3] for row in result]
                  }

            return output
        except Exception as e:
            raise

    def revenue_growth_yoy_explore(self,blueprint,req):
        logging.info("revenue growth yoy explore invoked...")

        try:
            db = PostgreSQL()
            input_date_range=req.get_json().get("input")
            input_date_range=', '.join(f"'{date}'" for date in input_date_range)
            sql_query = blueprint["revenue"]["growth"]["yoy_explore"]["query"]
            sql_query=sql_query.replace("date_range",input_date_range)
            print(sql_query)

            cursor = db.connect_to_db()
            result = db.execute_query(cursor, sql_query)
            cols=["year", "division","department_description","class_description","subclass_description","revenue_diff"]
            df = pd.DataFrame(result, columns=cols)
            df['levels'] = df.apply(calculate_level_adjusted, axis=1)
            df["id"]=df.index
            dict_file=build_separate_year_with_root(df)

            return dict_file
        except Exception as e:
            raise
        
    def revenue_growth_performance_metric(self,blueprint,req):
        logging.info("revenue growth performance metric invoked...")
        try:
            db = PostgreSQL()
            sql_query1 = blueprint["revenue"]["growth"]["performance_metric"]["query1"]
            sql_query2 = blueprint["revenue"]["growth"]["performance_metric"]["query2"]
            sql_query3 = blueprint["revenue"]["growth"]["performance_metric"]["query3"]
            sql_query4 = blueprint["revenue"]["growth"]["performance_metric"]["query4"]

            cursor1 = db.connect_to_db()
            result1 = db.execute_query(cursor1, sql_query1)
            cursor2 = db.connect_to_db()
            result2 = db.execute_query(cursor2, sql_query2)
            cursor3 = db.connect_to_db()
            result3 = db.execute_query(cursor3, sql_query3)
            cursor4 = db.connect_to_db()
            result4 = db.execute_query(cursor4, sql_query4)

            # Check results and extract data
            if not result1 or not result2:
                raise ValueError("No results returned from queries")

            current_week_data, last_week_data, current_month_data, last_year_same_month_data, current_year_data, last_year_data = result1[0]
            data_dict = {year: value for year, value in result3}
            years = sorted(data_dict.keys())
            current_year = max(years)
            previous_year = current_year - 1
            percentage_difference = ((data_dict[current_year] - data_dict[previous_year]) / data_dict[previous_year]) * 100
            formatted_data_dict = {
            "yearly_average": {
            "currentPeriod": {
                "name": current_year,
                "value": data_dict[current_year]
            },
            "previousPeriod": {
                "name": previous_year,
                "value": data_dict[previous_year]
            },
            "percentage_difference": percentage_difference
        }
    }
            data_dict1 = {year: value for year, value in result4}
            years1 = sorted(data_dict1.keys())
            current_year1 = max(years1)
            previous_year1 = current_year1 - 1
            percentage_difference1 = ((data_dict1[current_year1] - data_dict1[previous_year1]) / data_dict1[previous_year1]) * 100
            formatted_data_dict1 = {
            "yearly_average": {
            "currentPeriod": {
                "name": current_year1,
                "value": data_dict1[current_year1]
            },
            "previousPeriod": {
                "name": previous_year1,
                "value": data_dict1[previous_year1]
            },
            "percentage_difference": percentage_difference1
        }
    }

            # Unpack the result values safely
            weekly_percentage_change = ((current_week_data - last_week_data) / last_week_data) * 100
            monthly_percentage_change = ((current_month_data - last_year_same_month_data) / last_year_same_month_data) * 100
            yearly_percentage_change = ((current_year_data - last_year_data) / last_year_data) * 100

            # Ensure result2 contains tuples of dates
            if len(result2) > 0 and isinstance(result2[0], tuple):
                formatted_dates = [d.strftime("%b %d, %Y") for d in result2[0]]
            else:
                raise ValueError("Expected result2 to contain a tuple of dates")


            output = {
                "weekly": {
                    "currentPeriod": {
                        "name": formatted_dates[0],
                        "value": current_week_data
                    },
                    "previousPeriod": {
                        "name": formatted_dates[1],
                        "value": last_week_data
                    },
                    "percentageChange": f"{weekly_percentage_change:.2f}%"
                },
                "monthly": {
                    "currentPeriod": {
                        "name": formatted_dates[2],
                        "value": current_month_data
                    },
                    "previousPeriod": {
                        "name": formatted_dates[3],
                        "value": last_year_same_month_data
                    },
                    "percentageChange": f"{monthly_percentage_change:.2f}%"
                },
                "yearly": {
                    "currentPeriod": {
                        "name": formatted_dates[4],
                        "value": current_year_data
                    },
                    "previousPeriod": {
                        "name": formatted_dates[5],
                        "value": last_year_data
                    },
                    "percentageChange": f"{yearly_percentage_change:.2f}%"
                },
                "yearly_average": formatted_data_dict,
                "average_order_value": formatted_data_dict1
            }

            return output
        except Exception as e:
            raise