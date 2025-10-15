import azure.functions as func
import logging
import json
from collections import defaultdict
from datetime import datetime
import time
from services.kpi_service import KpiService
import os


revenue_bp = func.Blueprint()
revenue_file = open(os.path.join(os.getcwd(), "blueprints", "revenue_template.json"))
revenue_template = json.load(revenue_file)
service = KpiService()

@revenue_bp.route("revenue/progression/financialyear", methods=["POST"])
def revenue_progression_ytd_analysis(req: func.HttpRequest) -> func.HttpResponse:
    init_time = time.time()
    try:

        response = service.revenue_progression_ytd(revenue_template,req)

    except Exception as e:
        return func.HttpResponse(json.dumps({
            "status": 500,
            "message": f"Error : {e}",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))

    return func.HttpResponse(json.dumps({
            "data": response,
            "status": 200,
            "message": "Successfull",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))


@revenue_bp.route("revenue/growth/financialyear", methods=["POST"])
def revenue_growth_ytd_analysis(req: func.HttpRequest) -> func.HttpResponse:

    init_time = time.time()
    try:

        response = service.revenue_growth_ytd(revenue_template, req)


    except Exception as e:
        return func.HttpResponse(json.dumps({
            "status": 500,
            "message": f"Error : {e}",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))

    return func.HttpResponse(json.dumps({
            "data": response,
            "status": 200,
            "message": "Successfull",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))

@revenue_bp.route("revenue/growth/yoy", methods=["POST"])
def revenue_growth_yoy_analysis(req: func.HttpRequest) -> func.HttpResponse:

    init_time = time.time()
    try:

        response = service.revenue_growth_yoy(revenue_template, req)
    except Exception as e:
        return func.HttpResponse(json.dumps({
            "status": 500,
            "message": f"Error : {e}",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))

    return func.HttpResponse(json.dumps({
            "data": response,
            "status": 200,
            "message": "Successfull",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))


@revenue_bp.route("revenue/progression/yoy", methods=["POST"])
def revenue_progression_yoy_analysis(req: func.HttpRequest) -> func.HttpResponse:

    init_time = time.time()
    try:
        response = service.revenue_progression_yoy(revenue_template, req)

    except Exception as e:
        return func.HttpResponse(json.dumps({
            "status": 500,
            "message": f"Error : {e}",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))

    return func.HttpResponse(json.dumps({
            "data": response,
            "status": 200,
            "message": "Successfull",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))
@revenue_bp.route("/revenue/growth/yoy/explore", methods=["POST"])
def revenue_growth_yoy_explore_analysis(req: func.HttpRequest) -> func.HttpResponse:

    init_time = time.time()
    try:
        response = service.revenue_growth_yoy_explore(revenue_template, req)
        

    except Exception as e:
        return func.HttpResponse(json.dumps({
            "status": 500,
            "message": f"Error : {e}",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))

    return func.HttpResponse(json.dumps({
            "data": response,
            "status": 200,
            "message": "Successfull",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))
@revenue_bp.route("/revenue/growth/performance/metric", methods=["GET"])
def revenue_growth_performance_metric_analysis(req: func.HttpRequest) -> func.HttpResponse:

    init_time = time.time()

    try:
        response = service.revenue_growth_performance_metric(revenue_template, req)


    except Exception as e:
        return func.HttpResponse(json.dumps({
            "status": 500,
            "message": f"Error: {e}",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))

    return func.HttpResponse(json.dumps({
        "data": response,
        "status": 200,
        "message": "Successful",
        "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "executionTime": str(round((time.time() - init_time), 2))
    }))
