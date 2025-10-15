import azure.functions as func
import json
from datetime import datetime
import time
import os
from services.demand_forecast_service import DemandForecastService

demand_bp = func.Blueprint()

demand_file = open(os.path.join(os.getcwd(), "blueprints", "demand_template.json"))
demand_template = json.load(demand_file)

service = DemandForecastService()


@demand_bp.route("demand/shortfall", methods=["POST"])
def demand_shortfall_analysis(req: func.HttpRequest) -> func.HttpResponse:
    init_time = time.time()

    try:

        response = service.demand_shortfall(demand_template, req.get_json().get("entrytype"),
                                            req.get_json().get("offset"), req.get_json().get("limit"))

        return func.HttpResponse(json.dumps({
            "data": response,
            "status": 200,
            "message": "Successfull",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))

    except Exception as e:
        return func.HttpResponse(json.dumps({
            "status": 500,
            "message": f"Error : {e}",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))


@demand_bp.route("demand/surge", methods=["POST"])
def demand_surge_analysis(req: func.HttpRequest) -> func.HttpResponse:
    init_time = time.time()

    try:

        response = service.demand_surge(demand_template, req.get_json().get("entrytype"), req.get_json().get("offset"),
                                        req.get_json().get("limit"))

        return func.HttpResponse(json.dumps({
            "data": response,
            "status": 200,
            "message": "Successfull",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))

    except Exception as e:
        return func.HttpResponse(json.dumps({
            "status": 500,
            "message": f"Error : {e}",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))


@demand_bp.route("demand/forecast/actual-forecast", methods=["POST"])
def actual_forecast(req: func.HttpRequest) -> func.HttpResponse:
    req_body = req.get_json()
    init_time = time.time()

    try:

        response = service.actual_demand_forecast(demand_template, req_body=req_body)

        return func.HttpResponse(json.dumps({
            "status": 200,
            "message": "Successfull",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2)),
            "data": response,
        }))
    except Exception as e:
        return func.HttpResponse(json.dumps({
            "status": 500,
            "message": f"Error : {e}",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))

@demand_bp.route("demand/forecast/compare", methods=["POST"])
def demand_forecast_compare(req: func.HttpRequest) -> func.HttpResponse:
    req_body = req.get_json()
    init_time = time.time()

    try:

        response = service.demand_forecast_compare(demand_template, req_body=req_body)

        return func.HttpResponse(json.dumps({
            "status": 200,
            "message": "Successfull",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2)),
            "data": response,
        }))
    except Exception as e:
        return func.HttpResponse(json.dumps({
            "status": 500,
            "message": f"Error : {e}",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executionTime": str(round((time.time() - init_time), 2))
        }))
