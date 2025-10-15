import azure.functions as func
import json
from datetime import datetime
import time
import os
from services.sku_prioritization_service import SkuPrioritizationService

prioritization_bp = func.Blueprint()
prioritization_file = open(os.path.join(os.getcwd(), "blueprints", "sku_prioritization_template.json"))
prioritization_template = json.load(prioritization_file)
service = SkuPrioritizationService()

@prioritization_bp.route("sku-prioritization/top-performing", methods=["GET"])
def top_performing_sku(req: func.HttpRequest) -> func.HttpResponse:
    init_time = time.time()
    try:
        response = service.get_top_performing_skus(prioritization_template)
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

@prioritization_bp.route("sku-prioritization/top-performing-sku-count", methods=["GET"])
def top_performing_sku_count(req: func.HttpRequest) -> func.HttpResponse:
    init_time = time.time()
    try:
        response = service.get_top_performing_skus_count(prioritization_template)
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

@prioritization_bp.route("sku-prioritization/all", methods=["GET"])
def get_all_skus(req: func.HttpRequest) -> func.HttpResponse:
    init_time = time.time()
    try:
        response = service.all_prioritization_scores(prioritization_template)
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