import azure.functions as func
from controllers.demand_forecast_controller import demand_bp
from controllers.kpi_controller import revenue_bp
from controllers.sku_prioritization_controller import prioritization_bp
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
app.register_blueprint(demand_bp)
app.register_blueprint(revenue_bp)
app.register_blueprint(prioritization_bp)