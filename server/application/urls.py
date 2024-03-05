# from controller import *
from controller.station_realtime import app as station_surge_app
from controller.station import app as station_app

urlpatterns = [
    {"ApiRouter": station_surge_app, "prefix": "/station/realtime", "tags": ["海洋站实况模块"]},
]
