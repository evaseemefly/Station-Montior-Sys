# from controller import *
from controller.station_realtime import app as station_surge_app
from controller.station_alert import app as station_alert_app
from controller.station_astronomictide import app as station_astronomictide
from controller.station import app as station_base

urlpatterns = [
    {"ApiRouter": station_surge_app, "prefix": "/station/realtime", "tags": ["海洋站实况模块"]},
    {"ApiRouter": station_alert_app, "prefix": "/station/alert", "tags": ["警戒潮位模块"]},
    {"ApiRouter": station_astronomictide, "prefix": "/station/astronomictide", "tags": ["天文潮模块"]},
    {"ApiRouter": station_base, "prefix": "/station/base", "tags": ["海洋站基础信息模块"]},
]
