"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import business, consumer_migration, resource

ROUTERS = [consumer_migration, resource, business]
