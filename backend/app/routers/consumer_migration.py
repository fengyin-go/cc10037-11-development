"""消费任务迁移演练API：字段映射、演练、契约、切换条目、回滚与读数回查。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.services import service_for
from app.services.consumer_migration import ConsumerMigrationService

router = APIRouter(prefix="/api/consumer-migration", tags=["消费任务迁移"])


def _service() -> ConsumerMigrationService:
    service = service_for("consumer_migration")
    if not isinstance(service, ConsumerMigrationService):
        raise HTTPException(status_code=500, detail="消费迁移服务未注册")
    return service


def _call(func, *args, **kwargs) -> Any:
    try:
        return func(*args, **kwargs)
    except KeyError as error:
        raise HTTPException(status_code=404, detail=str(error))
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error))


@router.get("/consumers")
def list_consumers() -> dict[str, Any]:
    """列出消费任务及其当前生效的消费方读取契约。"""
    items = _service().list_consumers()
    return {"items": items, "total": len(items)}


@router.get("/consumers/{consumer_key}")
def get_consumer(consumer_key: str) -> dict[str, Any]:
    return _call(_service().get_consumer, consumer_key)


@router.get("/field-mapping")
def field_mapping(detail: str | None = Query(default=None)) -> dict[str, Any]:
    """解析新旧字段映射；不传 detail 时使用默认金额类型变更。"""
    return _service().field_mapping(detail)


@router.post("/rehearsals")
def create_rehearsal(body: dict[str, Any]) -> dict[str, Any]:
    """对单个消费任务执行迁移演练。

    body: consumer_key, target_version?, partitions?, data_source?
    """
    if not body.get("consumer_key"):
        raise HTTPException(status_code=400, detail="缺少必填参数: consumer_key")
    return _call(
        _service().rehearse,
        body["consumer_key"],
        target_version=body.get("target_version"),
        partitions=body.get("partitions"),
        data_source=body.get("data_source"),
    )


@router.post("/rehearsals/batch")
def batch_rehearse(body: dict[str, Any]) -> dict[str, Any]:
    """批量演练；body: consumer_keys?, target_versions?, partitions?"""
    return _call(
        _service().batch_rehearse,
        consumer_keys=body.get("consumer_keys"),
        target_versions=body.get("target_versions"),
        partitions=body.get("partitions"),
    )


@router.get("/rehearsals")
def list_rehearsals(consumer_key: str | None = Query(default=None)) -> dict[str, Any]:
    items = _service().list_rehearsals(consumer_key)
    return {"items": items, "total": len(items)}


@router.get("/rehearsals/{rehearsal_id}")
def get_rehearsal(rehearsal_id: str) -> dict[str, Any]:
    return _call(_service().get_rehearsal, rehearsal_id)


@router.post("/consumers/{consumer_key}/confirm")
def confirm_contract(consumer_key: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    """用某次演练结果更新消费方读取契约。"""
    rehearsal_id = (body or {}).get("rehearsal_id")
    return _call(_service().confirm, consumer_key, rehearsal_id)


@router.post("/consumers/{consumer_key}/unconfirm")
def unconfirm_contract(consumer_key: str) -> dict[str, Any]:
    """撤销确认，恢复上一版可读列与输入分区。"""
    return _call(_service().unconfirm, consumer_key)


@router.post("/switches")
def create_switch(body: dict[str, Any]) -> dict[str, Any]:
    """配置迁移：把已确认的读取契约转换成可提交切换条目。

    body: consumer_key, source_env?, target_env?
    """
    if not body.get("consumer_key"):
        raise HTTPException(status_code=400, detail="缺少必填参数: consumer_key")
    return _call(
        _service().create_switch,
        body["consumer_key"],
        source_env=body.get("source_env", "dev"),
        target_env=body.get("target_env", "prod"),
    )


@router.post("/config-migration/entries")
def build_config_migration_entries(body: dict[str, Any] | None = None) -> dict[str, Any]:
    """配置迁移入口：将消费方读取契约批量转换为切换条目。

    body: consumer_key?, source_env?, target_env?
    """
    from app.services import service_for

    migration = service_for("config_migration")
    if migration is None:
        raise HTTPException(status_code=500, detail="配置迁移服务未注册")
    body = body or {}
    return _call(
        migration.build_switch_entries,
        consumer_key=body.get("consumer_key"),
        source_env=body.get("source_env", "dev"),
        target_env=body.get("target_env", "prod"),
    )


@router.get("/switches")
def list_switches(consumer_key: str | None = Query(default=None)) -> dict[str, Any]:
    items = _service().list_switches(consumer_key)
    return {"items": items, "total": len(items)}


@router.post("/switches/{switch_id}/submit")
def submit_switch(switch_id: str) -> dict[str, Any]:
    return _call(_service().submit_switch, switch_id)


@router.post("/switches/{switch_id}/rollback")
def rollback_switch(switch_id: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    return _call(
        _service().rollback_switch,
        switch_id,
        reason=(body or {}).get("reason", "人工回滚"),
    )


@router.post("/switches/{switch_id}/fail")
def fail_switch(switch_id: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    return _call(
        _service().fail_switch,
        switch_id,
        reason=(body or {}).get("reason", "切换执行失败"),
    )


@router.get("/conflict")
def conflict_report() -> dict[str, Any]:
    """同一数据表两个消费者的切换冲突与两种策略的可读数据。"""
    return _service().conflict()


@router.post("/read-requests")
def read_requests(body: dict[str, Any] | None = None) -> dict[str, Any]:
    """同时接受多个消费任务的读请求，按分区钉住明确列版本。

    body: requests? [{consumer_key, partitions?, column_version?, data_source?}],
          partitions?
    """
    body = body or {}
    return _call(
        _service().read_requests,
        requests=body.get("requests"),
        partitions=body.get("partitions"),
    )


@router.get("/reads")
def query_reads(
    consumer_key: str | None = Query(default=None),
    partition: str | None = Query(default=None),
    rehearsal_id: str | None = Query(default=None),
    column_version: str | None = Query(default=None),
) -> dict[str, Any]:
    """已核验历史读数的回查接口，含校验和复核。"""
    return _service().reads(
        consumer_key=consumer_key,
        partition=partition,
        rehearsal_id=rehearsal_id,
        column_version=column_version,
    )
