"""消费任务迁移演练领域服务：以消费任务为单位演练字段类型切换。

与 schema_evolution 的关系：
- schema_evolution 回答「哪些表/管线受影响」；
- consumer_migration 回答「哪个读取作业、从哪一天起、能用哪一版列」。

状态都保存在进程内存里（与 store 的示例数据同一生命周期）：
- 消费方读取契约（contract）只会在演练通过且被确认后更新；
- 配置迁移把契约转换成可提交的切换条目（switch_entry）；
- 切换失败/回滚时按条目里的快照恢复列、数据源和输入分区；
- 每次成功读取都写入读数台账，失败恢复后仍可回查校验和。
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any

from app.engine import consumer_migration as engine
from app.services.base import BaseModuleService
from app.store import store

MODULE = "consumer_migration"

CONSUMER_DEFS: list[dict[str, Any]] = [
    {
        "key": "rpt_sales_subtotal",
        "name": "报表小计",
        "table": engine.TABLE,
        "description": "销售报表小计汇总，按 order_amount 求和、按 order_date 分组",
        "data_source": engine.TABLE,
        "required_columns": ["order_id", "order_amount", "order_date"],
        "accepted_types": {"order_amount": [engine.OLD_TYPE, engine.NEW_TYPE]},
        "desired_version": engine.NEW_VERSION,
        "initial_version": engine.OLD_VERSION,
        "initial_partitions": ["2026-11-13", "2026-11-14"],
        "verified_at": "2026-11-14",
    },
    {
        "key": "arc_order_archive_extract",
        "name": "存量归档抽取",
        "table": engine.TABLE,
        "description": "历史订单归档抽取，依赖旧列 legacy_status_code 和 decimal 金额",
        "data_source": "dw.dwd_order_wide_legacy",
        "required_columns": ["order_id", "order_amount", "legacy_status_code"],
        "accepted_types": {"order_amount": [engine.OLD_TYPE]},
        "desired_version": engine.OLD_VERSION,
        "initial_version": engine.OLD_VERSION,
        "initial_partitions": ["2026-11-13", "2026-11-14"],
        "verified_at": "2026-11-13",
    },
]


def _now() -> str:
    return datetime.utcnow().isoformat()


def _initial_contract(defn: dict[str, Any]) -> dict[str, Any]:
    version = defn["initial_version"]
    columns = [
        column for column in engine.columns_of(version)
        if column["name"] in defn["required_columns"]
    ]
    return {
        "consumer_key": defn["key"],
        "consumer_name": defn["name"],
        "table": defn["table"],
        "data_source": defn["data_source"],
        "column_version": version,
        "amount_type": engine.column_type(version, engine.FIELD),
        "verified_columns": [
            {
                "name": column["name"],
                "type": column["type"],
                "versions": [version],
                "partitions": list(defn["initial_partitions"]),
                "verified_at": defn["verified_at"],
            }
            for column in columns
        ],
        "input_partitions": list(defn["initial_partitions"]),
        "verified_at": defn["verified_at"],
        "field_mapping": engine.parse_field_mapping(),
        "rehearsal_id": None,
        "updated_at": defn["verified_at"] + "T00:00:00",
    }


class ConsumerMigrationService(BaseModuleService):
    MODULE = MODULE
    ENTITY = "消费迁移演练"
    OPERATION_TERM = "演练"
    LIST_FIELDS = [
        "消费任务", "数据表", "目标列版本", "读取契约", "关联数据源", "输入分区", "验证日期",
    ]
    REQUIRED_FIELDS = ["消费任务", "数据表"]
    STATUS_ORDER = ["待演练", "演练通过", "演练失败", "已确认", "已切换", "已回滚", "切换失败"]
    ACTION_RULES = {
        "执行演练": "待演练",
        "确认切换": "演练通过",
        "撤销确认": "已确认",
        "执行切换": "已确认",
        "回滚切换": "已切换",
        "标记失败": "已切换",
    }
    NEGATIVE_ACTIONS = ["回滚切换", "标记失败"]

    def __init__(self) -> None:
        self._consumers: dict[str, dict[str, Any]] = {}
        self._rehearsals: dict[str, dict[str, Any]] = {}
        self._switches: dict[str, dict[str, Any]] = {}
        self._ledger: list[dict[str, Any]] = []
        self._rehearsal_seq = 0
        self._switch_seq = 0
        for definition in CONSUMER_DEFS:
            self._consumers[definition["key"]] = {
                "definition": deepcopy(definition),
                "original_mapping": engine.parse_field_mapping(),
                "original_data_source": definition["data_source"],
                "contract": _initial_contract(definition),
                "previous_contract": None,
                "contract_history": [],
                "pending_rehearsal_id": None,
                "status": "待演练",
            }
        self._sync_rows()

    # ---- 通用列表 --------------------------------------------------------

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """消费任务来自演练注册表，不接受直接登记。"""
        return None, []

    def _sync_rows(self) -> None:
        rows = store.rows(MODULE)
        existing = {row.get("consumer_key"): row for row in rows}
        for index, definition in enumerate(CONSUMER_DEFS, start=1):
            state = self._consumers[definition["key"]]
            contract = state["contract"]
            row = existing.get(definition["key"])
            if row is None:
                row = {"id": index, "consumer_key": definition["key"]}
                rows.append(row)
            row.update({
                "消费任务": definition["name"],
                "数据表": definition["table"],
                "目标列版本": definition["desired_version"],
                "读取契约": (
                    f"v{contract['column_version'][1:]} / "
                    f"{contract['amount_type']} / "
                    f"{len(contract['verified_columns'])}列"
                ),
                "关联数据源": contract["data_source"],
                "输入分区": ",".join(contract["input_partitions"]) or "无",
                "验证日期": contract["verified_at"],
                "status": state["status"],
                "pending": state["status"] in ("待演练", "演练通过", "已确认"),
                "abnormal": state["status"] in ("演练失败", "切换失败"),
                "contract": deepcopy(contract),
                "field_mapping": deepcopy(engine.parse_field_mapping()),
                "original_data_source": state["original_data_source"],
                "last_operated_at": state.get("last_operated_at"),
            })

    def list_entries(self, **kwargs: Any) -> tuple[list[dict[str, Any]], int]:
        self._sync_rows()
        return super().list_entries(**kwargs)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        self._sync_rows()
        row = super().get_entry(entry_id)
        return deepcopy(row) if row else None

    def _state_by_key(self, consumer_key: str) -> dict[str, Any]:
        state = self._consumers.get(consumer_key)
        if state is None:
            raise KeyError(f"消费任务 {consumer_key} 不存在")
        return state

    def _set_status(self, state: dict[str, Any], status: str) -> None:
        state["status"] = status
        state["last_operated_at"] = _now()
        self._sync_rows()

    # ---- 消费任务与契约 --------------------------------------------------

    def list_consumers(self) -> list[dict[str, Any]]:
        self._sync_rows()
        result: list[dict[str, Any]] = []
        for definition in CONSUMER_DEFS:
            state = self._consumers[definition["key"]]
            contract = state["contract"]
            result.append({
                "consumer_key": definition["key"],
                "consumer_name": definition["name"],
                "table": definition["table"],
                "description": definition["description"],
                "data_source": contract["data_source"],
                "original_data_source": state["original_data_source"],
                "required_columns": list(definition["required_columns"]),
                "accepted_types": deepcopy(definition["accepted_types"]),
                "desired_version": definition["desired_version"],
                "capability_versions": engine.capability_versions(definition),
                "column_version": contract["column_version"],
                "amount_type": contract["amount_type"],
                "input_partitions": list(contract["input_partitions"]),
                "verified_at": contract["verified_at"],
                "contract": deepcopy(contract),
                "previous_contract": deepcopy(state["previous_contract"]),
                "status": state["status"],
                "pending_rehearsal_id": state["pending_rehearsal_id"],
            })
        return result

    def get_consumer(self, consumer_key: str) -> dict[str, Any]:
        for item in self.list_consumers():
            if item["consumer_key"] == consumer_key:
                return item
        raise KeyError(f"消费任务 {consumer_key} 不存在")

    def field_mapping(self, detail: str | None = None) -> dict[str, Any]:
        mappings = engine.parse_field_mapping(detail or engine.DEFAULT_CHANGE_DETAIL)
        return {
            "change_id": engine.DEFAULT_CHANGE_ID,
            "change_detail": detail or engine.DEFAULT_CHANGE_DETAIL,
            "table": engine.TABLE,
            "old_version": engine.OLD_VERSION,
            "new_version": engine.NEW_VERSION,
            "mappings": mappings,
            "mapping_count": len(mappings),
            "type_change_count": sum(
                1 for item in mappings if item["kind"] == "type_change"
            ),
            "parsed_at": _now(),
        }

    # ---- 演练 ------------------------------------------------------------

    def rehearse(
        self,
        consumer_key: str,
        target_version: str | None = None,
        partitions: list[str] | None = None,
        data_source: str | None = None,
    ) -> dict[str, Any]:
        state = self._state_by_key(consumer_key)
        definition = state["definition"]
        target_version = target_version or definition["desired_version"]
        source = data_source or definition["data_source"]
        if partitions is None:
            planned = engine.plan_partitions(target_version, source)
            if not planned and not data_source:
                # 默认数据源不暴露目标版本时，回到主表演练，
                # 这样失败原因落在列能力上，而不是「源没有分区」。
                source = engine.TABLE
                planned = engine.plan_partitions(target_version, source)
            partitions = planned

        self._rehearsal_seq += 1
        rehearsal_id = (
            f"RSH-{datetime.utcnow().strftime('%Y%m%d')}-{self._rehearsal_seq:03d}"
        )
        reads = engine.read_consumer_partitions(
            definition,
            partitions,
            read_prefix=rehearsal_id,
            data_source=source,
            explicit_versions={partition: target_version for partition in partitions},
            read_at=_now(),
            rehearsal_id=rehearsal_id,
        )
        result = engine.rehearsal_outcome(
            rehearsal_id,
            definition,
            target_version,
            reads,
            engine.parse_field_mapping(),
            started_at=_now(),
        )
        result.update({
            "data_source": source,
            "original_data_source": state["original_data_source"],
            "target_partitions": list(partitions),
            "field_mapping_snapshot": deepcopy(state["original_mapping"]),
            "created_at": _now(),
        })
        if not partitions:
            result["succeeded"] = False
            result["issues"] = [
                f"数据源 {source} 上没有任何分区暴露列版本 {target_version}，"
                "演练无法覆盖读取路径"
            ]
            result["finished_at"] = _now()
        self._rehearsals[rehearsal_id] = result
        self._record_reads(reads)

        if result["succeeded"]:
            state["pending_rehearsal_id"] = rehearsal_id
            if state["status"] not in ("已确认", "已切换"):
                self._set_status(state, "演练通过")
        else:
            state["pending_rehearsal_id"] = None
            if state["status"] not in ("已确认", "已切换"):
                self._set_status(state, "演练失败")
        return deepcopy(result)

    def batch_rehearse(
        self,
        consumer_keys: list[str] | None = None,
        target_versions: dict[str, str] | None = None,
        partitions: dict[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        keys = consumer_keys or [definition["key"] for definition in CONSUMER_DEFS]
        results = []
        for key in keys:
            results.append(self.rehearse(
                key,
                target_version=(target_versions or {}).get(key),
                partitions=(partitions or {}).get(key),
            ))
        return {
            "rehearsal_count": len(results),
            "passed": [r["rehearsal_id"] for r in results if r["succeeded"]],
            "failed": [r["rehearsal_id"] for r in results if not r["succeeded"]],
            "results": results,
            "executed_at": _now(),
        }

    def list_rehearsals(self, consumer_key: str | None = None) -> list[dict[str, Any]]:
        values = list(self._rehearsals.values())
        if consumer_key:
            values = [r for r in values if r["consumer_key"] == consumer_key]
        return [deepcopy(r) for r in sorted(values, key=lambda r: r["created_at"])]

    def get_rehearsal(self, rehearsal_id: str) -> dict[str, Any]:
        result = self._rehearsals.get(rehearsal_id)
        if result is None:
            raise KeyError(f"演练 {rehearsal_id} 不存在")
        return deepcopy(result)

    # ---- 契约确认 / 撤销 -------------------------------------------------

    def confirm(self, consumer_key: str, rehearsal_id: str | None = None) -> dict[str, Any]:
        state = self._state_by_key(consumer_key)
        rehearsal_id = rehearsal_id or state["pending_rehearsal_id"]
        if not rehearsal_id and state["contract"].get("rehearsal_id"):
            # 已经确认过同一契约，重复确认按幂等处理
            rehearsal_id = state["contract"]["rehearsal_id"]
        if not rehearsal_id:
            raise ValueError("没有待确认的演练；请先执行演练")
        rehearsal = self._rehearsals.get(rehearsal_id)
        if rehearsal is None:
            raise KeyError(f"演练 {rehearsal_id} 不存在")
        if rehearsal["consumer_key"] != consumer_key:
            raise ValueError("演练与消费任务不匹配")
        if not rehearsal["succeeded"]:
            raise ValueError("演练未通过，不能确认切换")

        if (
            state["contract"].get("rehearsal_id") == rehearsal_id
            and state["contract"]["column_version"] == rehearsal["target_version"]
        ):
            return {
                "ok": True,
                "consumer_key": consumer_key,
                "contract": deepcopy(state["contract"]),
                "previous_contract": deepcopy(state["previous_contract"]),
                "message": f"{rehearsal['consumer_name']}读取契约已是 {rehearsal['target_version']}",
            }

        candidate = {
            "consumer_key": consumer_key,
            "consumer_name": rehearsal["consumer_name"],
            "table": rehearsal["table"],
            "data_source": rehearsal["data_source"],
            "column_version": rehearsal["target_version"],
            "amount_type": rehearsal["target_amount_type"],
            "verified_columns": deepcopy(rehearsal["verified_columns"]),
            "input_partitions": [
                read["partition"] for read in rehearsal["partition_reads"] if read["ok"]
            ],
            "verified_at": rehearsal["verified_at"],
            "field_mapping": deepcopy(rehearsal["field_mapping"]),
            "rehearsal_id": rehearsal_id,
            "updated_at": _now(),
        }
        state["previous_contract"] = deepcopy(state["contract"])
        state["contract_history"].append({
            "contract": deepcopy(state["contract"]),
            "superseded_at": _now(),
            "reason": f"确认演练 {rehearsal_id}",
        })
        state["contract"] = candidate
        state["pending_rehearsal_id"] = None
        self._set_status(state, "已确认")
        return {
            "ok": True,
            "consumer_key": consumer_key,
            "contract": deepcopy(candidate),
            "previous_contract": deepcopy(state["previous_contract"]),
            "message": f"{rehearsal['consumer_name']}读取契约已确认到 {candidate['column_version']}",
        }

    def unconfirm(self, consumer_key: str) -> dict[str, Any]:
        state = self._state_by_key(consumer_key)
        previous = state["previous_contract"]
        if previous is None:
            raise ValueError("没有可恢复的上一版读取契约")
        active_switches = [
            item for item in self._switches.values()
            if item["consumer_key"] == consumer_key and item["status"] in ("已提交", "已切换")
        ]
        for item in active_switches:
            self._rollback_switch(item, reason=f"撤销 {consumer_key} 的确认")

        state["contract"] = deepcopy(previous)
        state["previous_contract"] = None
        state["pending_rehearsal_id"] = None
        self._set_status(state, "演练通过")
        contract = state["contract"]
        return {
            "ok": True,
            "consumer_key": consumer_key,
            "restored_contract": deepcopy(contract),
            "restored_input_partitions": list(contract["input_partitions"]),
            "restored_amount_type": contract["amount_type"],
            "restored_column_version": contract["column_version"],
            "rolled_back_switches": [item["switch_id"] for item in active_switches],
            "message": (
                f"{state['definition']['name']}已恢复输入分区 "
                f"{','.join(contract['input_partitions'])}，"
                f"金额类型 {contract['amount_type']}"
            ),
        }

    # ---- 配置迁移：契约转切换条目 ----------------------------------------

    def create_switch(
        self,
        consumer_key: str,
        source_env: str = "dev",
        target_env: str = "prod",
    ) -> dict[str, Any]:
        state = self._state_by_key(consumer_key)
        if state["status"] not in ("已确认", "已切换"):
            raise ValueError("读取契约尚未确认，不能生成切换条目")
        contract = state["contract"]
        previous = state["previous_contract"] or contract
        self._switch_seq += 1
        switch_id = (
            f"SW-{datetime.utcnow().strftime('%Y%m%d')}-{self._switch_seq:03d}"
        )
        entry = {
            "switch_id": switch_id,
            "id": self._switch_seq,
            "consumer_key": consumer_key,
            "consumer_name": state["definition"]["name"],
            "table": contract["table"],
            "source_env": source_env,
            "target_env": target_env,
            "contract_version": contract["column_version"],
            "column_version": contract["column_version"],
            "amount_type": contract["amount_type"],
            "data_source": contract["data_source"],
            "input_partitions": list(contract["input_partitions"]),
            "verified_columns": deepcopy(contract["verified_columns"]),
            "column_mapping": deepcopy(contract["field_mapping"]),
            "rollback_columns": deepcopy(previous["verified_columns"]),
            "rollback_column_version": previous["column_version"],
            "rollback_amount_type": previous["amount_type"],
            "rollback_data_source": previous["data_source"],
            "rollback_input_partitions": list(previous["input_partitions"]),
            "rollback_verified_at": previous["verified_at"],
            "original_mapping": deepcopy(state["original_mapping"]),
            "original_data_source": state["original_data_source"],
            "status": "待提交",
            "created_at": _now(),
            "submitted_at": None,
            "rolled_back_at": None,
            "failure_reason": None,
        }
        self._switches[switch_id] = entry
        return deepcopy(entry)

    def list_switches(self, consumer_key: str | None = None) -> list[dict[str, Any]]:
        values = list(self._switches.values())
        if consumer_key:
            values = [v for v in values if v["consumer_key"] == consumer_key]
        return [deepcopy(v) for v in sorted(values, key=lambda v: v["created_at"])]

    def get_switch(self, switch_id: str) -> dict[str, Any]:
        entry = self._switches.get(switch_id)
        if entry is None:
            raise KeyError(f"切换条目 {switch_id} 不存在")
        return entry

    def submit_switch(self, switch_id: str) -> dict[str, Any]:
        entry = self.get_switch(switch_id)
        if entry["status"] not in ("待提交",):
            raise ValueError(f"切换条目当前状态为 {entry['status']}，不能提交")
        entry["status"] = "已切换"
        entry["submitted_at"] = _now()
        state = self._state_by_key(entry["consumer_key"])
        self._set_status(state, "已切换")
        return {
            "ok": True,
            "switch_id": switch_id,
            "status": entry["status"],
            "applied_columns": [
                {"name": c["name"], "type": c["type"]}
                for c in entry["verified_columns"]
            ],
            "input_partitions": list(entry["input_partitions"]),
            "message": f"切换条目 {switch_id} 已提交并应用",
        }

    def _rollback_switch(self, entry: dict[str, Any], reason: str = "") -> dict[str, Any]:
        entry["status"] = "已回滚"
        entry["rolled_back_at"] = _now()
        entry["rollback_reason"] = reason
        state = self._state_by_key(entry["consumer_key"])
        restored = {
            "consumer_key": entry["consumer_key"],
            "consumer_name": entry["consumer_name"],
            "table": entry["table"],
            "data_source": entry["rollback_data_source"],
            "column_version": entry["rollback_column_version"],
            "amount_type": entry["rollback_amount_type"],
            "verified_columns": deepcopy(entry["rollback_columns"]),
            "input_partitions": list(entry["rollback_input_partitions"]),
            "verified_at": entry.get("rollback_verified_at")
            or state["contract"]["verified_at"],
            "field_mapping": deepcopy(entry["column_mapping"]),
            "rehearsal_id": state["contract"].get("rehearsal_id"),
            "updated_at": _now(),
        }
        state["contract"] = restored
        state["previous_contract"] = None
        state["pending_rehearsal_id"] = None
        self._set_status(state, "已回滚")
        return restored

    def rollback_switch(self, switch_id: str, reason: str = "人工回滚") -> dict[str, Any]:
        entry = self.get_switch(switch_id)
        if entry["status"] in ("已回滚", "切换失败"):
            raise ValueError(f"切换条目当前状态为 {entry['status']}，不能回滚")
        restored = self._rollback_switch(entry, reason=reason)
        return {
            "ok": True,
            "switch_id": switch_id,
            "status": entry["status"],
            "restored_contract": restored,
            "restored_input_partitions": restored["input_partitions"],
            "restored_columns": [
                {"name": c["name"], "type": c["type"]}
                for c in restored["verified_columns"]
            ],
            "restored_data_source": restored["data_source"],
            "message": (
                f"已恢复 {len(restored['verified_columns'])} 个可读列、"
                f"{len(restored['input_partitions'])} 个输入分区"
            ),
        }

    def fail_switch(self, switch_id: str, reason: str = "切换执行失败") -> dict[str, Any]:
        entry = self.get_switch(switch_id)
        if entry["status"] in ("已回滚", "切换失败"):
            raise ValueError(f"切换条目当前状态为 {entry['status']}，不能标记失败")
        entry["status"] = "切换失败"
        entry["failure_reason"] = reason
        entry["failed_at"] = _now()
        state = self._state_by_key(entry["consumer_key"])
        # 用任务原先关联的列映射和数据源恢复，而不是切换条目里的新值。
        restored = {
            "consumer_key": entry["consumer_key"],
            "consumer_name": entry["consumer_name"],
            "table": entry["table"],
            "data_source": entry["original_data_source"],
            "column_version": entry["rollback_column_version"],
            "amount_type": entry["rollback_amount_type"],
            "verified_columns": deepcopy(entry["rollback_columns"]),
            "input_partitions": list(entry["rollback_input_partitions"]),
            "verified_at": entry.get("rollback_verified_at")
            or state["contract"]["verified_at"],
            "field_mapping": deepcopy(entry["original_mapping"]),
            "rehearsal_id": state["contract"].get("rehearsal_id"),
            "updated_at": _now(),
        }
        state["contract"] = restored
        state["previous_contract"] = None
        state["pending_rehearsal_id"] = None
        self._set_status(state, "切换失败")
        return {
            "ok": True,
            "switch_id": switch_id,
            "status": entry["status"],
            "failure_reason": reason,
            "restored_contract": restored,
            "restored_data_source": restored["data_source"],
            "restored_input_partitions": restored["input_partitions"],
            "restored_columns": [
                {"name": c["name"], "type": c["type"]}
                for c in restored["verified_columns"]
            ],
            "message": (
                f"切换失败，已用原列映射和数据源 {restored['data_source']} 恢复"
            ),
        }

    # ---- 同时读取 / 冲突解释 --------------------------------------------

    def conflict(self) -> dict[str, Any]:
        consumers = [self._consumers[definition["key"]]["definition"]
                     for definition in CONSUMER_DEFS]
        report = engine.conflict_report(consumers)
        report["consumer_contracts"] = [
            {
                "consumer_key": item["consumer_key"],
                "consumer_name": item["consumer_name"],
                "column_version": item["column_version"],
                "amount_type": item["amount_type"],
                "desired_version": item["desired_version"],
                "data_source": item["data_source"],
                "input_partitions": item["input_partitions"],
            }
            for item in self.list_consumers()
        ]
        return report

    def read_requests(
        self,
        requests: list[dict[str, Any]] | None = None,
        partitions: list[str] | None = None,
    ) -> dict[str, Any]:
        """同时接受多个消费任务的读请求，按任务各自契约钉版读取。"""
        if not requests:
            requests = [
                {"consumer_key": definition["key"]}
                for definition in CONSUMER_DEFS
            ]
        results: list[dict[str, Any]] = []
        readable_by_consumer: dict[str, set[str]] = {}
        for index, request in enumerate(requests, start=1):
            consumer_key = request["consumer_key"]
            state = self._state_by_key(consumer_key)
            definition = state["definition"]
            contract = state["contract"]
            # 不指定分区时按各自读取契约里的输入分区并发读取；
            # 这样两类读请求都落在自己真正验证过的分区上。
            wanted = request.get("partitions") or (
                list(contract["input_partitions"]) if partitions is None else partitions
            )
            preferred = request.get("column_version") or contract["column_version"]
            source = request.get("data_source") or contract["data_source"]
            if request.get("column_version"):
                # 读请求显式点了一个列版本：所有分区都按它读，
                # 分区物理版本不一致时按「一次读取单版本」拒绝。
                read_versions: dict[str, str] = {}
            else:
                # 未显式点版本时，每个分区按自己的物理版本读取；
                # 消费者读不了的版本会在 simulate_read 里失败。
                read_versions = {
                    partition: (engine.partition_version(partition) or engine.OLD_VERSION)
                    for partition in wanted
                }
            default_version = preferred
            reads = engine.read_consumer_partitions(
                definition,
                wanted,
                read_prefix=f"REQ-{index:02d}",
                data_source=source,
                explicit_versions=read_versions or None,
                default_version=default_version,
                read_at=_now(),
            )
            self._record_reads(reads)
            successful = [r for r in reads if r["ok"]]
            readable_by_consumer[consumer_key] = {r["partition"] for r in successful}
            results.append({
                "consumer_key": consumer_key,
                "consumer_name": definition["name"],
                "requested_version": preferred,
                "data_source": source,
                "read_partitions": wanted,
                "column_version": preferred,
                "amount_type": engine.column_type(preferred, engine.FIELD),
                "single_version_per_partition": all(r["single_version"] for r in reads),
                "reads": reads,
                "readable_partitions": [r["partition"] for r in successful],
                "blocked_partitions": [r["partition"] for r in reads if not r["ok"]],
                "issues": sorted({i for r in reads for i in r["issues"]}),
            })
        common: set[str] | None = None
        for partitions_read in readable_by_consumer.values():
            common = set(partitions_read) if common is None else common & partitions_read
        return {
            "table": engine.TABLE,
            "request_count": len(results),
            "results": results,
            "commonly_readable_partitions": sorted(common or set()),
            "executed_at": _now(),
            "conflict": self.conflict(),
        }

    # ---- 读数台账 --------------------------------------------------------

    def _record_reads(self, reads: list[dict[str, Any]]) -> None:
        for read in reads:
            if not read.get("ok"):
                continue
            self._ledger.append({
                "read_id": f"{read['read_id']}-{len(self._ledger) + 1:04d}",
                "source_read_id": read["read_id"],
                "rehearsal_id": read.get("rehearsal_id"),
                "consumer_key": read["consumer_key"],
                "consumer_name": read["consumer_name"],
                "table": read["table"],
                "data_source": read["data_source"],
                "partition": read["partition"],
                "column_version": read["column_version"],
                "version_source": read["version_source"],
                "columns": deepcopy(read["columns"]),
                "row_count": read["row_count"],
                "checksum": read["checksum"],
                "amount_type": read.get("amount_type")
                or engine.column_type(read["column_version"], engine.FIELD),
                "verified": True,
                "read_at": read["read_at"],
            })

    def reads(
        self,
        consumer_key: str | None = None,
        partition: str | None = None,
        rehearsal_id: str | None = None,
        column_version: str | None = None,
    ) -> dict[str, Any]:
        values = list(self._ledger)
        if consumer_key:
            values = [v for v in values if v["consumer_key"] == consumer_key]
        if partition:
            values = [v for v in values if v["partition"] == partition]
        if rehearsal_id:
            values = [v for v in values if v["rehearsal_id"] == rehearsal_id]
        if column_version:
            values = [v for v in values if v["column_version"] == column_version]
        items = []
        for value in values:
            expected = engine._checksum(
                value["consumer_key"], value["partition"], value["column_version"]
            )
            item = deepcopy(value)
            item["checksum_verified"] = expected == value["checksum"]
            items.append(item)
        return {
            "total": len(items),
            "items": items,
            "filters": {
                "consumer_key": consumer_key,
                "partition": partition,
                "rehearsal_id": rehearsal_id,
                "column_version": column_version,
            },
            "snapshot_at": _now(),
        }

    # ---- 通用动作分派 ----------------------------------------------------

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        self._sync_rows()
        row = store.find(MODULE, entry_id)
        if row is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"
        consumer_key = str(row["consumer_key"])
        try:
            if action == "执行演练":
                result = self.rehearse(consumer_key)
                message = (
                    f"演练 {result['rehearsal_id']} "
                    + ("通过" if result["succeeded"] else "未通过")
                )
            elif action == "确认切换":
                result = self.confirm(consumer_key)
                message = result["message"]
            elif action == "撤销确认":
                result = self.unconfirm(consumer_key)
                message = result["message"]
            elif action == "执行切换":
                switch = self.create_switch(consumer_key)
                result = self.submit_switch(switch["switch_id"])
                message = result["message"]
            elif action == "回滚切换":
                switches = [
                    item for item in self.list_switches(consumer_key)
                    if item["status"] in ("已提交", "已切换")
                ]
                if not switches:
                    return None, "没有可回滚的切换条目"
                result = self.rollback_switch(switches[-1]["switch_id"])
                message = result["message"]
            elif action == "标记失败":
                switches = [
                    item for item in self.list_switches(consumer_key)
                    if item["status"] in ("已提交", "已切换")
                ]
                if not switches:
                    return None, "没有可标记失败的切换条目"
                result = self.fail_switch(switches[-1]["switch_id"])
                message = result["message"]
            else:
                return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        except (KeyError, ValueError) as error:
            return None, str(error)
        self._sync_rows()
        return store.find(MODULE, entry_id), message
