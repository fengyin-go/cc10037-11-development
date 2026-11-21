"""消费任务维度的字段迁移演练引擎。

Schema 变更评估通常只回答「哪些表/管线受影响」，但无法回答
「哪个读取作业、从哪一天起、可以用哪一版列」。本引擎把迁移演练
下探到消费任务（consumer task）粒度：

- 解析新旧字段映射，保留旧列/新列/类型和转换方式；
- 每个消费任务单独演练，记录它实际验证过的列及日期，
  形成「消费方读取契约」；
- 同一数据表的两个消费者可能一个能读新类型、一个仍依赖旧列，
  引擎给出「先推进单个消费者」和「等待整表切换」两种策略下
  各自留下的可读数据；
- 一次读取固定一个列版本，分区级显式钉住，不会在同一读取里混列。

本模块只做纯计算，可独立单测；状态与种子数据在
app.services.consumer_migration 中维护。
"""

from __future__ import annotations

import hashlib
import re
from copy import deepcopy
from datetime import datetime
from typing import Any

# ---- 场景常量 ------------------------------------------------------------

TABLE = "dw.dwd_order_wide"
FIELD = "order_amount"
OLD_TYPE = "decimal(20,2)"
NEW_TYPE = "bigint"
OLD_VERSION = "v1"
NEW_VERSION = "v2"

DEFAULT_CHANGE_ID = "chg_order_amount_20261119"
DEFAULT_CHANGE_DETAIL = (
    "order_amount 从 decimal(20,2) 改为 bigint（账务金额元转分），"
    "同时新增 payment_channel、下线 legacy_status_code"
)

# 分区 -> 列版本。切换日 2026-11-15 之前的分区保留旧列，之后写入新列。
PARTITION_VERSIONS: dict[str, str] = {
    "2026-10-01": OLD_VERSION,
    "2026-10-15": OLD_VERSION,
    "2026-10-31": OLD_VERSION,
    "2026-11-13": OLD_VERSION,
    "2026-11-14": OLD_VERSION,
    "2026-11-15": NEW_VERSION,
    "2026-11-16": NEW_VERSION,
    "2026-11-17": NEW_VERSION,
    "2026-11-18": NEW_VERSION,
    "2026-11-19": NEW_VERSION,
}

# 列版本定义。列顺序即读取时的投影顺序。
COLUMN_VERSIONS: dict[str, dict[str, Any]] = {
    OLD_VERSION: {
        "version": OLD_VERSION,
        "label": "旧列版本（金额 decimal 元）",
        "columns": [
            {"name": "order_id", "type": "bigint", "nullable": False},
            {"name": "user_id", "type": "bigint", "nullable": True},
            {"name": "order_amount", "type": OLD_TYPE, "nullable": True},
            {"name": "order_date", "type": "date", "nullable": False},
            {"name": "legacy_status_code", "type": "varchar(16)", "nullable": True},
        ],
        "partition_range": ["2026-10-01", "2026-11-14"],
    },
    NEW_VERSION: {
        "version": NEW_VERSION,
        "label": "新列版本（金额 bigint 分）",
        "columns": [
            {"name": "order_id", "type": "bigint", "nullable": False},
            {"name": "user_id", "type": "bigint", "nullable": True},
            {"name": "order_amount", "type": NEW_TYPE, "nullable": True},
            {"name": "order_date", "type": "date", "nullable": False},
            {"name": "payment_channel", "type": "varchar(32)", "nullable": True},
        ],
        "partition_range": ["2026-11-15", "2026-11-19"],
    },
}

# 数据源能力：一个数据源只暴露固定版本的分区。
SOURCE_DEFS: dict[str, dict[str, Any]] = {
    "dw.dwd_order_wide": {
        "description": "订单宽表主数据源，切换日后提供 v2 分区",
        "versions": [OLD_VERSION, NEW_VERSION],
        "partitions": [
            "2026-10-01",
            "2026-10-15",
            "2026-10-31",
            "2026-11-13",
            "2026-11-14",
            "2026-11-15",
            "2026-11-16",
            "2026-11-17",
            "2026-11-18",
            "2026-11-19",
        ],
    },
    "dw.dwd_order_wide_legacy": {
        "description": "订单宽表旧列只读视图，只提供 v1 分区",
        "versions": [OLD_VERSION],
        "partitions": [
            "2026-10-01",
            "2026-10-15",
            "2026-10-31",
            "2026-11-13",
            "2026-11-14",
        ],
    },
}

# 类型兼容关系。仅用于字段映射说明，实际读取由消费方声明决定。
_TYPE_FAMILY = {
    "decimal(20,2)": "numeric",
    "bigint": "numeric",
    "int": "numeric",
    "varchar(16)": "string",
    "varchar(32)": "string",
    "date": "date",
}


def _norm_type(value: str) -> str:
    return str(value or "").strip().lower().replace(" ", "")


def type_family(value: str) -> str:
    return _TYPE_FAMILY.get(_norm_type(value), "other")


def column_type(version: str, name: str) -> str | None:
    """取某列版本中一列的数据类型。"""
    for column in COLUMN_VERSIONS.get(version, {}).get("columns", []):
        if column["name"] == name:
            return str(column["type"])
    return None


def columns_of(version: str) -> list[dict[str, Any]]:
    return deepcopy(COLUMN_VERSIONS.get(version, {}).get("columns", []))


def partitions_of(version: str) -> list[str]:
    return sorted(p for p, v in PARTITION_VERSIONS.items() if v == version)


def partition_version(partition: str) -> str | None:
    """按切换日推导分区列版本；未知分区返回 None。"""
    return PARTITION_VERSIONS.get(partition)


def all_partitions() -> list[str]:
    return sorted(PARTITION_VERSIONS)


# ---- 字段映射解析 --------------------------------------------------------

_TYPE_TOKEN = r"[A-Za-z]+(?:\([^()]*\))?"
_IDENT = r"[A-Za-z_][\w.]*"


def _mapping_from_versions() -> list[dict[str, Any]]:
    """按两版列定义做基线 diff，保证映射永远完整。"""
    old_columns = {c["name"]: c for c in COLUMN_VERSIONS[OLD_VERSION]["columns"]}
    new_columns = {c["name"]: c for c in COLUMN_VERSIONS[NEW_VERSION]["columns"]}
    mappings: list[dict[str, Any]] = []

    for name, old in old_columns.items():
        if name not in new_columns:
            mappings.append({
                "old_column": name,
                "new_column": None,
                "old_type": old["type"],
                "new_type": None,
                "kind": "drop_column",
                "transform": f"新版本不再输出 {name}，依赖该列的消费者必须停留在旧列版本",
                "requires_transform": False,
                "compatible": False,
            })
        else:
            new = new_columns[name]
            if _norm_type(old["type"]) != _norm_type(new["type"]):
                mappings.append({
                    "old_column": name,
                    "new_column": name,
                    "old_type": old["type"],
                    "new_type": new["type"],
                    "kind": "type_change",
                    "transform": "ROUND(order_amount * 100)::bigint（元转分，需业务确认舍入规则）",
                    "requires_transform": True,
                    "compatible": type_family(old["type"]) == type_family(new["type"]),
                })

    for name, new in new_columns.items():
        if name not in old_columns:
            mappings.append({
                "old_column": None,
                "new_column": name,
                "old_type": None,
                "new_type": new["type"],
                "kind": "add_column",
                "transform": f"新增列 {name}，旧消费者可按 SELECT 显式列继续读取",
                "requires_transform": False,
                "compatible": True,
            })

    return mappings


def _parse_detail(detail: str) -> list[dict[str, Any]]:
    """从人类可写的变更详情里解析字段映射。

    支持三种写法：
      order_amount decimal(20,2) -> order_amount bigint
      order_amount 从 decimal(20,2) 改为 bigint
      新增列 payment_channel VARCHAR(32) / 删除列 legacy_status_code
    """
    parsed: list[dict[str, Any]] = []
    if not detail:
        return parsed

    arrow = re.compile(
        rf"(?P<old_col>{_IDENT})\s+(?P<old_type>{_TYPE_TOKEN})\s*->\s*"
        rf"(?:(?P<new_col>{_IDENT})\s+)?(?P<new_type>{_TYPE_TOKEN})",
        re.IGNORECASE,
    )
    for match in arrow.finditer(detail):
        old_col = match.group("old_col")
        new_col = match.group("new_col") or old_col
        parsed.append({
            "old_column": old_col,
            "new_column": new_col,
            "old_type": match.group("old_type"),
            "new_type": match.group("new_type"),
            "kind": "rename" if old_col != new_col else "type_change",
            "source_text": match.group(0),
        })

    changed = re.compile(
        rf"(?P<col>{_IDENT})\s*从\s*(?P<old_type>{_TYPE_TOKEN})\s*改为\s*(?P<new_type>{_TYPE_TOKEN})",
        re.IGNORECASE,
    )
    for match in changed.finditer(detail):
        parsed.append({
            "old_column": match.group("col"),
            "new_column": match.group("col"),
            "old_type": match.group("old_type"),
            "new_type": match.group("new_type"),
            "kind": "type_change",
            "source_text": match.group(0),
        })

    # 注意：列名与类型之间必须至少有空白，否则会影响 line 会截短列名。
    added = re.compile(
        rf"(?:新增列|新增)\s*(?P<col>{_IDENT})\s+(?P<type>{_TYPE_TOKEN})",
        re.IGNORECASE,
    )
    for match in added.finditer(detail):
        parsed.append({
            "old_column": None,
            "new_column": match.group("col"),
            "old_type": None,
            "new_type": match.group("type"),
            "kind": "add_column",
            "source_text": match.group(0),
        })

    dropped = re.compile(
        rf"(?:删除列|删除|下线)\s*(?P<col>{_IDENT})", re.IGNORECASE
    )
    for match in dropped.finditer(detail):
        parsed.append({
            "old_column": match.group("col"),
            "new_column": None,
            "old_type": column_type(OLD_VERSION, match.group("col")),
            "new_type": None,
            "kind": "drop_column",
            "source_text": match.group(0),
        })

    return parsed


def parse_field_mapping(
    detail: str = DEFAULT_CHANGE_DETAIL,
) -> list[dict[str, Any]]:
    """解析新旧字段映射。

    以两版列定义做完整 diff，再把变更详情里能解析出的原始描述
    （source_text）合并进来。返回值即消费方读取契约使用的映射。
    """
    baseline = _mapping_from_versions()
    parsed = _parse_detail(detail)

    merged: list[dict[str, Any]] = []
    used: set[int] = set()
    for item in baseline:
        match = None
        for index, candidate in enumerate(parsed):
            if index in used:
                continue
            if candidate["kind"] == item["kind"] and (
                candidate["old_column"] == item["old_column"]
                and candidate["new_column"] == item["new_column"]
            ):
                match = (index, candidate)
                break
        if match:
            index, candidate = match
            used.add(index)
            merged.append({**item, "source_text": candidate.get("source_text", "")})
        else:
            merged.append({**item, "source_text": ""})

    # 版本定义里没有、但详情里写到的映射也保留，避免解析结果丢信息。
    def _already_covered(candidate: dict[str, Any]) -> bool:
        for item in merged:
            if candidate["kind"] == "add_column" and (
                item["new_column"] == candidate["new_column"]
                or (item["new_column"] or "").startswith(candidate["new_column"])
            ):
                return True
            if candidate["kind"] in ("drop_column", "type_change") and (
                item["old_column"] == candidate["old_column"]
            ):
                return True
        return False

    for index, candidate in enumerate(parsed):
        if index not in used and not _already_covered(candidate):
            merged.append({
                "old_column": candidate["old_column"],
                "new_column": candidate["new_column"],
                "old_type": candidate["old_type"],
                "new_type": candidate["new_type"],
                "kind": candidate["kind"],
                "transform": "按变更详情解析，未在两版列定义中检出",
                "requires_transform": candidate["kind"] == "type_change",
                "compatible": candidate["kind"] != "drop_column",
                "source_text": candidate.get("source_text", ""),
            })

    return merged


# ---- 单次读取 ------------------------------------------------------------

def _row_count(consumer_key: str, partition: str, version: str) -> int:
    seed = f"{consumer_key}|{partition}|{version}"
    digest = int(hashlib.sha1(seed.encode("utf-8")).hexdigest()[:8], 16)
    return 1200 + digest % 7919


def _checksum(consumer_key: str, partition: str, version: str) -> str:
    seed = f"{consumer_key}|{partition}|{version}|{FIELD}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]


def simulate_read(
    consumer: dict[str, Any],
    partition: str,
    version: str,
) -> dict[str, Any]:
    """按指定列版本模拟一次消费任务读取。

    一次读取只采用一个列版本；缺列或类型不在消费方接受范围内即失败，
    失败结果不会写进消费方读取契约。
    """
    definition = COLUMN_VERSIONS.get(version)
    issues: list[str] = []
    if definition is None:
        return {
            "ok": False,
            "partition": partition,
            "column_version": version,
            "issues": [f"未知列版本 {version}"],
            "columns": [],
            "row_count": 0,
            "checksum": "",
        }

    by_name = {c["name"]: c for c in definition["columns"]}
    accepted = {
        name: {_norm_type(t) for t in types}
        for name, types in consumer.get("accepted_types", {}).items()
    }
    verified_columns: list[dict[str, Any]] = []

    for name in consumer.get("required_columns", []):
        column = by_name.get(name)
        if column is None:
            issues.append(f"缺少列 {name}（{version} 不提供）")
            continue
        allowed = accepted.get(name)
        actual = _norm_type(column["type"])
        if allowed and actual not in allowed:
            issues.append(
                f"列 {name} 类型为 {column['type']}，消费方只接受 "
                + "/".join(sorted(allowed))
            )
            continue
        verified_columns.append({"name": name, "type": column["type"]})

    return {
        "ok": not issues,
        "partition": partition,
        "column_version": version,
        "columns": [c for c in definition["columns"] if c["name"] in
                    {v["name"] for v in verified_columns}],
        "verified_columns": verified_columns,
        "issues": issues,
        "row_count": _row_count(consumer["key"], partition, version) if not issues else 0,
        "checksum": _checksum(consumer["key"], partition, version) if not issues else "",
        "amount_type": column_type(version, FIELD),
    }


def capability_versions(consumer: dict[str, Any]) -> list[str]:
    """消费方在哪些列版本上能完整读出所需列。"""
    versions: list[str] = []
    for version in (OLD_VERSION, NEW_VERSION):
        probe = simulate_read(consumer, partitions_of(version)[0], version)
        if probe["ok"]:
            versions.append(version)
    return versions


# ---- 冲突与两种策略 ------------------------------------------------------

def _readable_partitions(
    consumer: dict[str, Any],
    decisions: dict[str, str],
    partitions: list[str],
) -> list[dict[str, Any]]:
    """按决策版本计算消费者实际能读到的分区。

    每个分区仍按自己的列版本读取（分区显式钉版），但消费者只能读
    自己能力覆盖的版本；决策版本为 v1 时，v2 分区整体不可读。
    """
    capability = set(capability_versions(consumer))
    source = default_data_source(consumer)
    available = set(source_partitions(source))
    readable: list[dict[str, Any]] = []
    for partition in partitions:
        if partition not in available:
            continue
        physical = partition_version(partition) or OLD_VERSION
        decided = decisions.get(consumer["key"], physical)
        if decided == OLD_VERSION and physical == NEW_VERSION:
            continue
        if physical not in capability:
            continue
        probe = simulate_read(consumer, partition, physical)
        if probe["ok"]:
            readable.append({
                "partition": partition,
                "column_version": physical,
                "row_count": probe["row_count"],
                "checksum": probe["checksum"],
            })
    return readable


def build_strategy(
    strategy: str,
    consumers: list[dict[str, Any]],
    decisions: dict[str, str],
    partitions: list[str] | None = None,
) -> dict[str, Any]:
    """构造一种切换策略下的可读数据视图。"""
    partitions = partitions or all_partitions()
    rows: list[dict[str, Any]] = []
    readable_sets: list[set[str]] = []
    all_readable: set[str] = set()

    for consumer in consumers:
        readable = _readable_partitions(consumer, decisions, partitions)
        readable_set = {item["partition"] for item in readable}
        readable_sets.append(readable_set)
        all_readable |= readable_set
        blocked = [p for p in partitions if p not in readable_set]
        rows.append({
            "consumer_key": consumer["key"],
            "consumer_name": consumer["name"],
            "decided_version": decisions.get(consumer["key"]),
            "capability_versions": capability_versions(consumer),
            "readable_partitions": readable,
            "readable_count": len(readable),
            "blocked_partitions": blocked,
            "blocked_count": len(blocked),
            "amount_type": column_type(decisions.get(consumer["key"], OLD_VERSION), FIELD),
        })

    common = set(partitions)
    for readable_set in readable_sets:
        common &= readable_set

    if strategy == "advance_single_consumer":
        label = "先推进单个消费者（报表小计切到新类型）"
        explanation = (
            "报表小计按分区列版本读取：旧分区用 v1、新分区用 v2，历史与新数据都能读；"
            "存量归档抽取仍停留在 v1，只能读到 2026-11-14 及以前的旧列分区，"
            "2026-11-15 之后的新类型分区对它不可读。两边共同可读的只有旧列分区。"
        )
    else:
        label = "等待整表切换（两个消费者都留在旧列）"
        explanation = (
            "整表保持 v1 直到存量归档抽取完成改造：两个消费者都能读旧列分区，"
            "但 2026-11-15 之后写入的新类型分区对任何消费者都不可读，"
            "新金额类型的数据要等整表切换完成后才能被消费。"
        )

    return {
        "strategy": strategy,
        "label": label,
        "decisions": decisions,
        "consumers": rows,
        "readable_partitions_any": sorted(all_readable),
        "readable_count_any": len(all_readable),
        "commonly_readable_partitions": sorted(common),
        "commonly_readable_count": len(common),
        "blocked_partitions": [p for p in partitions if p not in all_readable],
        "new_type_visible_to": [
            row["consumer_key"] for row in rows if row["decided_version"] == NEW_VERSION
        ],
        "explanation": explanation,
    }


def conflict_report(
    consumers: list[dict[str, Any]],
    partitions: list[str] | None = None,
) -> dict[str, Any]:
    """同一数据表两个消费者切换冲突的完整解释。

    报表小计已能读新类型，存量归档抽取还依赖旧列；同时接受两类读请求时，
    分别给出「先推进单个消费者」和「等待整表切换」留下的可读数据。
    """
    partitions = partitions or all_partitions()
    desired = {
        consumer["key"]: consumer.get("desired_version", OLD_VERSION)
        for consumer in consumers
    }
    has_conflict = len(set(desired.values())) > 1

    advance = build_strategy("advance_single_consumer", consumers, desired, partitions)
    wait = build_strategy(
        "wait_full_table_switch", consumers,
        {consumer["key"]: OLD_VERSION for consumer in consumers}, partitions,
    )

    return {
        "table": TABLE,
        "has_conflict": has_conflict,
        "desired_versions": desired,
        "conflict_summary": (
            "报表小计已能读取 order_amount bigint，存量归档抽取仍依赖 "
            "order_amount decimal(20,2) 与 legacy_status_code；同一数据表不能同时"
            "满足两种读取版本。"
            if has_conflict else "两个消费者目标版本一致，无切换冲突。"
        ),
        "strategies": [advance, wait],
        "recommendation": (
            "先推进报表小计，让新分区先服务报表；存量归档抽取保留旧列只读视图，"
            "并设定整表切换的截止分区。若归档是合规硬依赖，则等待整表切换，"
            "期间新类型分区暂不对外提供读取。"
            if has_conflict else "可整表切换。"
        ),
    }


def default_data_source(consumer: dict[str, Any]) -> str:
    return str(consumer.get("data_source") or TABLE)


def source_partitions(data_source: str) -> list[str]:
    return list(SOURCE_DEFS.get(data_source, SOURCE_DEFS[TABLE]).get("partitions", []))


def source_versions(data_source: str) -> list[str]:
    return list(SOURCE_DEFS.get(data_source, SOURCE_DEFS[TABLE]).get("versions", []))


def plan_partitions(
    target_version: str,
    data_source: str | None = None,
) -> list[str]:
    """演练默认分区：数据源上属于目标列版本的分区。"""
    available = source_partitions(data_source or TABLE)
    return [p for p in available if partition_version(p) == target_version]


def read_consumer_partitions(
    consumer: dict[str, Any],
    partitions: list[str],
    *,
    read_prefix: str,
    data_source: str | None = None,
    explicit_versions: dict[str, str] | None = None,
    default_version: str | None = None,
    read_at: str | None = None,
    rehearsal_id: str | None = None,
) -> list[dict[str, Any]]:
    """按分区逐个读取，每个分区显式钉住一个列版本。

    读取时优先用调用方给出的分区级版本，其次用 default_version；
    都不给时按分区的实际物理版本（PARTITION_VERSIONS）推导。
    一次读取只使用一个版本，读出的列来自单一列版本定义。
    """
    source = data_source or default_data_source(consumer)
    available = set(source_partitions(source))
    read_at = read_at or datetime.utcnow().isoformat()
    reads: list[dict[str, Any]] = []

    for index, partition in enumerate(partitions, start=1):
        if explicit_versions and partition in explicit_versions:
            version = explicit_versions[partition]
            version_source = "pinned"
        elif default_version:
            version = default_version
            version_source = "default"
        else:
            version = partition_version(partition) or OLD_VERSION
            version_source = "partition"

        block_issue = None
        if partition not in available:
            block_issue = f"数据源 {source} 不提供分区 {partition}"
        elif version not in source_versions(source):
            block_issue = f"数据源 {source} 不提供列版本 {version}"
        elif partition_version(partition) != version:
            block_issue = (
                f"分区 {partition} 实际列版本为 {partition_version(partition)}，"
                f"不能在本次读取里按 {version} 读取（一次读取只用一个列版本）"
            )
        if block_issue:
            reads.append({
                "read_id": f"{read_prefix}-{index:02d}",
                "rehearsal_id": rehearsal_id,
                "consumer_key": consumer["key"],
                "consumer_name": consumer["name"],
                "table": consumer["table"],
                "data_source": source,
                "partition": partition,
                "column_version": version,
                "version_source": version_source,
                "single_version": True,
                "ok": False,
                "columns": [],
                "verified_columns": [],
                "issues": [block_issue],
                "row_count": 0,
                "checksum": "",
                "amount_type": column_type(version, FIELD),
                "read_at": read_at,
            })
            continue

        probe = simulate_read(consumer, partition, version)
        reads.append({
            "read_id": f"{read_prefix}-{index:02d}",
            "rehearsal_id": rehearsal_id,
            "consumer_key": consumer["key"],
            "consumer_name": consumer["name"],
            "table": consumer["table"],
            "data_source": source,
            "partition": partition,
            "column_version": version,
            "version_source": version_source,
            "single_version": True,
            "ok": probe["ok"],
            "columns": probe["columns"],
            "verified_columns": probe["verified_columns"],
            "issues": probe["issues"],
            "row_count": probe["row_count"],
            "checksum": probe["checksum"],
            "amount_type": probe["amount_type"],
            "read_at": read_at,
        })

    return reads


def verified_columns_from_reads(
    reads: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """汇总一次演练里消费方实际验证过的列和验证日期。"""
    merged: dict[str, dict[str, Any]] = {}
    for read in reads:
        if not read.get("ok"):
            continue
        for column in read.get("verified_columns", []):
            name = column["name"]
            entry = merged.setdefault(name, {
                "name": name,
                "type": column["type"],
                "versions": [],
                "partitions": [],
                "verified_at": read.get("read_at"),
            })
            if read["column_version"] not in entry["versions"]:
                entry["versions"].append(read["column_version"])
            if read["partition"] not in entry["partitions"]:
                entry["partitions"].append(read["partition"])
    result = list(merged.values())
    for entry in result:
        entry["versions"].sort()
        entry["partitions"].sort()
    return sorted(result, key=lambda item: item["name"])


def rehearsal_outcome(
    rehearsal_id: str,
    consumer: dict[str, Any],
    target_version: str,
    reads: list[dict[str, Any]],
    field_mapping: list[dict[str, Any]],
    *,
    change_id: str = DEFAULT_CHANGE_ID,
    change_detail: str = DEFAULT_CHANGE_DETAIL,
    started_at: str | None = None,
) -> dict[str, Any]:
    """生成消费任务演练结果；只有全部读取成功才算演练通过。"""
    succeeded = bool(reads) and all(read["ok"] for read in reads)
    issues: list[str] = []
    for read in reads:
        for issue in read["issues"]:
            message = issue if issue.startswith("分区 ") else (
                f"分区 {read['partition']}：{issue}"
            )
            if message not in issues:
                issues.append(message)

    return {
        "rehearsal_id": rehearsal_id,
        "change_id": change_id,
        "change_detail": change_detail,
        "consumer_key": consumer["key"],
        "consumer_name": consumer["name"],
        "table": consumer["table"],
        "data_source": default_data_source(consumer),
        "target_version": target_version,
        "target_amount_type": column_type(target_version, FIELD),
        "field_mapping": deepcopy(field_mapping),
        "partition_reads": reads,
        "partition_count": len(reads),
        "success_count": sum(1 for read in reads if read["ok"]),
        "failed_count": sum(1 for read in reads if not read["ok"]),
        "verified_columns": verified_columns_from_reads(reads),
        "verified_at": datetime.utcnow().date().isoformat(),
        "succeeded": succeeded,
        "issues": issues,
        "started_at": started_at or datetime.utcnow().isoformat(),
        "finished_at": datetime.utcnow().isoformat(),
    }
