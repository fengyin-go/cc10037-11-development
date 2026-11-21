"""消费任务迁移演练的端到端用例（标准库 unittest，无需额外依赖）。

运行：cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import unittest

from app.engine import consumer_migration as engine
from app.services.consumer_migration import ConsumerMigrationService


class FieldMappingTests(unittest.TestCase):
    def test_mapping_covers_type_change_drop_and_add(self):
        result = ConsumerMigrationService().field_mapping()
        kinds = {item["kind"] for item in result["mappings"]}
        self.assertEqual(kinds, {"type_change", "drop_column", "add_column"})
        amount = [m for m in result["mappings"] if m["old_column"] == "order_amount"][0]
        self.assertEqual(amount["old_type"], "decimal(20,2)")
        self.assertEqual(amount["new_type"], "bigint")
        self.assertTrue(amount["requires_transform"])
        self.assertIn("order_amount", amount["source_text"])

    def test_parses_arrow_syntax(self):
        mappings = engine.parse_field_mapping(
            "order_amount decimal(20,2) -> amount_cent bigint"
        )
        renamed = [m for m in mappings if m["new_column"] == "amount_cent"]
        self.assertTrue(renamed)
        self.assertEqual(renamed[0]["kind"], "rename")


class RehearsalTests(unittest.TestCase):
    def setUp(self):
        self.service = ConsumerMigrationService()

    def test_report_rehearsal_uses_new_type_only(self):
        result = self.service.rehearse("rpt_sales_subtotal")
        self.assertTrue(result["succeeded"])
        self.assertEqual(result["target_version"], engine.NEW_VERSION)
        self.assertEqual(result["target_amount_type"], "bigint")
        self.assertTrue(all(
            read["column_version"] == engine.NEW_VERSION
            for read in result["partition_reads"]
        ))
        date = result["verified_at"]
        self.assertTrue(all(
            column["verified_at"].startswith(date)
            for column in result["verified_columns"]
        ))

    def test_archive_rehearsal_uses_old_columns(self):
        result = self.service.rehearse("arc_order_archive_extract")
        self.assertTrue(result["succeeded"])
        self.assertEqual(result["target_version"], engine.OLD_VERSION)
        names = {c["name"] for c in result["verified_columns"]}
        self.assertIn("legacy_status_code", names)

    def test_failed_rehearsal_does_not_enter_contract(self):
        before = self.service.get_consumer("arc_order_archive_extract")["contract"]
        result = self.service.rehearse(
            "arc_order_archive_extract", target_version=engine.NEW_VERSION
        )
        self.assertFalse(result["succeeded"])
        self.assertTrue(result["issues"])
        after = self.service.get_consumer("arc_order_archive_extract")["contract"]
        self.assertEqual(before["column_version"], after["column_version"])
        self.assertIsNone(after["rehearsal_id"])


class ScenarioTests(unittest.TestCase):
    """题目描述的操作序列：分别演练 → 只确认报表 → 检查取数 → 撤销确认。"""

    def setUp(self):
        self.service = ConsumerMigrationService()

    def test_confirm_only_report_then_check_reads_then_unconfirm(self):
        self.assertTrue(self.service.rehearse("rpt_sales_subtotal")["succeeded"])
        self.assertTrue(
            self.service.rehearse("arc_order_archive_extract")["succeeded"]
        )

        self.service.confirm("rpt_sales_subtotal")
        report = self.service.get_consumer("rpt_sales_subtotal")
        archive = self.service.get_consumer("arc_order_archive_extract")
        self.assertEqual(report["column_version"], engine.NEW_VERSION)
        self.assertEqual(report["amount_type"], "bigint")
        self.assertEqual(archive["column_version"], engine.OLD_VERSION)
        self.assertEqual(archive["contract"]["rehearsal_id"], None)

        reads = self.service.read_requests(
            partitions=["2026-11-13", "2026-11-14", "2026-11-15", "2026-11-16"]
        )
        by_key = {r["consumer_key"]: r for r in reads["results"]}
        self.assertIn("2026-11-15", by_key["rpt_sales_subtotal"]["readable_partitions"])
        self.assertIn(
            "2026-11-15", by_key["arc_order_archive_extract"]["blocked_partitions"]
        )
        # 报表切到新类型后，归档仍是旧列，两者共同可读的只剩旧分区
        self.assertEqual(
            reads["commonly_readable_partitions"], ["2026-11-13", "2026-11-14"]
        )
        # 每个分区在一次读取中只采用一个明确的列版本
        for result in reads["results"]:
            self.assertTrue(result["single_version_per_partition"])
            for read in result["reads"]:
                used_versions = {
                    column["type"] for column in read["columns"]
                }
                reference = {
                    column["type"] for column in engine.columns_of(read["column_version"])
                    if column["name"] in {c["name"] for c in read["columns"]}
                }
                self.assertEqual(used_versions, reference)

        restored = self.service.unconfirm("rpt_sales_subtotal")
        self.assertEqual(restored["restored_column_version"], engine.OLD_VERSION)
        self.assertEqual(restored["restored_amount_type"], "decimal(20,2)")
        self.assertEqual(restored["restored_input_partitions"], ["2026-11-13", "2026-11-14"])
        self.assertEqual(
            self.service.get_consumer("rpt_sales_subtotal")["input_partitions"],
            ["2026-11-13", "2026-11-14"],
        )

    def test_two_conflict_strategies_leave_different_readable_data(self):
        report = self.service.conflict()
        self.assertTrue(report["has_conflict"])
        advance, wait = report["strategies"]
        self.assertEqual(advance["strategy"], "advance_single_consumer")
        self.assertEqual(wait["strategy"], "wait_full_table_switch")
        # 先推进单个消费者：报表能读全部历史+新分区，归档只剩旧列分区
        self.assertGreater(advance["readable_count_any"], wait["readable_count_any"])
        archive_advance = [c for c in advance["consumers"]
                           if c["consumer_key"] == "arc_order_archive_extract"][0]
        self.assertIn("2026-11-15", archive_advance["blocked_partitions"])
        # 等待整表切换：新类型分区对所有人不可读
        self.assertIn("2026-11-15", wait["blocked_partitions"])
        self.assertEqual(advance["commonly_readable_count"], wait["commonly_readable_count"])

    def test_switch_entry_rollback_restores_columns_and_partitions(self):
        self.service.rehearse("rpt_sales_subtotal")
        self.service.confirm("rpt_sales_subtotal")
        entry = self.service.create_switch("rpt_sales_subtotal")
        self.assertEqual(entry["status"], "待提交")
        self.assertEqual(entry["rollback_column_version"], engine.OLD_VERSION)
        self.assertEqual(entry["rollback_input_partitions"], ["2026-11-13", "2026-11-14"])

        self.service.submit_switch(entry["switch_id"])
        rollback = self.service.rollback_switch(entry["switch_id"])
        self.assertEqual(rollback["status"], "已回滚")
        self.assertEqual(rollback["restored_input_partitions"], ["2026-11-13", "2026-11-14"])
        self.assertEqual(
            [c["type"] for c in rollback["restored_columns"] if c["name"] == "order_amount"],
            ["decimal(20,2)"],
        )

    def test_failed_switch_restores_original_mapping_and_source(self):
        self.service.rehearse("rpt_sales_subtotal")
        self.service.confirm("rpt_sales_subtotal")
        entry = self.service.create_switch("rpt_sales_subtotal")
        self.service.submit_switch(entry["switch_id"])

        result = self.service.fail_switch(entry["switch_id"], "模拟写入失败")
        self.assertEqual(result["status"], "切换失败")
        # 报表任务原本关联的是主表，恢复后数据源仍是主表、列回到旧版
        self.assertEqual(result["restored_data_source"], "dw.dwd_order_wide")
        self.assertEqual(result["restored_input_partitions"], ["2026-11-13", "2026-11-14"])
        contract = self.service.get_consumer("rpt_sales_subtotal")["contract"]
        self.assertEqual(contract["column_version"], engine.OLD_VERSION)
        self.assertTrue(any(
            m["kind"] == "drop_column" for m in contract["field_mapping"]
        ))

    def test_historical_reads_remain_queryable_after_rollback(self):
        self.service.rehearse("rpt_sales_subtotal")
        self.service.confirm("rpt_sales_subtotal")
        self.service.unconfirm("rpt_sales_subtotal")

        ledger = self.service.reads(
            consumer_key="rpt_sales_subtotal", column_version=engine.NEW_VERSION
        )
        self.assertGreater(ledger["total"], 0)
        for item in ledger["items"]:
            self.assertTrue(item["checksum_verified"])
            self.assertEqual(item["amount_type"], "bigint")

    def test_partition_read_blocks_version_mixing(self):
        consumer = self.service.get_consumer("rpt_sales_subtotal")["contract"]
        definition = self.service._consumers["rpt_sales_subtotal"]["definition"]
        reads = engine.read_consumer_partitions(
            definition,
            ["2026-11-13"],
            read_prefix="T",
            data_source=consumer["data_source"],
            default_version=engine.NEW_VERSION,
        )
        self.assertFalse(reads[0]["ok"])
        self.assertIn("实际列版本", reads[0]["issues"][0])


if __name__ == "__main__":
    unittest.main()
