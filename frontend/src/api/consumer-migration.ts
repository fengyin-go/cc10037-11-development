/** 消费任务迁移演练接口封装。 */
import { request } from './client'

async function readJson<T>(response: Response, message: string): Promise<T> {
  if (!response.ok) {
    let detail = message
    try {
      const payload = (await response.json()) as { detail?: string }
      if (payload.detail) detail = payload.detail
    } catch {
      /* 保留默认错误 */
    }
    throw new Error(detail)
  }
  return (await response.json()) as T
}

export type FieldMapping = {
  old_column: string | null
  new_column: string | null
  old_type: string | null
  new_type: string | null
  kind: string
  transform: string
  compatible: boolean
  source_text?: string
}

export type FieldMappingResult = {
  change_id: string
  change_detail: string
  table: string
  old_version: string
  new_version: string
  mappings: FieldMapping[]
  mapping_count: number
  type_change_count: number
}

export type VerifiedColumn = {
  name: string
  type: string
  versions: string[]
  partitions: string[]
  verified_at: string
}

export type ConsumerContract = {
  consumer_key: string
  consumer_name: string
  table: string
  data_source: string
  column_version: string
  amount_type: string
  verified_columns: VerifiedColumn[]
  input_partitions: string[]
  verified_at: string
  rehearsal_id: string | null
  field_mapping: FieldMapping[]
  updated_at: string
}

export type ConsumerInfo = {
  consumer_key: string
  consumer_name: string
  table: string
  description: string
  data_source: string
  original_data_source: string
  required_columns: string[]
  desired_version: string
  capability_versions: string[]
  column_version: string
  amount_type: string
  input_partitions: string[]
  verified_at: string
  contract: ConsumerContract
  previous_contract: ConsumerContract | null
  status: string
  pending_rehearsal_id: string | null
}

export type PartitionRead = {
  read_id: string
  consumer_key: string
  consumer_name: string
  table: string
  data_source: string
  partition: string
  column_version: string
  version_source: string
  single_version: boolean
  ok: boolean
  columns: { name: string; type: string }[]
  verified_columns: { name: string; type: string }[]
  issues: string[]
  row_count: number
  checksum: string
  amount_type?: string
  read_at: string
}

export type Rehearsal = {
  rehearsal_id: string
  consumer_key: string
  consumer_name: string
  table: string
  data_source: string
  target_version: string
  target_amount_type: string
  target_partitions: string[]
  partition_reads: PartitionRead[]
  partition_count: number
  success_count: number
  failed_count: number
  verified_columns: VerifiedColumn[]
  verified_at: string
  succeeded: boolean
  issues: string[]
  field_mapping: FieldMapping[]
}

export type StrategyView = {
  strategy: string
  label: string
  decisions: Record<string, string>
  consumers: {
    consumer_key: string
    consumer_name: string
    decided_version: string
    readable_count: number
    readable_partitions: { partition: string; column_version: string; row_count: number }[]
    blocked_partitions: string[]
    amount_type: string
  }[]
  readable_partitions_any: string[]
  readable_count_any: number
  commonly_readable_partitions: string[]
  commonly_readable_count: number
  blocked_partitions: string[]
  explanation: string
}

export type ConflictReport = {
  table: string
  has_conflict: boolean
  conflict_summary: string
  strategies: StrategyView[]
  recommendation: string
  consumer_contracts: {
    consumer_key: string
    consumer_name: string
    column_version: string
    amount_type: string
    desired_version: string
    data_source: string
    input_partitions: string[]
  }[]
}

export type ReadRequestResult = {
  table: string
  request_count: number
  results: {
    consumer_key: string
    consumer_name: string
    requested_version: string
    data_source: string
    column_version: string
    amount_type: string
    single_version_per_partition: boolean
    reads: PartitionRead[]
    readable_partitions: string[]
    blocked_partitions: string[]
    issues: string[]
  }[]
}

export type SwitchEntry = {
  switch_id: string
  consumer_key: string
  consumer_name: string
  table: string
  source_env: string
  target_env: string
  column_version: string
  amount_type: string
  data_source: string
  input_partitions: string[]
  verified_columns: VerifiedColumn[]
  rollback_columns: VerifiedColumn[]
  rollback_column_version: string
  rollback_amount_type: string
  rollback_data_source: string
  rollback_input_partitions: string[]
  status: string
  created_at: string
  failure_reason: string | null
}

export type RestoreResult = {
  ok: boolean
  restored_input_partitions?: string[]
  restored_amount_type?: string
  restored_column_version?: string
  restored_data_source?: string
  restored_columns?: { name: string; type: string }[]
  rolled_back_switches?: string[]
  message: string
}

export async function listConsumers(): Promise<ConsumerInfo[]> {
  const response = await request('/api/consumer-migration/consumers')
  const payload = await readJson<{ items: ConsumerInfo[] }>(response, '消费任务读取失败')
  return payload.items
}

export async function getFieldMapping(): Promise<FieldMappingResult> {
  const response = await request('/api/consumer-migration/field-mapping')
  return readJson<FieldMappingResult>(response, '字段映射解析失败')
}

export async function runRehearsal(consumerKey: string): Promise<Rehearsal> {
  const response = await request('/api/consumer-migration/rehearsals', {
    method: 'POST',
    body: JSON.stringify({ consumer_key: consumerKey }),
  })
  return readJson<Rehearsal>(response, '迁移演练失败')
}

export async function confirmContract(consumerKey: string): Promise<{ contract: ConsumerContract; message: string }> {
  const response = await request(`/api/consumer-migration/consumers/${consumerKey}/confirm`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
  return readJson(response, '确认切换失败')
}

export async function unconfirmContract(consumerKey: string): Promise<RestoreResult> {
  const response = await request(`/api/consumer-migration/consumers/${consumerKey}/unconfirm`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
  return readJson<RestoreResult>(response, '撤销确认失败')
}

export async function loadConflict(): Promise<ConflictReport> {
  const response = await request('/api/consumer-migration/conflict')
  return readJson<ConflictReport>(response, '冲突分析读取失败')
}

export async function loadReadRequests(): Promise<ReadRequestResult> {
  const response = await request('/api/consumer-migration/read-requests', {
    method: 'POST',
    body: JSON.stringify({}),
  })
  return readJson<ReadRequestResult>(response, '同时读取失败')
}

export async function listSwitches(): Promise<SwitchEntry[]> {
  const response = await request('/api/consumer-migration/switches')
  const payload = await readJson<{ items: SwitchEntry[] }>(response, '切换条目读取失败')
  return payload.items
}

export async function createSwitch(consumerKey: string): Promise<SwitchEntry> {
  const response = await request('/api/consumer-migration/switches', {
    method: 'POST',
    body: JSON.stringify({ consumer_key: consumerKey }),
  })
  return readJson<SwitchEntry>(response, '生成切换条目失败')
}

export type ConfigMigrationResult = {
  source_env: string
  target_env: string
  entry_count: number
  skipped: { consumer_key: string; reason: string }[]
  entries: {
    switch_id: string
    consumer_key: string
    consumer_name: string
    column_version: string
    amount_type: string
    input_partitions: string[]
    rollback_snapshot: {
      column_version: string
      amount_type: string
      data_source: string
      input_partitions: string[]
      columns: { name: string; type: string }[]
    }
    status: string
  }[]
}

export async function buildConfigMigrationEntries(): Promise<ConfigMigrationResult> {
  const response = await request('/api/consumer-migration/config-migration/entries', {
    method: 'POST',
    body: JSON.stringify({}),
  })
  return readJson<ConfigMigrationResult>(response, '读取契约转换失败')
}

export async function submitSwitch(switchId: string): Promise<{ status: string; message: string }> {
  const response = await request(`/api/consumer-migration/switches/${switchId}/submit`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
  return readJson(response, '提交切换失败')
}

export async function failSwitch(switchId: string): Promise<RestoreResult> {
  const response = await request(`/api/consumer-migration/switches/${switchId}/fail`, {
    method: 'POST',
    body: JSON.stringify({ reason: '演练页面标记切换失败' }),
  })
  return readJson<RestoreResult>(response, '切换失败恢复失败')
}

export async function rollbackSwitch(switchId: string): Promise<RestoreResult> {
  const response = await request(`/api/consumer-migration/switches/${switchId}/rollback`, {
    method: 'POST',
    body: JSON.stringify({ reason: '演练页面回滚' }),
  })
  return readJson<RestoreResult>(response, '回滚切换失败')
}

export type LedgerRead = {
  read_id: string
  consumer_key: string
  consumer_name: string
  partition: string
  column_version: string
  data_source: string
  row_count: number
  checksum: string
  amount_type: string
  checksum_verified: boolean
  read_at: string
}

export async function loadReads(consumerKey?: string, columnVersion?: string): Promise<LedgerRead[]> {
  const query = new URLSearchParams()
  if (consumerKey) query.set('consumer_key', consumerKey)
  if (columnVersion) query.set('column_version', columnVersion)
  const response = await request(`/api/consumer-migration/reads?${query.toString()}`)
  const payload = await readJson<{ items: LedgerRead[] }>(response, '历史读数回查失败')
  return payload.items
}
