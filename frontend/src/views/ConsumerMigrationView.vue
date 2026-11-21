<template>
  <section class="page" data-module="consumer_migration">
    <header class="page-head">
      <div>
        <h2>消费迁移演练</h2>
        <p class="page-desc">
          账务金额 decimal → bigint 的变更会列出影响表，这里补上「哪个读取作业、从哪一天起、能用哪一版列」：
          逐消费任务演练、确认读取契约、生成切换条目，并在失败时按原列映射和输入分区恢复。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh" type="button" @click="reload">刷新状态</button>
        <button class="btn primary" data-testid="run-all" type="button" :disabled="busy" @click="runAll">
          全部消费任务各跑一次演练
        </button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="error">{{ errorMessage }}</p>
    <p v-if="notice" class="notice-text" data-testid="notice">{{ notice }}</p>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <h3 class="section-title">新旧字段映射 <span class="muted">（{{ mapping?.change_id }} · {{ mapping?.table }}）</span></h3>
    <table class="data-table" data-testid="mapping-table">
      <thead>
        <tr><th>旧列</th><th>新列</th><th>旧类型</th><th>新类型</th><th>变更类型</th><th>转换/影响</th></tr>
      </thead>
      <tbody>
        <tr v-for="(item, index) in mapping?.mappings ?? []" :key="index">
          <td>{{ item.old_column ?? '—' }}</td>
          <td>{{ item.new_column ?? '—（下线）' }}</td>
          <td>{{ item.old_type ?? '—' }}</td>
          <td>{{ item.new_type ?? '—' }}</td>
          <td>{{ item.kind }}</td>
          <td>{{ item.transform }}</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">消费任务与读取契约</h3>
    <table class="data-table" data-testid="consumer-table">
      <thead>
        <tr>
          <th>消费任务</th><th>目标版本</th><th>当前可读版本</th><th>金额类型</th>
          <th>已验证列（日期）</th><th>关联数据源</th><th>输入分区</th><th>状态</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="consumer in consumers" :key="consumer.consumer_key" :data-testid="`consumer-${consumer.consumer_key}`">
          <td>
            <strong>{{ consumer.consumer_name }}</strong>
            <div class="muted small">{{ consumer.description }}</div>
          </td>
          <td>{{ consumer.desired_version }}</td>
          <td data-testid="consumer-version">{{ consumer.column_version }}</td>
          <td>{{ consumer.amount_type }}</td>
          <td>
            <div v-for="column in consumer.contract.verified_columns" :key="column.name" class="small">
              {{ column.name }} · {{ column.type }} <span class="muted">({{ column.verified_at }})</span>
            </div>
          </td>
          <td>{{ consumer.data_source }}</td>
          <td class="small">{{ consumer.input_partitions.join(', ') || '—' }}</td>
          <td>{{ consumer.status }}</td>
          <td class="row-actions">
            <button class="link" type="button" :disabled="busy"
                    :data-testid="`rehearse-${consumer.consumer_key}`"
                    @click="rehearse(consumer.consumer_key)">演练</button>
            <button class="link" type="button" :disabled="busy || !consumer.pending_rehearsal_id"
                    :data-testid="`confirm-${consumer.consumer_key}`"
                    @click="confirm(consumer.consumer_key)">确认切换</button>
            <button class="link" type="button" :disabled="busy || !consumer.previous_contract"
                    :data-testid="`unconfirm-${consumer.consumer_key}`"
                    @click="unconfirm(consumer.consumer_key)">撤销确认</button>
            <button class="link" type="button" :disabled="busy || consumer.status !== '已确认'"
                    :data-testid="`switch-${consumer.consumer_key}`"
                    @click="doSwitch(consumer.consumer_key)">执行切换</button>
          </td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">最近一次演练</h3>
    <div v-if="lastRehearsal" class="panel" data-testid="rehearsal-panel">
      <p>
        <strong>{{ lastRehearsal.rehearsal_id }}</strong>
        · {{ lastRehearsal.consumer_name }}
        · 目标 {{ lastRehearsal.target_version }}（{{ lastRehearsal.target_amount_type }}）
        · {{ lastRehearsal.succeeded ? '通过' : '未通过' }}
        · 成功 {{ lastRehearsal.success_count }}/{{ lastRehearsal.partition_count }} 个分区
      </p>
      <p v-if="lastRehearsal.issues.length" class="error-text small">
        {{ lastRehearsal.issues.join('；') }}
      </p>
      <table class="data-table small-table">
        <thead>
          <tr><th>分区</th><th>读取列版本</th><th>金额类型</th><th>结果</th><th>行数</th><th>校验和</th></tr>
        </thead>
        <tbody>
          <tr v-for="read in lastRehearsal.partition_reads" :key="read.read_id">
            <td>{{ read.partition }}</td>
            <td>{{ read.column_version }}<span class="muted"> ({{ read.version_source }})</span></td>
            <td>{{ read.amount_type ?? '—' }}</td>
            <td>{{ read.ok ? '可读' : read.issues.join('；') }}</td>
            <td>{{ read.row_count }}</td>
            <td class="mono">{{ read.checksum || '—' }}</td>
          </tr>
        </tbody>
      </table>
    </div>
    <p v-else class="muted">尚未执行演练。报表小计默认切到新类型，存量归档抽取演练旧列。</p>

    <h3 class="section-title">同一数据表两个消费者的切换冲突</h3>
    <div v-if="conflict" class="panel" data-testid="conflict-panel">
      <p>{{ conflict.conflict_summary }}</p>
      <p class="muted">建议：{{ conflict.recommendation }}</p>
      <div class="strategy-grid">
        <article v-for="strategy in conflict.strategies" :key="strategy.strategy" class="analysis-card">
          <h4>{{ strategy.label }}</h4>
          <p class="small">{{ strategy.explanation }}</p>
          <p class="small">
            任一消费者可读 <strong data-testid="readable-any">{{ strategy.readable_count_any }}</strong> 个分区；
            两者共同可读 <strong>{{ strategy.commonly_readable_count }}</strong> 个分区。
          </p>
          <ul class="small">
            <li v-for="item in strategy.consumers" :key="item.consumer_key">
              {{ item.consumer_name }}（{{ item.decided_version }} / {{ item.amount_type }}）：
              可读 {{ item.readable_count }} 个分区
              <span v-if="item.blocked_partitions.length" class="error-text">
                ，不可读 {{ item.blocked_partitions.join(', ') }}
              </span>
            </li>
          </ul>
        </article>
      </div>
      <button class="btn" type="button" :disabled="busy" data-testid="read-requests" @click="simulateReads">
        同时接受两类读请求
      </button>
      <table v-if="readResult" class="data-table small-table" data-testid="read-result">
        <thead>
          <tr><th>消费任务</th><th>列版本</th><th>金额类型</th><th>单次读取单版本</th><th>可读分区</th><th>不可读分区</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in readResult.results" :key="item.consumer_key">
            <td>{{ item.consumer_name }}</td>
            <td>{{ item.column_version }}</td>
            <td>{{ item.amount_type }}</td>
            <td>{{ item.single_version_per_partition ? '是' : '否' }}</td>
            <td>{{ item.readable_partitions.join(', ') || '—' }}</td>
            <td>{{ item.blocked_partitions.join(', ') || '—' }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <h3 class="section-title">切换条目（配置迁移产出）</h3>
    <p class="small muted">
      下面把每个「已确认」的消费方读取契约转换成可提交的切换条目；未确认的消费任务会被跳过，
      条目中的回滚快照保留切换前可读的列、数据源和输入分区。
    </p>
    <button class="btn" type="button" :disabled="busy" data-testid="build-entries" @click="buildEntries">
      按读取契约生成切换条目
    </button>
    <p v-if="configResult" class="small" data-testid="config-result">
      生成 {{ configResult.entry_count }} 条；
      <span v-if="configResult.skipped.length" class="error-text">
        跳过：{{ configResult.skipped.map((item) => `${item.consumer_key}（${item.reason}）`).join('；') }}
      </span>
      <span v-else>全部已确认消费任务均已转换。</span>
    </p>
    <table class="data-table" data-testid="switch-table">
      <thead>
        <tr><th>切换条目</th><th>消费任务</th><th>目标版本</th><th>可提交列</th><th>输入分区</th><th>回滚恢复列版本</th><th>状态</th><th>操作</th></tr>
      </thead>
      <tbody>
        <tr v-for="item in switches" :key="item.switch_id">
          <td>{{ item.switch_id }}</td>
          <td>{{ item.consumer_name }}</td>
          <td>{{ item.column_version }} / {{ item.amount_type }}</td>
          <td class="small">{{ item.verified_columns.map((c) => `${c.name}:${c.type}`).join(', ') }}</td>
          <td class="small">{{ item.input_partitions.join(', ') }}</td>
          <td>{{ item.rollback_column_version }} / {{ item.rollback_amount_type }}</td>
          <td>{{ item.status }}</td>
          <td class="row-actions">
            <button class="link" type="button" :disabled="busy || item.status !== '待提交'"
                    @click="submitSwitchEntry(item.switch_id)">提交</button>
            <button v-if="item.status === '已切换'" class="link" type="button" :disabled="busy"
                    :data-testid="`fail-${item.switch_id}`"
                    @click="failSwitchEntry(item.switch_id)">标记失败并恢复</button>
            <button v-if="item.status === '已切换'" class="link" type="button" :disabled="busy"
                    @click="rollbackSwitchEntry(item.switch_id)">回滚</button>
          </td>
        </tr>
        <tr v-if="!switches.length">
          <td colspan="8" class="empty-state">还没有切换条目：先确认某个消费任务的读取契约，再执行切换。</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">已核验历史读数（可回查）</h3>
    <div class="filter-bar">
      <label class="filter-item">
        <span>消费任务</span>
        <select v-model="ledgerConsumer" data-testid="ledger-consumer" @change="refreshReads">
          <option value="">全部</option>
          <option v-for="consumer in consumers" :key="consumer.consumer_key" :value="consumer.consumer_key">
            {{ consumer.consumer_name }}
          </option>
        </select>
      </label>
      <label class="filter-item">
        <span>列版本</span>
        <select v-model="ledgerVersion" data-testid="ledger-version" @change="refreshReads">
          <option value="">全部</option>
          <option value="v1">v1</option>
          <option value="v2">v2</option>
        </select>
      </label>
    </div>
    <table class="data-table" data-testid="ledger-table">
      <thead>
        <tr><th>读数</th><th>消费任务</th><th>分区</th><th>列版本</th><th>金额类型</th><th>行数</th><th>校验和</th><th>校验和复核</th><th>读取时间</th></tr>
      </thead>
      <tbody>
        <tr v-for="read in ledger" :key="read.read_id">
          <td>{{ read.read_id }}</td>
          <td>{{ read.consumer_name }}</td>
          <td>{{ read.partition }}</td>
          <td>{{ read.column_version }}</td>
          <td>{{ read.amount_type }}</td>
          <td>{{ read.row_count }}</td>
          <td class="mono">{{ read.checksum }}</td>
          <td>{{ read.checksum_verified ? '一致' : '不一致' }}</td>
          <td class="small">{{ read.read_at }}</td>
        </tr>
        <tr v-if="!ledger.length">
          <td colspan="9" class="empty-state">还没有成功读数；先执行演练或同时读取。</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  buildConfigMigrationEntries,
  confirmContract,
  createSwitch,
  failSwitch,
  getFieldMapping,
  listConsumers,
  listSwitches,
  loadConflict,
  loadReadRequests,
  loadReads,
  rollbackSwitch,
  runRehearsal,
  submitSwitch,
  unconfirmContract,
  type ConfigMigrationResult,
  type ConflictReport,
  type ConsumerInfo,
  type FieldMappingResult,
  type LedgerRead,
  type ReadRequestResult,
  type Rehearsal,
  type RestoreResult,
  type SwitchEntry,
} from '@/api/consumer-migration'

const consumers = ref<ConsumerInfo[]>([])
const mapping = ref<FieldMappingResult | null>(null)
const conflict = ref<ConflictReport | null>(null)
const switches = ref<SwitchEntry[]>([])
const ledger = ref<LedgerRead[]>([])
const lastRehearsal = ref<Rehearsal | null>(null)
const readResult = ref<ReadRequestResult | null>(null)
const configResult = ref<ConfigMigrationResult | null>(null)
const ledgerConsumer = ref('')
const ledgerVersion = ref('')
const busy = ref(false)
const errorMessage = ref('')
const notice = ref('')

const stats = computed(() => [
  { label: '消费任务数', value: consumers.value.length },
  { label: '已确认契约', value: consumers.value.filter((c) => c.status === '已确认' || c.status === '已切换').length },
  { label: '切换失败', value: consumers.value.filter((c) => c.status === '切换失败').length },
])

function failMessage(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback
}

function describeRestore(result: RestoreResult): string {
  const partitions = result.restored_input_partitions?.join(', ') ?? '—'
  const columns = result.restored_columns?.map((c) => `${c.name}:${c.type}`).join(', ') ?? '—'
  return `${result.message}；恢复列 ${columns}；恢复输入分区 ${partitions}`
}

async function reload() {
  errorMessage.value = ''
  try {
    const [consumerList, switchList] = await Promise.all([listConsumers(), listSwitches()])
    consumers.value = consumerList
    switches.value = switchList
    mapping.value = await getFieldMapping()
    conflict.value = await loadConflict()
    await refreshReads()
  } catch (error) {
    errorMessage.value = failMessage(error, '状态读取失败')
  }
}

async function refreshReads() {
  try {
    ledger.value = await loadReads(ledgerConsumer.value, ledgerVersion.value)
  } catch (error) {
    errorMessage.value = failMessage(error, '历史读数回查失败')
  }
}

async function rehearse(consumerKey: string) {
  busy.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    const result = await runRehearsal(consumerKey)
    lastRehearsal.value = result
    notice.value = `${result.consumer_name}演练${result.succeeded ? '通过' : '未通过'}：${result.rehearsal_id}`
    await reload()
  } catch (error) {
    errorMessage.value = failMessage(error, '迁移演练失败')
  } finally {
    busy.value = false
  }
}

async function runAll() {
  busy.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    for (const consumer of consumers.value) {
      const result = await runRehearsal(consumer.consumer_key)
      lastRehearsal.value = result
    }
    notice.value = '两个消费任务已分别演练：报表小计验证新类型，存量归档抽取验证旧列。'
    await reload()
  } catch (error) {
    errorMessage.value = failMessage(error, '批量演练失败')
  } finally {
    busy.value = false
  }
}

async function confirm(consumerKey: string) {
  busy.value = true
  errorMessage.value = ''
  try {
    const result = await confirmContract(consumerKey)
    notice.value = result.message
    await reload()
  } catch (error) {
    errorMessage.value = failMessage(error, '确认切换失败')
  } finally {
    busy.value = false
  }
}

async function unconfirm(consumerKey: string) {
  busy.value = true
  errorMessage.value = ''
  try {
    const result = await unconfirmContract(consumerKey)
    notice.value = describeRestore(result)
    await reload()
  } catch (error) {
    errorMessage.value = failMessage(error, '撤销确认失败')
  } finally {
    busy.value = false
  }
}

async function doSwitch(consumerKey: string) {
  busy.value = true
  errorMessage.value = ''
  try {
    const entry = await createSwitch(consumerKey)
    const submitted = await submitSwitch(entry.switch_id)
    notice.value = submitted.message
    await reload()
  } catch (error) {
    errorMessage.value = failMessage(error, '切换失败')
  } finally {
    busy.value = false
  }
}

async function buildEntries() {
  busy.value = true
  errorMessage.value = ''
  try {
    configResult.value = await buildConfigMigrationEntries()
    notice.value = `配置迁移已生成 ${configResult.value.entry_count} 条切换条目`
    await reload()
  } catch (error) {
    errorMessage.value = failMessage(error, '读取契约转换失败')
  } finally {
    busy.value = false
  }
}

async function submitSwitchEntry(switchId: string) {
  busy.value = true
  errorMessage.value = ''
  try {
    notice.value = (await submitSwitch(switchId)).message
    await reload()
  } catch (error) {
    errorMessage.value = failMessage(error, '提交切换失败')
  } finally {
    busy.value = false
  }
}

async function failSwitchEntry(switchId: string) {
  busy.value = true
  errorMessage.value = ''
  try {
    notice.value = describeRestore(await failSwitch(switchId))
    await reload()
  } catch (error) {
    errorMessage.value = failMessage(error, '切换失败恢复失败')
  } finally {
    busy.value = false
  }
}

async function rollbackSwitchEntry(switchId: string) {
  busy.value = true
  errorMessage.value = ''
  try {
    notice.value = describeRestore(await rollbackSwitch(switchId))
    await reload()
  } catch (error) {
    errorMessage.value = failMessage(error, '回滚失败')
  } finally {
    busy.value = false
  }
}

async function simulateReads() {
  busy.value = true
  errorMessage.value = ''
  try {
    readResult.value = await loadReadRequests()
    await refreshReads()
  } catch (error) {
    errorMessage.value = failMessage(error, '同时读取失败')
  } finally {
    busy.value = false
  }
}

onMounted(reload)
</script>
