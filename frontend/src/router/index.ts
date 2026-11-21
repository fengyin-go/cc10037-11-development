import { createRouter, createWebHistory } from 'vue-router'

import ConsumerMigrationView from '@/views/ConsumerMigrationView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 消费迁移演练有独立的演练/契约/切换面板，优先于通用工作台匹配
    { path: '/consumer_migration', name: 'consumer-migration', component: ConsumerMigrationView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
