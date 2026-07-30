/**
 * KAFKA GO MANAGEMENT UI DASHBOARD - CLIENT ENGINE
 */

(function () {
  // STATE MANAGEMENT
  const state = {
    theme: localStorage.getItem('kafka_theme') || 'dark',
    activeTab: 'overviewTab',
    topics: [],
    groups: [],
    selectedTopic: '',
    selectedPartition: 0,
    wsConnected: false,
  };

  // TAB TITLE MAPPING
  const tabTitles = {
    overviewTab: 'Cluster Overview',
    topicsTab: 'Topics & Partitions Directory',
    messagesTab: 'Live Message Stream Inspector',
    groupsTab: 'Consumer Groups & Lag Monitor',
    logsTab: 'Broker Operational Logs',
  };

  // DOM ELEMENTS
  const themeToggleBtn = document.getElementById('themeToggleBtn');
  const themeBtnText = document.getElementById('themeBtnText');
  const pageTitleHeading = document.getElementById('pageTitleHeading');
  const navLinks = document.querySelectorAll('.nav-link');
  const tabPanels = document.querySelectorAll('.tab-panel');

  const nodeStatusText = document.getElementById('nodeStatusText');
  const sidebarNodePill = document.getElementById('sidebarNodePill');
  const sidebarUptimePill = document.getElementById('sidebarUptimePill');

  const statTotalTopics = document.getElementById('statTotalTopics');
  const statPartitionsCount = document.getElementById('statPartitionsCount');
  const statTotalMessages = document.getElementById('statTotalMessages');
  const statMsgRate = document.getElementById('statMsgRate');
  const statStorageSize = document.getElementById('statStorageSize');
  const statDataDir = document.getElementById('statDataDir');
  const statThroughput = document.getElementById('statThroughput');
  const statThroughputSub = document.getElementById('statThroughputSub');
  const statMemory = document.getElementById('statMemory');
  const statGoroutines = document.getElementById('statGoroutines');

  const msgTopicSelect = document.getElementById('msgTopicSelect');
  const msgPartitionSelect = document.getElementById('msgPartitionSelect');
  const msgOffsetInput = document.getElementById('msgOffsetInput');
  const fetchMessagesBtn = document.getElementById('fetchMessagesBtn');
  const messagesTableBody = document.getElementById('messagesTableBody');
  const topicsTableBody = document.getElementById('topicsTableBody');
  const groupsContainer = document.getElementById('groupsContainer');
  const consoleTerminal = document.getElementById('consoleTerminal');

  // INITIALIZATION
  function init() {
    setupTheme();
    setupTabs();
    setupEventListeners();
    fetchClusterOverview();
    fetchTopics();
    fetchGroups();
    connectWebSocket();
    logConsole('System', 'Web Management Dashboard initialized.', 'success');
  }

  // THEME MANAGEMENT
  function setupTheme() {
    document.documentElement.setAttribute('data-theme', state.theme);
    themeBtnText.textContent = state.theme === 'dark' ? 'Theme: Dark' : 'Theme: Light';

    themeToggleBtn.addEventListener('click', () => {
      state.theme = state.theme === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', state.theme);
      themeBtnText.textContent = state.theme === 'dark' ? 'Theme: Dark' : 'Theme: Light';
      localStorage.setItem('kafka_theme', state.theme);
    });
  }

  // TABS MANAGEMENT
  function setupTabs() {
    navLinks.forEach(link => {
      link.addEventListener('click', () => {
        const tabId = link.getAttribute('data-tab');

        navLinks.forEach(l => l.classList.remove('active'));
        tabPanels.forEach(p => p.classList.remove('active'));

        link.classList.add('active');
        document.getElementById(tabId).classList.add('active');
        state.activeTab = tabId;

        if (pageTitleHeading && tabTitles[tabId]) {
          pageTitleHeading.textContent = tabTitles[tabId];
        }

        if (tabId === 'topicsTab') fetchTopics();
        if (tabId === 'groupsTab') fetchGroups();
        if (tabId === 'messagesTab' && state.selectedTopic) fetchMessages();
      });
    });
  }

  // EVENT LISTENERS
  function setupEventListeners() {
    document.getElementById('globalRefreshBtn').addEventListener('click', () => {
      fetchClusterOverview();
      fetchTopics();
      fetchGroups();
      if (state.selectedTopic) fetchMessages();
    });

    document.getElementById('refreshTopicsBtn').addEventListener('click', fetchTopics);
    document.getElementById('refreshGroupsBtn').addEventListener('click', fetchGroups);
    fetchMessagesBtn.addEventListener('click', fetchMessages);

    const latestBtn = document.getElementById('latestOffsetBtn');
    if (latestBtn) {
      latestBtn.addEventListener('click', () => {
        const topic = state.topics.find(t => t.topic_name === state.selectedTopic);
        if (topic && topic.partitions && topic.partitions.length > 0) {
          const p = topic.partitions.find(part => part.partition_id === state.selectedPartition) || topic.partitions[0];
          if (p && p.leo > 0) {
            const limitSelect = document.getElementById('msgLimitSelect');
            const limit = limitSelect ? parseInt(limitSelect.value, 10) : 50;
            msgOffsetInput.value = Math.max(0, p.leo - limit);
            fetchMessages();
          }
        }
      });
    }

    const btnFirst = document.getElementById('btnPageFirst');
    const btnPrev = document.getElementById('btnPagePrev');
    const btnNext = document.getElementById('btnPageNext');
    const btnLast = document.getElementById('btnPageLast');
    const limitSelect = document.getElementById('msgLimitSelect');

    if (limitSelect) limitSelect.addEventListener('change', fetchMessages);

    if (btnFirst) {
      btnFirst.addEventListener('click', () => {
        msgOffsetInput.value = 0;
        fetchMessages();
      });
    }

    if (btnPrev) {
      btnPrev.addEventListener('click', () => {
        const limit = limitSelect ? parseInt(limitSelect.value, 10) : 50;
        const curr = parseInt(msgOffsetInput.value, 10) || 0;
        msgOffsetInput.value = Math.max(0, curr - limit);
        fetchMessages();
      });
    }

    if (btnNext) {
      btnNext.addEventListener('click', () => {
        const limit = limitSelect ? parseInt(limitSelect.value, 10) : 50;
        const curr = parseInt(msgOffsetInput.value, 10) || 0;
        msgOffsetInput.value = curr + limit;
        fetchMessages();
      });
    }

    if (btnLast) {
      btnLast.addEventListener('click', () => {
        const topic = state.topics.find(t => t.topic_name === state.selectedTopic);
        if (topic && topic.partitions && topic.partitions.length > 0) {
          const p = topic.partitions.find(part => part.partition_id == state.selectedPartition) || topic.partitions[0];
          if (p) {
            const limit = limitSelect ? parseInt(limitSelect.value, 10) : 50;
            msgOffsetInput.value = Math.max(0, p.leo - limit);
            fetchMessages();
          }
        }
      });
    }

    document.getElementById('clearLogsBtn').addEventListener('click', () => {
      consoleTerminal.innerHTML = '';
      logConsole('System', 'Log terminal cleared.', 'info');
    });

    msgTopicSelect.addEventListener('change', (e) => {
      state.selectedTopic = e.target.value;
      populatePartitionsDropdown();
      fetchMessages();
    });

    msgPartitionSelect.addEventListener('change', (e) => {
      state.selectedPartition = parseInt(e.target.value, 10);
      fetchMessages();
    });
  }

  // WEBSOCKET TELEMETRY STREAM
  function connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws/stream`;

    const ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      state.wsConnected = true;
      nodeStatusText.textContent = 'BROKER ONLINE';
      logConsole('WebSocket', 'Connected to real-time telemetry stream.', 'success');
    };

    ws.onmessage = (evt) => {
      try {
        const data = JSON.parse(evt.data);
        if (data.type === 'telemetry_tick') {
          updateMetricsUI(data.metrics);
          if (data.topics) renderTopicsTable(data.topics);
          if (data.groups) renderGroups(data.groups);
        }
      } catch (err) {
        console.error('Failed to parse WS telemetry frame:', err);
      }
    };

    ws.onclose = () => {
      state.wsConnected = false;
      nodeStatusText.textContent = 'RECONNECTING...';
      logConsole('WebSocket', 'Connection lost. Retrying in 3s...', 'warn');
      setTimeout(connectWebSocket, 3000);
    };

    ws.onerror = (err) => {
      console.error('WebSocket error:', err);
    };
  }

  // FETCH CLUSTER OVERVIEW
  async function fetchClusterOverview() {
    try {
      const res = await fetch('/api/v1/cluster');
      if (!res.ok) return;
      const data = await res.json();

      sidebarNodePill.textContent = `Node ${data.node_id} | ${data.host}:${data.port}`;
      statTotalTopics.textContent = data.total_topics;
      statPartitionsCount.textContent = `${data.total_partitions} partitions total`;
      statTotalMessages.textContent = formatNumber(data.total_messages);
      statStorageSize.textContent = formatBytes(data.total_bytes);
      statDataDir.textContent = data.data_dir;

      document.getElementById('ovNodeId').textContent = data.node_id;
      document.getElementById('ovAddress').textContent = `${data.host}:${data.port}`;
      document.getElementById('ovDataDir').textContent = data.data_dir;

      updateMetricsUI(data.metrics);
    } catch (err) {
      console.error('Error fetching cluster overview:', err);
    }
  }

  function updateMetricsUI(metrics) {
    if (!metrics) return;

    sidebarUptimePill.textContent = `Uptime: ${formatDuration(metrics.uptime_seconds)}`;
    statMsgRate.textContent = `${metrics.msg_in_rate.toFixed(1)} msg/sec`;

    const totalRate = metrics.bytes_in_rate + metrics.bytes_out_rate;
    statThroughput.textContent = `${formatBytes(totalRate)}/s`;
    statThroughputSub.textContent = `In: ${formatBytes(metrics.bytes_in_rate)}/s | Out: ${formatBytes(metrics.bytes_out_rate)}/s`;

    statMemory.textContent = `${metrics.alloc_heap_mb.toFixed(1)} MB`;
    statGoroutines.textContent = `Goroutines: ${metrics.goroutines} | WS: ${metrics.ws_subscribers || 0}`;

    // Overview Tab fields
    document.getElementById('ovUptime').textContent = formatDuration(metrics.uptime_seconds);
    document.getElementById('ovGoroutines').textContent = metrics.goroutines;
    document.getElementById('ovMemStats').textContent = `${metrics.alloc_heap_mb.toFixed(1)} MB / ${metrics.sys_mem_mb.toFixed(1)} MB`;
    document.getElementById('ovNumGC').textContent = metrics.num_gc;
    document.getElementById('ovMsgRate').textContent = `${metrics.msg_in_rate.toFixed(1)} /s`;
    document.getElementById('ovBytesIn').textContent = `${formatBytes(metrics.bytes_in_rate)}/s`;
    document.getElementById('ovBytesOut').textContent = `${formatBytes(metrics.bytes_out_rate)}/s`;
    document.getElementById('ovWsClients').textContent = metrics.ws_subscribers || 0;
  }

  // FETCH TOPICS
  async function fetchTopics() {
    try {
      const res = await fetch('/api/v1/topics');
      if (!res.ok) return;
      const topics = await res.json();
      state.topics = topics;
      renderTopicsTable(topics);
      updateTopicDropdown(topics);
    } catch (err) {
      console.error('Error fetching topics:', err);
    }
  }

  function renderTopicsTable(topics) {
    if (!topics || topics.length === 0) {
      topicsTableBody.innerHTML = `<tr><td colspan="5" style="text-align:center; color:var(--text-muted);">No topics created yet. Produce messages to auto-create topics.</td></tr>`;
      return;
    }

    topicsTableBody.innerHTML = topics.map(t => `
      <tr>
        <td><strong class="mono" style="color:var(--accent);">${escapeHtml(t.topic_name)}</strong></td>
        <td><span class="badge badge-blue">${t.partitions_count} Partitions</span></td>
        <td class="mono">${formatNumber(t.total_messages)}</td>
        <td class="mono">${formatBytes(t.total_size_bytes)}</td>
        <td>
          <button class="btn" onclick="window.selectTopicView('${escapeHtml(t.topic_name)}')">Inspect Messages</button>
        </td>
      </tr>
    `).join('');
  }

  function updateTopicDropdown(topics) {
    const prevSelected = msgTopicSelect.value;
    msgTopicSelect.innerHTML = `<option value="">-- Select Topic --</option>` +
      topics.map(t => `<option value="${escapeHtml(t.topic_name)}">${escapeHtml(t.topic_name)}</option>`).join('');

    if (prevSelected && topics.some(t => t.topic_name === prevSelected)) {
      msgTopicSelect.value = prevSelected;
    } else if (topics.length > 0 && !state.selectedTopic) {
      state.selectedTopic = topics[0].topic_name;
      msgTopicSelect.value = state.selectedTopic;
      populatePartitionsDropdown();
    }
  }

  function populatePartitionsDropdown() {
    const topic = state.topics.find(t => t.topic_name === state.selectedTopic);
    if (!topic || !topic.partitions) {
      msgPartitionSelect.innerHTML = `<option value="0">Partition 0</option>`;
      return;
    }

    msgPartitionSelect.innerHTML = topic.partitions.map(p =>
      `<option value="${p.partition_id}">Partition ${p.partition_id} (LEO: ${p.leo})</option>`
    ).join('');
  }

  window.selectTopicView = function (topicName) {
    state.selectedTopic = topicName;
    msgTopicSelect.value = topicName;
    populatePartitionsDropdown();

    // Switch tab
    navLinks.forEach(l => l.classList.remove('active'));
    tabPanels.forEach(p => p.classList.remove('active'));

    const msgBtn = document.querySelector('[data-tab="messagesTab"]');
    if (msgBtn) msgBtn.classList.add('active');
    document.getElementById('messagesTab').classList.add('active');

    if (pageTitleHeading) pageTitleHeading.textContent = tabTitles['messagesTab'];

    fetchMessages();
  };

  // FETCH MESSAGES
  async function fetchMessages() {
    if (!state.selectedTopic) return;

    const partition = msgPartitionSelect.value || 0;
    const offset = msgOffsetInput.value || 0;
    const limitSelect = document.getElementById('msgLimitSelect');
    const limit = limitSelect ? limitSelect.value : 50;

    try {
      const res = await fetch(`/api/v1/topics/${encodeURIComponent(state.selectedTopic)}/messages?partition=${partition}&offset=${offset}&limit=${limit}`);
      if (!res.ok) return;
      const data = await res.json();

      let msgs = [];
      let leo = 0;
      let reqOffset = parseInt(offset, 10);
      let pageLimit = parseInt(limit, 10);

      if (Array.isArray(data)) {
        msgs = data;
      } else if (data && data.messages) {
        msgs = data.messages;
        leo = data.leo || 0;
        reqOffset = data.offset || 0;
        pageLimit = data.limit || 50;
      }

      renderMessagesTable(msgs);
      updatePaginationControls(msgs, reqOffset, pageLimit, leo);
    } catch (err) {
      console.error('Error fetching messages:', err);
    }
  }

  function renderMessagesTable(msgs) {
    if (!msgs || msgs.length === 0) {
      messagesTableBody.innerHTML = `<tr><td colspan="5" style="text-align:center; color:var(--text-muted);">No messages found for this partition/offset range.</td></tr>`;
      return;
    }

    messagesTableBody.innerHTML = msgs.map(m => `
      <tr>
        <td class="mono" style="color:var(--accent); font-weight:600;">${m.offset}</td>
        <td class="mono" style="color:var(--text-muted);">${formatTimestamp(m.timestamp)}</td>
        <td class="mono">${escapeHtml(m.key || '-')}</td>
        <td class="mono" style="word-break:break-all;">${formatPayload(m.value)}</td>
        <td class="mono" style="color:var(--text-muted);">${m.size} B</td>
      </tr>
    `).join('');
  }

  function updatePaginationControls(msgs, offset, limit, leo) {
    const infoText = document.getElementById('paginationInfoText');
    const pageDisplay = document.getElementById('currentPageDisplay');
    const btnFirst = document.getElementById('btnPageFirst');
    const btnPrev = document.getElementById('btnPagePrev');
    const btnNext = document.getElementById('btnPageNext');
    const btnLast = document.getElementById('btnPageLast');

    if (!infoText) return;

    if (!msgs || msgs.length === 0) {
      infoText.textContent = `Showing records offset 0 - 0 of ${formatNumber(leo)}`;
      if (pageDisplay) pageDisplay.textContent = `Page 1 of ${formatNumber(Math.ceil(leo / limit) || 1)}`;
      if (btnFirst) btnFirst.disabled = true;
      if (btnPrev) btnPrev.disabled = true;
      if (btnNext) btnNext.disabled = true;
      if (btnLast) btnLast.disabled = true;
      return;
    }

    const firstOffset = msgs[0].offset;
    const lastOffset = msgs[msgs.length - 1].offset;
    const totalPages = Math.ceil(leo / limit) || 1;
    const currentPage = Math.floor(offset / limit) + 1;

    infoText.textContent = `Showing records offset ${formatNumber(firstOffset)} - ${formatNumber(lastOffset)} of ${formatNumber(leo)}`;
    if (pageDisplay) pageDisplay.textContent = `Page ${formatNumber(currentPage)} of ${formatNumber(totalPages)}`;

    if (btnFirst) btnFirst.disabled = (offset === 0);
    if (btnPrev) btnPrev.disabled = (offset === 0);
    if (btnNext) btnNext.disabled = (lastOffset >= leo - 1 || msgs.length < limit);
    if (btnLast) btnLast.disabled = (lastOffset >= leo - 1 || msgs.length < limit);
  }

  // FETCH CONSUMER GROUPS
  async function fetchGroups() {
    try {
      const res = await fetch('/api/v1/groups');
      if (!res.ok) return;
      const groups = await res.json();
      renderGroups(groups);
    } catch (err) {
      console.error('Error fetching consumer groups:', err);
    }
  }

  function renderGroups(groups) {
    if (!groups || groups.length === 0) {
      groupsContainer.innerHTML = `<div style="padding:24px; text-align:center; color:var(--text-muted);">No active Consumer Groups. Start a Kafka consumer client to monitor consumer group rebalances and lag.</div>`;
      return;
    }

    groupsContainer.innerHTML = groups.map(g => `
      <div style="border-bottom:1px solid var(--border-color); padding:16px 20px;">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
          <div>
            <strong class="mono" style="font-size:15px; color:var(--accent);">${escapeHtml(g.group_id)}</strong>
            <span class="badge ${g.state === 'Stable' ? 'badge-green' : 'badge-amber'}" style="margin-left:8px;">${g.state}</span>
          </div>
          <div class="mono" style="font-size:12px; color:var(--text-muted);">Generation: ${g.generation_id} | Members: ${g.members_count}</div>
        </div>

        ${g.lag_info && g.lag_info.length > 0 ? `
          <table>
            <thead>
              <tr>
                <th>Topic</th>
                <th>Partition</th>
                <th>Committed Offset</th>
                <th>Log End Offset</th>
                <th>Consumer Lag</th>
                <th>Last Commit</th>
              </tr>
            </thead>
            <tbody>
              ${g.lag_info.map(l => `
                <tr>
                  <td class="mono">${escapeHtml(l.topic)}</td>
                  <td class="mono">${l.partition}</td>
                  <td class="mono">${l.committed_offset}</td>
                  <td class="mono">${l.log_end_offset}</td>
                  <td>
                    <span class="badge ${l.lag === 0 ? 'badge-green' : 'badge-amber'} mono">
                      ${l.lag === 0 ? '0 (Up to date)' : 'lag: ' + l.lag}
                    </span>
                  </td>
                  <td class="mono" style="color:var(--text-muted);">${l.commit_time}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        ` : `<div style="font-size:13px; color:var(--text-muted);">No committed offsets recorded for this group.</div>`}
      </div>
    `).join('');
  }

  // UTILITIES
  function logConsole(source, msg, type = 'info') {
    const timeStr = new Date().toLocaleTimeString();
    const line = document.createElement('div');
    line.className = `console-line ${type}`;
    line.textContent = `[${timeStr}] [${source}] ${msg}`;
    consoleTerminal.appendChild(line);
    consoleTerminal.scrollTop = consoleTerminal.scrollHeight;
  }

  function formatBytes(bytes) {
    if (!bytes || bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
  }

  function formatNumber(num) {
    if (num === undefined || num === null) return '0';
    return num.toLocaleString();
  }

  function formatDuration(sec) {
    if (!sec) return '0s';
    const d = Math.floor(sec / 86400);
    const h = Math.floor((sec % 86400) / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = sec % 60;
    if (d > 0) return `${d}d ${h}h ${m}m`;
    if (h > 0) return `${h}h ${m}m ${s}s`;
    if (m > 0) return `${m}m ${s}s`;
    return `${s}s`;
  }

  function formatTimestamp(ts) {
    if (!ts) return '-';
    if (ts > 1e16) ts = Math.floor(ts / 1e6);
    return new Date(ts).toLocaleString();
  }

  function formatPayload(val) {
    if (!val) return '<span style="color:var(--text-muted);">&lt;empty&gt;</span>';
    try {
      const obj = JSON.parse(val);
      return `<code style="color:var(--accent);">${escapeHtml(JSON.stringify(obj))}</code>`;
    } catch (e) {
      return escapeHtml(val);
    }
  }

  function escapeHtml(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  // START APP
  document.addEventListener('DOMContentLoaded', init);
})();
