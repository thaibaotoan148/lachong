// Đường dẫn Firebase Realtime Database chung (lưu ở nhánh riêng /vovinam_lachong.json):
const DEFAULT_CLOUD_DB_URL = 'https://thai-bao-toan-default-rtdb.asia-southeast1.firebasedatabase.app';

const STORAGE_KEYS = {
  MEMBERS: 'vovinam_lachong_members_v2',
  ATTENDANCE: 'vovinam_lachong_attendance_v2',
  CLOUD_URL: 'vovinam_lachong_cloud_url_v2',
};

const LEGACY_SAMPLE_IDS = new Set(['lh1', 'lh2', 'lh3', 'lh4', 'lh5', 'lh6', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6', 'm7', 'm8']);

const CLUB_SHIFT = {
  id: 'ca_246',
  name: 'Thứ 2 - Thứ 4 - Thứ 6 (17h30 – 19h00)',
  shortName: 'Thứ 2-4-6 (17h30 – 19h00)',
  timeRange: '17h30 – 19h00',
};

const WEEKDAY_NAMES = [
  'Chủ Nhật',
  'Thứ Hai',
  'Thứ Ba',
  'Thứ Tư',
  'Thứ Năm',
  'Thứ Sáu',
  'Thứ Bảy',
];

const BELT_OPTIONS = [
  'Tự vệ nhập môn',
  'Lam đai',
  'Lam đai I',
  'Lam đai II',
  'Lam đai III',
  'Hoàng đai',
];

function getBeltRank(belt) {
  const idx = BELT_OPTIONS.indexOf(belt);
  return idx === -1 ? 0 : idx;
}

function sortMembersByBelt(list) {
  return list.sort((a, b) => {
    const rankDiff = getBeltRank(a.belt) - getBeltRank(b.belt);
    if (rankDiff !== 0) return rankDiff;
    return 0;
  });
}

function cleanSampleMembers(list) {
  if (!Array.isArray(list)) return [];
  return list.filter((m) => m && !LEGACY_SAMPLE_IDS.has(m.id));
}

let state = {
  selectedDate: getTodayISO(),
  hasClassToday: true,
  currentView: 'attendance', // 'attendance' | 'members' | 'add' | 'stats'
  searchQuery: '',
  statusFilter: 'all', // 'all' | 'present' | 'excused' | 'absent'
  expandedEditIds: new Set(),
  members: [],
  attendance: {},
};

function getTodayISO() {
  const now = new Date();
  const offset = now.getTimezoneOffset() * 60000;
  return new Date(now.getTime() - offset).toISOString().split('T')[0];
}

function parseISODate(isoStr) {
  const [y, m, d] = (isoStr || getTodayISO()).split('-').map(Number);
  return new Date(y, (m || 1) - 1, d || 1);
}

function formatDateVN(isoStr) {
  const [y, m, d] = (isoStr || '').split('-');
  if (!y || !m || !d) return isoStr;
  return `${d}/${m}/${y}`;
}

function addDaysISO(isoStr, days) {
  const dt = parseISODate(isoStr);
  dt.setDate(dt.getDate() + days);
  const offset = dt.getTimezoneOffset() * 60000;
  return new Date(dt.getTime() - offset).toISOString().split('T')[0];
}

/**
 * CLB Vovinam Lạc Hồng chỉ tập vào Thứ 2 (1), Thứ 4 (3), Thứ 6 (5) từ 17h30 - 19h00.
 * Các ngày Thứ 3 (2), Thứ 5 (4), Thứ 7 (6), Chủ Nhật (0) đều nghỉ.
 */
function getScheduleForDate(isoStr) {
  const dt = parseISODate(isoStr);
  const dayIndex = dt.getDay(); // 0..6
  const weekdayName = WEEKDAY_NAMES[dayIndex];
  const hasClass = dayIndex === 1 || dayIndex === 3 || dayIndex === 5;

  return {
    dayIndex,
    weekdayName,
    hasClass,
    shiftInfo: hasClass ? CLUB_SHIFT : null,
  };
}

function findNearestClassDate(isoStr, stepDirection) {
  let cursor = isoStr;
  for (let i = 0; i < 7; i++) {
    cursor = addDaysISO(cursor, stepDirection);
    const sched = getScheduleForDate(cursor);
    if (sched.hasClass) {
      return { isoDate: cursor, sched };
    }
  }
  return { isoDate: isoStr, sched: getScheduleForDate(isoStr) };
}

function syncScheduleFromDate() {
  const sched = getScheduleForDate(state.selectedDate);
  state.hasClassToday = sched.hasClass;
}

function loadState() {
  try {
    // Clean up old v1 sample storage if present
    localStorage.removeItem('vovinam_lachong_members_v1');

    const savedMembers = localStorage.getItem(STORAGE_KEYS.MEMBERS);
    const savedAttendance = localStorage.getItem(STORAGE_KEYS.ATTENDANCE);

    if (savedMembers) {
      const parsed = cleanSampleMembers(JSON.parse(savedMembers));
      state.members = sortMembersByBelt(parsed);
    } else {
      state.members = [];
    }

    if (savedAttendance) {
      state.attendance = JSON.parse(savedAttendance);
    }
  } catch (err) {
    console.error('Lỗi đọc dữ liệu localStorage:', err);
    state.members = [];
  }
  syncScheduleFromDate();
}

function saveMembers(skipCloud = false) {
  sortMembersByBelt(state.members);
  localStorage.setItem(STORAGE_KEYS.MEMBERS, JSON.stringify(state.members));
  if (!skipCloud) scheduleCloudPush();
}

function saveAttendance(skipCloud = false) {
  localStorage.setItem(STORAGE_KEYS.ATTENDANCE, JSON.stringify(state.attendance));
  if (!skipCloud) scheduleCloudPush();
}

/* =====================================================================
   REAL-TIME CLOUD SYNC (Firebase Realtime Database REST + SSE Stream)
   ===================================================================== */
let cloudState = {
  dbUrl: '',
  eventSource: null,
  pollTimer: null,
  pushTimer: null,
  lastUpdatedByMe: 0,
  lastSeenUpdatedAt: 0,
};

function normalizeFirebaseUrl(rawUrl) {
  const trimmed = (rawUrl || '').trim().replace(/\/+$/, '');
  if (!trimmed) return '';
  return trimmed.replace(/\/vovinam_lachong\.json$/i, '');
}

function updateCloudStatusUI(connected, labelText) {
  const btn = document.getElementById('cloudSyncBtn');
  const txt = document.getElementById('cloudStatusText');
  if (!btn || !txt) return;
  btn.classList.toggle('connected', Boolean(connected));
  txt.textContent = labelText || (connected ? 'Đã đồng bộ Cloud' : 'Đồng bộ Cloud');
}

function scheduleCloudPush() {
  if (!cloudState.dbUrl) return;
  clearTimeout(cloudState.pushTimer);
  cloudState.pushTimer = setTimeout(() => {
    pushStateToCloud(false);
  }, 350);
}

async function pushStateToCloud(showSuccessToast = false) {
  if (!cloudState.dbUrl) return;
  const endpoint = `${cloudState.dbUrl}/vovinam_lachong.json`;
  const payload = {
    updatedAt: Date.now(),
    members: state.members,
    attendance: state.attendance,
  };
  cloudState.lastUpdatedByMe = payload.updatedAt;
  cloudState.lastSeenUpdatedAt = payload.updatedAt;

  try {
    const res = await fetch(endpoint, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    updateCloudStatusUI(true, 'Đã đồng bộ Cloud');
    if (showSuccessToast) {
      showToast('Đã đồng bộ dữ liệu CLB Lạc Hồng lên Cloud!');
    }
  } catch (err) {
    console.error('Lỗi đẩy dữ liệu Cloud:', err);
    updateCloudStatusUI(false, 'Lỗi kết nối Cloud');
    if (showSuccessToast) {
      showToast('Không thể kết nối Cloud. Hãy kiểm tra lại link Firebase!');
    }
  }
}

function applyRemoteCloudData(data, forceRender = false) {
  if (!data || typeof data !== 'object') return;
  if (data.updatedAt && data.updatedAt === cloudState.lastUpdatedByMe) return;
  if (!forceRender && data.updatedAt && data.updatedAt <= cloudState.lastSeenUpdatedAt) return;

  if (data.updatedAt) {
    cloudState.lastSeenUpdatedAt = data.updatedAt;
  }

  const rawMembers = Array.isArray(data.members) ? data.members : [];
  const cleaned = cleanSampleMembers(rawMembers);
  state.members = sortMembersByBelt(cleaned);
  localStorage.setItem(STORAGE_KEYS.MEMBERS, JSON.stringify(state.members));

  // If remote still had legacy sample members, overwrite remote once with cleaned list
  if (cleaned.length !== rawMembers.length) {
    pushStateToCloud(false);
  }

  state.attendance = data.attendance && typeof data.attendance === 'object' ? data.attendance : {};
  localStorage.setItem(STORAGE_KEYS.ATTENDANCE, JSON.stringify(state.attendance));

  const activeEl = document.activeElement;
  const isTyping =
    activeEl &&
    (activeEl.matches('[data-action="edit-field"]') || activeEl.matches('[data-action="note"]'));

  if (!isTyping || forceRender) {
    renderAll();
  } else {
    renderDayBanner();
    renderStats();
  }
}

async function connectCloudSync(rawUrl, showFeedback = false) {
  const baseUrl = normalizeFirebaseUrl(rawUrl);
  if (cloudState.eventSource) {
    cloudState.eventSource.close();
    cloudState.eventSource = null;
  }
  if (cloudState.pollTimer) {
    clearInterval(cloudState.pollTimer);
    cloudState.pollTimer = null;
  }

  if (!baseUrl) {
    cloudState.dbUrl = '';
    localStorage.removeItem(STORAGE_KEYS.CLOUD_URL);
    updateCloudStatusUI(false, 'Đồng bộ Cloud');
    return;
  }

  cloudState.dbUrl = baseUrl;
  localStorage.setItem(STORAGE_KEYS.CLOUD_URL, baseUrl);
  const endpoint = `${baseUrl}/vovinam_lachong.json`;

  try {
    const res = await fetch(endpoint);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const remoteData = await res.json();

    if (remoteData && typeof remoteData === 'object') {
      applyRemoteCloudData(remoteData, true);
      if (showFeedback) showToast('Đã kết nối và tải dữ liệu mới nhất từ Cloud!');
    } else {
      await pushStateToCloud(false);
      if (showFeedback) showToast('Đã kết nối Cloud cho CLB Vovinam Lạc Hồng!');
    }

    updateCloudStatusUI(true, 'Đã đồng bộ Cloud');

    if (window.EventSource) {
      const es = new EventSource(endpoint);
      es.addEventListener('put', (e) => {
        try {
          const packet = JSON.parse(e.data);
          if (packet.path === '/' && packet.data) {
            applyRemoteCloudData(packet.data);
            updateCloudStatusUI(true, 'Đã đồng bộ Cloud');
          } else {
            fetch(endpoint)
              .then((r) => r.json())
              .then((d) => applyRemoteCloudData(d))
              .catch(() => {});
          }
        } catch (err) {}
      });
      cloudState.eventSource = es;
    }

    cloudState.pollTimer = setInterval(async () => {
      if (document.hidden || !cloudState.dbUrl) return;
      try {
        const r = await fetch(endpoint);
        if (r.ok) {
          const d = await r.json();
          if (d) applyRemoteCloudData(d);
          updateCloudStatusUI(true, 'Đã đồng bộ Cloud');
        }
      } catch (e) {}
    }, 4000);
  } catch (err) {
    console.error('Lỗi kết nối Cloud:', err);
    updateCloudStatusUI(false, 'Lỗi kết nối Cloud');
    if (showFeedback) {
      showToast('Không thể kết nối Firebase URL. Vui lòng kiểm tra lại link & Rules!');
    }
  }
}

function getAttendanceKey() {
  return `${state.selectedDate}_ca_246`;
}

function getFilteredMembers() {
  const q = state.searchQuery.trim().toLowerCase();
  const key = getAttendanceKey();
  const sessionRecord = state.attendance[key] || {};

  return state.members.filter((m) => {
    const matchesSearch =
      !q ||
      m.name.toLowerCase().includes(q) ||
      (m.studentId && m.studentId.toLowerCase().includes(q)) ||
      (m.phone && m.phone.toLowerCase().includes(q));

    if (!matchesSearch) return false;

    if (state.statusFilter !== 'all' && state.currentView === 'attendance') {
      const st = sessionRecord[m.id]?.status || '';
      return st === state.statusFilter;
    }

    return true;
  });
}

function setAttendanceStatus(memberId, status) {
  if (!state.hasClassToday) return;
  const key = getAttendanceKey();
  if (!state.attendance[key]) {
    state.attendance[key] = {};
  }
  const current = state.attendance[key][memberId] || { status: '', note: '' };
  state.attendance[key][memberId] = {
    ...current,
    status: current.status === status ? '' : status,
  };
  saveAttendance();
  renderAll();
}

function setAttendanceNote(memberId, note) {
  if (!state.hasClassToday) return;
  const key = getAttendanceKey();
  if (!state.attendance[key]) {
    state.attendance[key] = {};
  }
  const current = state.attendance[key][memberId] || { status: '', note: '' };
  state.attendance[key][memberId] = {
    ...current,
    note,
  };
  saveAttendance();
}

function updateMemberField(memberId, field, value) {
  const member = state.members.find((m) => m.id === memberId);
  if (!member) return;
  member[field] = value;
  saveMembers();

  const card = document.querySelector(`.student-card[data-id="${CSS.escape(memberId)}"]`);
  if (card) {
    const nameEl = card.querySelector('.student-name-text');
    if (nameEl && field === 'name') {
      nameEl.textContent = value || 'Chưa đặt tên';
    }
    const metaContainer = card.querySelector('.student-meta-grid');
    if (metaContainer) {
      metaContainer.innerHTML = buildMetaChipsHtml(member);
    }
  }

  renderDayBanner();
  renderStats();
}

function escapeHtml(str = '') {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function getInitials(fullName = '') {
  const parts = fullName.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return 'VS';
  const last = parts[parts.length - 1];
  return last.charAt(0).toUpperCase();
}

function buildBeltBadgeHtml(belt = 'Tự vệ nhập môn') {
  let modifierClass = '';
  let stripesCount = 0;

  if (belt === 'Tự vệ nhập môn') {
    modifierClass = 'belt-nhapmon';
  } else if (belt === 'Lam đai I') {
    stripesCount = 1;
  } else if (belt === 'Lam đai II') {
    stripesCount = 2;
  } else if (belt === 'Lam đai III') {
    stripesCount = 3;
  } else if (belt.includes('Hoàng đai')) {
    modifierClass = 'belt-hoangdai';
  }

  const stripesHtml = Array.from({ length: stripesCount })
    .map(() => '<span class="belt-stripe"></span>')
    .join('');

  return `
    <span class="vovinam-belt-badge ${modifierClass}">
      <span class="belt-visual-bar" aria-hidden="true">${stripesHtml}</span>
      <span>${escapeHtml(belt)}</span>
    </span>
  `;
}

function buildMetaChipsHtml(m) {
  return `
    ${buildBeltBadgeHtml(m.belt || 'Tự vệ nhập môn')}
    <span class="meta-item meta-id">Mã ĐD: <strong>${escapeHtml(m.studentId || 'Chưa cập nhật')}</strong></span>
    ${m.phone ? `<span class="meta-item">SĐT: <strong>${escapeHtml(m.phone)}</strong></span>` : ''}
  `;
}

function renderDayBanner() {
  const sched = getScheduleForDate(state.selectedDate);
  const dateVN = formatDateVN(state.selectedDate);

  const banner = document.getElementById('dayScheduleBanner');
  const bannerTag = document.getElementById('bannerWeekdayText');
  const timePill = document.getElementById('bannerTimePill');
  const bannerTitle = document.getElementById('bannerShiftTitle');
  const bannerSub = document.getElementById('bannerShiftSub');
  const progressWrap = document.getElementById('heroProgressWrap');

  banner.classList.toggle('offday-banner', !sched.hasClass);
  bannerTag.textContent = `${sched.weekdayName} • ${dateVN}`;

  if (!sched.hasClass) {
    timePill.classList.add('hidden');
    progressWrap.classList.add('hidden');
    bannerTitle.textContent = 'Hôm nay không có buổi học';
    bannerSub.textContent = 'CLB Vovinam Lạc Hồng chỉ tập vào Thứ 2 - Thứ 4 - Thứ 6 (17h30 – 19h00)';
  } else {
    timePill.classList.remove('hidden');
    progressWrap.classList.remove('hidden');
    bannerTitle.textContent = `Ca tập: ${CLUB_SHIFT.name}`;
    bannerSub.textContent = `Tổng số ${state.members.length} võ sinh trong danh sách CLB Vovinam Lạc Hồng`;
  }
}

function renderViewNavigation() {
  const sched = getScheduleForDate(state.selectedDate);

  document.querySelectorAll('.section-tab').forEach((tab) => {
    tab.classList.toggle('active', tab.dataset.view === state.currentView);
  });

  const noClassPanel = document.getElementById('noClassPanel');
  const listContainer = document.getElementById('view-list-container');
  const addContainer = document.getElementById('view-add-container');
  const statsContainer = document.getElementById('view-stats-container');
  const quickStats = document.getElementById('quickStatsSection');

  const isOffDayAttendanceOrStats =
    !sched.hasClass && (state.currentView === 'attendance' || state.currentView === 'stats');

  noClassPanel.classList.toggle('hidden', !isOffDayAttendanceOrStats);
  if (isOffDayAttendanceOrStats) {
    const noClassTitle = document.getElementById('noClassTitle');
    if (noClassTitle) {
      noClassTitle.textContent = `${sched.weekdayName} (${formatDateVN(state.selectedDate)}) — Hôm nay không có buổi học`;
    }
    const prevClass = findNearestClassDate(state.selectedDate, -1);
    const nextClass = findNearestClassDate(state.selectedDate, 1);
    const prevBtn = document.getElementById('goPrevClassDayBtn');
    const nextBtn = document.getElementById('goNextClassDayBtn');
    if (prevBtn) {
      prevBtn.textContent = `‹ Xem ${prevClass.sched.weekdayName} (${formatDateVN(prevClass.isoDate)})`;
    }
    if (nextBtn) {
      nextBtn.textContent = `Chuyển tới ${nextClass.sched.weekdayName} (${formatDateVN(nextClass.isoDate)}) ›`;
    }
  }

  const isListView =
    (state.currentView === 'attendance' && sched.hasClass) || state.currentView === 'members';
  listContainer.classList.toggle('hidden-view', !isListView);
  addContainer.classList.toggle('hidden-view', state.currentView !== 'add');
  statsContainer.classList.toggle('hidden-view', !(state.currentView === 'stats' && sched.hasClass));
  quickStats.classList.toggle('hidden-view', state.currentView === 'add' || !sched.hasClass);

  if (isListView) {
    const titleEl = document.getElementById('listViewTitle');
    const subEl = document.getElementById('listViewSubtitle');
    const quickActions = document.getElementById('attendanceQuickActions');
    const filterPills = document.getElementById('statusFilterPills');

    if (state.currentView === 'members') {
      titleEl.textContent = 'Hồ Sơ & Chỉnh Sửa Thông Tin Võ Sinh';
      subEl.textContent = 'Chỉnh sửa trực tiếp Họ tên, Mã định danh, Cấp đai, SĐT cho từng em (Tự động lưu & xếp theo cấp đai)';
      quickActions.classList.add('hidden');
      filterPills.classList.add('hidden');
    } else {
      titleEl.textContent = `Điểm Danh ${sched.weekdayName} (${formatDateVN(state.selectedDate)})`;
      subEl.textContent = 'Tự động xếp theo cấp đai từ nhỏ đến lớn • Nhấn "Sửa TT" trên thẻ nếu phát hiện nhập sai';
      quickActions.classList.remove('hidden');
      filterPills.classList.remove('hidden');
    }
  }
}

function renderStats() {
  const total = state.members.length;
  const key = getAttendanceKey();
  const sessionRecord = state.attendance[key] || {};

  let present = 0;
  let excused = 0;
  let absent = 0;

  if (state.hasClassToday) {
    state.members.forEach((m) => {
      const st = sessionRecord[m.id]?.status;
      if (st === 'present') present++;
      else if (st === 'excused') excused++;
      else if (st === 'absent') absent++;
    });
  }

  document.getElementById('statTotal').textContent = state.hasClassToday ? total : 0;
  document.getElementById('statPresent').textContent = present;
  document.getElementById('statExcused').textContent = excused;
  document.getElementById('statAbsent').textContent = absent;

  // Update Hero Progress Bar
  const markedCount = present + excused + absent;
  const pct = total > 0 ? Math.round((markedCount / total) * 100) : 0;
  const statusTextEl = document.getElementById('progressStatusText');
  const pctTextEl = document.getElementById('progressPercentText');
  const fillEl = document.getElementById('heroProgressFill');

  if (statusTextEl) {
    statusTextEl.innerHTML = `Đã điểm danh: <strong>${markedCount} / ${total}</strong> võ sinh`;
  }
  if (pctTextEl) {
    pctTextEl.textContent = `${pct}%`;
  }
  if (fillEl) {
    fillEl.style.width = `${pct}%`;
  }
}

function renderMemberList() {
  const container = document.getElementById('memberListContainer');
  const emptyState = document.getElementById('emptyState');
  const emptyTitle = document.getElementById('emptyStateTitle');
  const emptyDesc = document.getElementById('emptyStateDesc');
  const members = getFilteredMembers();
  const key = getAttendanceKey();
  const sessionRecord = state.attendance[key] || {};
  const isEditModeView = state.currentView === 'members';

  if (members.length === 0) {
    container.innerHTML = '';
    emptyState.classList.remove('hidden');
    if (state.members.length === 0) {
      emptyTitle.textContent = 'Chưa có võ sinh nào trong danh sách CLB';
      emptyDesc.textContent = 'Hãy bấm nút bên dưới để bắt đầu nhập võ sinh của CLB Vovinam Lạc Hồng.';
    } else {
      emptyTitle.textContent = 'Không tìm thấy võ sinh phù hợp';
      emptyDesc.textContent = 'Hãy thử từ khóa khác hoặc chuyển bộ lọc trạng thái về "Tất cả".';
    }
    return;
  }

  emptyState.classList.add('hidden');

  container.innerHTML = members
    .map((m, idx) => {
      const record = sessionRecord[m.id] || { status: '', note: '' };
      const isExpanded = isEditModeView || state.expandedEditIds.has(m.id);
      const statusClass = !isEditModeView && record.status ? `status-${record.status}` : '';

      const beltOptionsHtml = BELT_OPTIONS.map(
        (b) => `<option value="${escapeHtml(b)}" ${m.belt === b ? 'selected' : ''}>${escapeHtml(b)}</option>`
      ).join('');

      return `
        <article class="student-card ${statusClass}" data-id="${escapeHtml(m.id)}">
          <div class="student-card-main">
            <div class="student-identity">
              <div class="student-avatar" aria-hidden="true">
                <span class="student-avatar-stt">#${idx + 1}</span>
                <span class="student-avatar-char">${escapeHtml(getInitials(m.name))}</span>
              </div>
              <div class="student-info-block">
                <div class="student-name-row">
                  <span class="student-name-text">${escapeHtml(m.name)}</span>
                  ${
                    !isEditModeView
                      ? `<button type="button" class="btn-edit-toggle ${isExpanded ? 'active' : ''}" data-action="toggle-edit" data-id="${escapeHtml(m.id)}">
                          ✏️ ${isExpanded ? 'Thu gọn' : 'Sửa TT'}
                        </button>`
                      : ''
                  }
                </div>
                <div class="student-meta-grid">
                  ${buildMetaChipsHtml(m)}
                </div>
              </div>
            </div>

            ${
              !isEditModeView
                ? `<div class="student-attendance-controls">
                    <div class="status-selector" role="group" aria-label="Điểm danh ${escapeHtml(m.name)}">
                      <button type="button" class="status-btn ${record.status === 'present' ? 'active-present' : ''}" data-action="status" data-status="present" data-id="${escapeHtml(m.id)}">
                        ✓ Có mặt
                      </button>
                      <button type="button" class="status-btn ${record.status === 'excused' ? 'active-excused' : ''}" data-action="status" data-status="excused" data-id="${escapeHtml(m.id)}">
                        Có phép
                      </button>
                      <button type="button" class="status-btn ${record.status === 'absent' ? 'active-absent' : ''}" data-action="status" data-status="absent" data-id="${escapeHtml(m.id)}">
                        ✕ Vắng
                      </button>
                    </div>
                    <input
                      type="text"
                      class="note-input"
                      placeholder="Ghi chú buổi tập..."
                      value="${escapeHtml(record.note || '')}"
                      data-action="note"
                      data-id="${escapeHtml(m.id)}"
                    />
                  </div>`
                : ''
            }
          </div>

          ${
            isExpanded
              ? `<div class="direct-edit-grid">
                  <div class="edit-field">
                    <label>Họ và tên võ sinh</label>
                    <input type="text" value="${escapeHtml(m.name)}" data-action="edit-field" data-field="name" data-id="${escapeHtml(m.id)}" placeholder="Nhập họ tên..." />
                  </div>
                  <div class="edit-field">
                    <label>Mã định danh</label>
                    <input type="text" value="${escapeHtml(m.studentId || '')}" data-action="edit-field" data-field="studentId" data-id="${escapeHtml(m.id)}" placeholder="Nhập mã định danh..." />
                  </div>
                  <div class="edit-field">
                    <label>Cấp đai Vovinam</label>
                    <select data-action="edit-field" data-field="belt" data-id="${escapeHtml(m.id)}">
                      ${beltOptionsHtml}
                    </select>
                  </div>
                  <div class="edit-field">
                    <label>SĐT Phụ huynh / Liên hệ</label>
                    <input type="tel" value="${escapeHtml(m.phone || '')}" data-action="edit-field" data-field="phone" data-id="${escapeHtml(m.id)}" placeholder="Nhập SĐT..." />
                  </div>
                  <div class="edit-actions-row">
                    <span class="auto-save-badge">✓ Mọi chỉnh sửa được lưu tự động</span>
                    <button type="button" class="delete-btn" data-action="delete" data-id="${escapeHtml(m.id)}">
                      <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <polyline points="3 6 5 6 21 6"/>
                        <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
                      </svg>
                      Xóa võ sinh
                    </button>
                  </div>
                </div>`
              : ''
          }
        </article>
      `;
    })
    .join('');
}

function renderDetailedStatsView() {
  const grid = document.getElementById('reportSummaryGrid');
  const label = document.getElementById('statsSessionLabel');
  if (!grid || !state.hasClassToday) return;

  const sched = getScheduleForDate(state.selectedDate);
  label.textContent = `${sched.weekdayName} (${formatDateVN(state.selectedDate)}) • Ca tập: ${CLUB_SHIFT.name}`;

  const key = getAttendanceKey();
  const sessionRecord = state.attendance[key] || {};

  const groups = {
    present: { title: '✓ Có mặt', color: 'var(--present-text)', items: [] },
    excused: { title: '⏳ Vắng có phép', color: 'var(--excused-text)', items: [] },
    absent: { title: '✕ Vắng không phép', color: 'var(--absent-text)', items: [] },
    unmarked: { title: '• Chưa điểm danh', color: 'var(--text-secondary)', items: [] },
  };

  state.members.forEach((m) => {
    const st = sessionRecord[m.id]?.status || 'unmarked';
    const note = sessionRecord[m.id]?.note || '';
    const target = groups[st] || groups.unmarked;
    target.items.push({ member: m, note });
  });

  grid.innerHTML = Object.values(groups)
    .map(
      (g) => `
      <div class="report-group-card">
        <div class="report-group-title" style="color: ${g.color}">
          <span>${g.title}</span>
          <span>${g.items.length} em</span>
        </div>
        ${
          g.items.length === 0
            ? `<p style="font-size: 0.8rem; color: var(--text-muted);">Không có võ sinh nào</p>`
            : `<ul class="report-student-list">
                ${g.items
                  .map(
                    ({ member, note }) => `
                    <li>
                      <span><strong>${escapeHtml(member.name)}</strong></span>
                      <span style="font-size: 0.76rem; color: var(--text-muted);">${escapeHtml(note || member.belt || '')}</span>
                    </li>`
                  )
                  .join('')}
              </ul>`
        }
      </div>
    `
    )
    .join('');
}

function renderAll() {
  syncScheduleFromDate();
  renderDayBanner();
  renderViewNavigation();
  renderStats();
  renderMemberList();
  renderDetailedStatsView();
}

function showToast(message) {
  const toast = document.getElementById('toast');
  toast.textContent = message;
  toast.classList.add('show');
  clearTimeout(showToast._timer);
  showToast._timer = setTimeout(() => {
    toast.classList.remove('show');
  }, 2600);
}

/* =====================================================================
   PURE-JS .XLSX (OpenXML Excel Workbook) GENERATOR - WORKS 100% OFFLINE
   ===================================================================== */
function makeCrc32Table() {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) {
      c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    }
    table[n] = c >>> 0;
  }
  return table;
}
const CRC_TABLE = makeCrc32Table();

function crc32(bytes) {
  let crc = 0xffffffff;
  for (let i = 0; i < bytes.length; i++) {
    crc = CRC_TABLE[(crc ^ bytes[i]) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function createZipArchive(files) {
  const encoder = new TextEncoder();
  const entries = files.map((f) => {
    const nameBytes = encoder.encode(f.name);
    const dataBytes = typeof f.content === 'string' ? encoder.encode(f.content) : f.content;
    return {
      nameBytes,
      dataBytes,
      crc: crc32(dataBytes),
      offset: 0,
    };
  });

  let localSize = 0;
  let centralSize = 0;
  entries.forEach((e) => {
    e.offset = localSize;
    localSize += 30 + e.nameBytes.length + e.dataBytes.length;
    centralSize += 46 + e.nameBytes.length;
  });

  const totalSize = localSize + centralSize + 22;
  const out = new Uint8Array(totalSize);
  const view = new DataView(out.buffer);
  let pos = 0;

  entries.forEach((e) => {
    view.setUint32(pos, 0x04034b50, true);
    view.setUint16(pos + 4, 20, true);
    view.setUint16(pos + 6, 0x0800, true);
    view.setUint16(pos + 8, 0, true);
    view.setUint16(pos + 10, 0, true);
    view.setUint16(pos + 12, 0, true);
    view.setUint32(pos + 14, e.crc, true);
    view.setUint32(pos + 18, e.dataBytes.length, true);
    view.setUint32(pos + 22, e.dataBytes.length, true);
    view.setUint16(pos + 26, e.nameBytes.length, true);
    view.setUint16(pos + 28, 0, true);
    pos += 30;
    out.set(e.nameBytes, pos);
    pos += e.nameBytes.length;
    out.set(e.dataBytes, pos);
    pos += e.dataBytes.length;
  });

  const centralOffset = pos;

  entries.forEach((e) => {
    view.setUint32(pos, 0x02014b50, true);
    view.setUint16(pos + 4, 20, true);
    view.setUint16(pos + 6, 20, true);
    view.setUint16(pos + 8, 0x0800, true);
    view.setUint16(pos + 10, 0, true);
    view.setUint16(pos + 12, 0, true);
    view.setUint16(pos + 14, 0, true);
    view.setUint32(pos + 16, e.crc, true);
    view.setUint32(pos + 20, e.dataBytes.length, true);
    view.setUint32(pos + 24, e.dataBytes.length, true);
    view.setUint16(pos + 28, e.nameBytes.length, true);
    view.setUint16(pos + 30, 0, true);
    view.setUint16(pos + 32, 0, true);
    view.setUint16(pos + 34, 0, true);
    view.setUint16(pos + 36, 0, true);
    view.setUint32(pos + 38, 0, true);
    view.setUint32(pos + 42, e.offset, true);
    pos += 46;
    out.set(e.nameBytes, pos);
    pos += e.nameBytes.length;
  });

  view.setUint32(pos, 0x06054b50, true);
  view.setUint16(pos + 4, 0, true);
  view.setUint16(pos + 6, 0, true);
  view.setUint16(pos + 8, entries.length, true);
  view.setUint16(pos + 10, entries.length, true);
  view.setUint32(pos + 12, centralSize, true);
  view.setUint32(pos + 16, centralOffset, true);
  view.setUint16(pos + 20, 0, true);

  return new Blob([out], {
    type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  });
}

function xmlEscape(val = '') {
  return String(val)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function colLetter(colIdx) {
  let s = '';
  let n = colIdx + 1;
  while (n > 0) {
    const rem = (n - 1) % 26;
    s = String.fromCharCode(65 + rem) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

function buildXlsxBlob(rows) {
  const sheetRowsXml = rows
    .map((row, rIdx) => {
      const rowNum = rIdx + 1;
      const cellsXml = row
        .map((cellObj, cIdx) => {
          const ref = `${colLetter(cIdx)}${rowNum}`;
          const val = typeof cellObj === 'object' && cellObj !== null ? cellObj.v : cellObj;
          const styleId = typeof cellObj === 'object' && cellObj !== null ? cellObj.s || 0 : 0;
          const isNumber = typeof val === 'number';
          if (isNumber) {
            return `<c r="${ref}" s="${styleId}"><v>${val}</v></c>`;
          }
          return `<c r="${ref}" s="${styleId}" t="inlineStr"><is><t>${xmlEscape(val ?? '')}</t></is></c>`;
        })
        .join('');
      const ht = rIdx === 0 ? ' ht="26" customHeight="1"' : rIdx === 3 ? ' ht="22" customHeight="1"' : '';
      return `<row r="${rowNum}"${ht}>${cellsXml}</row>`;
    })
    .join('');

  const sheetXml = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <cols>
    <col min="1" max="1" width="8" customWidth="1"/>
    <col min="2" max="2" width="28" customWidth="1"/>
    <col min="3" max="3" width="20" customWidth="1"/>
    <col min="4" max="4" width="18" customWidth="1"/>
    <col min="5" max="5" width="18" customWidth="1"/>
    <col min="6" max="6" width="18" customWidth="1"/>
    <col min="7" max="7" width="28" customWidth="1"/>
  </cols>
  <sheetData>${sheetRowsXml}</sheetData>
</worksheet>`;

  const stylesXml = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="3">
    <font><sz val="11"/><name val="Arial"/></font>
    <font><b/><sz val="13"/><color rgb="FF0B2545"/><name val="Arial"/></font>
    <font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Arial"/></font>
  </fonts>
  <fills count="3">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF0B2545"/></patternFill></fill>
  </fills>
  <borders count="2">
    <border><left/><right/><top/><bottom/><diagonal/></border>
    <border>
      <left style="thin"><color rgb="FFCBD5E1"/></left>
      <right style="thin"><color rgb="FFCBD5E1"/></right>
      <top style="thin"><color rgb="FFCBD5E1"/></top>
      <bottom style="thin"><color rgb="FFCBD5E1"/></bottom>
      <diagonal/>
    </border>
  </borders>
  <cellStyleXfs count="1">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>
  </cellStyleXfs>
  <cellXfs count="4">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
    <xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0"/>
    <xf numFmtId="0" fontId="2" fillId="2" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1"/>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1"/>
  </cellXfs>
</styleSheet>`;

  const workbookXml = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Diem Danh Lac Hong" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>`;

  const workbookRelsXml = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>`;

  const rootRelsXml = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>`;

  const contentTypesXml = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>`;

  return createZipArchive([
    { name: '[Content_Types].xml', content: contentTypesXml },
    { name: '_rels/.rels', content: rootRelsXml },
    { name: 'xl/workbook.xml', content: workbookXml },
    { name: 'xl/_rels/workbook.xml.rels', content: workbookRelsXml },
    { name: 'xl/styles.xml', content: stylesXml },
    { name: 'xl/worksheets/sheet1.xml', content: sheetXml },
  ]);
}

function exportToXLSX() {
  const sched = getScheduleForDate(state.selectedDate);
  if (!sched.hasClass) {
    showToast(`${sched.weekdayName} không có buổi học để xuất điểm danh!`);
    return;
  }

  if (state.members.length === 0) {
    showToast('Chưa có dữ liệu võ sinh để xuất file!');
    return;
  }

  const key = getAttendanceKey();
  const sessionRecord = state.attendance[key] || {};

  const statusMap = {
    present: 'Có mặt',
    excused: 'Vắng có phép',
    absent: 'Vắng không phép',
    '': 'Chưa điểm danh',
  };

  const rows = [
    [{ v: 'CLB VOVINAM LẠC HỒNG', s: 1 }],
    [
      { v: `Ngày tập: ${sched.weekdayName}, ${formatDateVN(state.selectedDate)}`, s: 0 },
      { v: `Ca tập: ${CLUB_SHIFT.name}`, s: 0 },
    ],
    [],
    [
      { v: 'STT', s: 2 },
      { v: 'Họ và tên võ sinh', s: 2 },
      { v: 'Mã định danh', s: 2 },
      { v: 'Cấp đai', s: 2 },
      { v: 'SĐT Phụ huynh', s: 2 },
      { v: 'Trạng thái', s: 2 },
      { v: 'Ghi chú', s: 2 },
    ],
  ];

  state.members.forEach((m, index) => {
    const rec = sessionRecord[m.id] || { status: '', note: '' };
    rows.push([
      { v: index + 1, s: 3 },
      { v: m.name, s: 3 },
      { v: m.studentId || '', s: 3 },
      { v: m.belt || '', s: 3 },
      { v: m.phone || '', s: 3 },
      { v: statusMap[rec.status || ''], s: 3 },
      { v: rec.note || '', s: 3 },
    ]);
  });

  const blob = buildXlsxBlob(rows);
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = `DiemDanh_Vovinam_LacHong_${state.selectedDate}.xlsx`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);

  showToast('Đã tải xuống file Excel (.xlsx) thành công!');
}

function setDateAndRender(newIsoDate) {
  state.selectedDate = newIsoDate;
  const dateInput = document.getElementById('attendanceDate');
  if (dateInput) dateInput.value = newIsoDate;
  renderAll();
}

function initEvents() {
  const dateInput = document.getElementById('attendanceDate');
  dateInput.value = state.selectedDate;
  dateInput.addEventListener('change', (e) => {
    if (e.target.value) {
      setDateAndRender(e.target.value);
    }
  });

  document.getElementById('prevDayBtn').addEventListener('click', () => {
    setDateAndRender(addDaysISO(state.selectedDate, -1));
  });

  document.getElementById('nextDayBtn').addEventListener('click', () => {
    setDateAndRender(addDaysISO(state.selectedDate, 1));
  });

  document.getElementById('todayBtn').addEventListener('click', () => {
    setDateAndRender(getTodayISO());
  });

  // Off-day quick jump buttons
  document.getElementById('goPrevClassDayBtn').addEventListener('click', () => {
    const prev = findNearestClassDate(state.selectedDate, -1);
    setDateAndRender(prev.isoDate);
  });

  document.getElementById('goNextClassDayBtn').addEventListener('click', () => {
    const next = findNearestClassDate(state.selectedDate, 1);
    setDateAndRender(next.isoDate);
  });

  // Section Navigation Tabs
  document.querySelectorAll('.section-tab').forEach((tab) => {
    tab.addEventListener('click', () => {
      state.currentView = tab.dataset.view;
      renderAll();
    });
  });

  // Go to Add tab from empty state button
  const goToAddBtn = document.getElementById('goToAddTabBtn');
  if (goToAddBtn) {
    goToAddBtn.addEventListener('click', () => {
      state.currentView = 'add';
      renderAll();
      const nameInput = document.getElementById('memberName');
      if (nameInput) nameInput.focus();
    });
  }

  // Status Filter pills
  document.querySelectorAll('.filter-pill').forEach((pill) => {
    pill.addEventListener('click', () => {
      state.statusFilter = pill.dataset.filter;
      document.querySelectorAll('.filter-pill').forEach((p) =>
        p.classList.toggle('active', p.dataset.filter === state.statusFilter)
      );
      renderMemberList();
    });
  });

  // Add single member
  const addMemberForm = document.getElementById('addMemberForm');
  addMemberForm.addEventListener('submit', (e) => {
    e.preventDefault();
    const nameInput = document.getElementById('memberName');
    const studentIdInput = document.getElementById('memberStudentId');
    const beltSelect = document.getElementById('memberBelt');
    const phoneInput = document.getElementById('memberPhone');

    const name = nameInput.value.trim();
    if (!name) return;

    const newMember = {
      id: 'lh_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6),
      name,
      studentId: studentIdInput ? studentIdInput.value.trim() : '',
      belt: beltSelect.value,
      phone: phoneInput.value.trim(),
    };

    state.members.push(newMember);
    saveMembers();

    nameInput.value = '';
    if (studentIdInput) studentIdInput.value = '';
    phoneInput.value = '';
    nameInput.focus();

    renderAll();
    showToast(`Đã thêm võ sinh "${name}" (đã xếp theo cấp đai)!`);
  });

  // Bulk import handler
  document.getElementById('bulkImportBtn').addEventListener('click', () => {
    const textarea = document.getElementById('bulkTextarea');
    const lines = textarea.value
      .split('\n')
      .map((l) => l.trim())
      .filter(Boolean);

    if (lines.length === 0) {
      showToast('Vui lòng nhập ít nhất 1 dòng tên võ sinh!');
      return;
    }

    let addedCount = 0;
    lines.forEach((line) => {
      const parts = line.split('-').map((p) => p.trim());
      const name = parts[0];
      if (!name) return;
      state.members.push({
        id: 'lh_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7) + '_' + addedCount,
        name,
        studentId: parts[1] || '',
        belt: 'Tự vệ nhập môn',
        phone: parts[2] || '',
      });
      addedCount++;
    });

    saveMembers();
    textarea.value = '';
    renderAll();
    showToast(`Đã thêm nhanh ${addedCount} võ sinh vào CLB Vovinam Lạc Hồng!`);
  });

  // Search box
  document.getElementById('searchInput').addEventListener('input', (e) => {
    state.searchQuery = e.target.value;
    renderMemberList();
  });

  // Mark all present
  document.getElementById('markAllPresentBtn').addEventListener('click', () => {
    if (!state.hasClassToday || state.members.length === 0) return;
    const key = getAttendanceKey();
    if (!state.attendance[key]) state.attendance[key] = {};

    state.members.forEach((m) => {
      const prev = state.attendance[key][m.id] || { note: '' };
      state.attendance[key][m.id] = { ...prev, status: 'present' };
    });

    saveAttendance();
    renderAll();
    showToast('Đã đánh dấu Có mặt tất cả võ sinh!');
  });

  // Reset attendance for current session
  document.getElementById('resetAttendanceBtn').addEventListener('click', () => {
    if (!state.hasClassToday) return;
    const key = getAttendanceKey();
    if (!state.attendance[key]) return;
    if (confirm('Bạn có chắc muốn đặt lại trạng thái điểm danh của buổi này?')) {
      delete state.attendance[key];
      saveAttendance();
      renderAll();
      showToast('Đã đặt lại điểm danh buổi tập!');
    }
  });

  // Delegated events on member list
  const listContainer = document.getElementById('memberListContainer');

  listContainer.addEventListener('click', (e) => {
    const statusBtn = e.target.closest('[data-action="status"]');
    if (statusBtn) {
      setAttendanceStatus(statusBtn.dataset.id, statusBtn.dataset.status);
      return;
    }

    const toggleEditBtn = e.target.closest('[data-action="toggle-edit"]');
    if (toggleEditBtn) {
      const id = toggleEditBtn.dataset.id;
      if (state.expandedEditIds.has(id)) {
        state.expandedEditIds.delete(id);
      } else {
        state.expandedEditIds.add(id);
      }
      renderMemberList();
      return;
    }

    const deleteBtn = e.target.closest('[data-action="delete"]');
    if (deleteBtn) {
      const id = deleteBtn.dataset.id;
      const member = state.members.find((m) => m.id === id);
      if (member && confirm(`Xóa võ sinh "${member.name}" khỏi danh sách CLB?`)) {
        state.members = state.members.filter((m) => m.id !== id);
        state.expandedEditIds.delete(id);
        saveMembers();
        renderAll();
        showToast(`Đã xóa võ sinh "${member.name}".`);
      }
    }
  });

  listContainer.addEventListener('input', (e) => {
    const noteInput = e.target.closest('[data-action="note"]');
    if (noteInput) {
      setAttendanceNote(noteInput.dataset.id, noteInput.value);
      return;
    }

    const editField = e.target.closest('[data-action="edit-field"]');
    if (editField && editField.tagName === 'INPUT') {
      updateMemberField(editField.dataset.id, editField.dataset.field, editField.value);
    }
  });

  listContainer.addEventListener('change', (e) => {
    const editSelect = e.target.closest('[data-action="edit-field"]');
    if (editSelect && editSelect.tagName === 'SELECT') {
      updateMemberField(editSelect.dataset.id, editSelect.dataset.field, editSelect.value);
      if (editSelect.dataset.field === 'belt') {
        renderMemberList();
        showToast('Đã cập nhật cấp đai và sắp xếp lại danh sách!');
      }
    }
  });

  // Export XLSX buttons
  document.getElementById('exportXlsxBtn').addEventListener('click', exportToXLSX);
  const exportBtn2 = document.getElementById('exportXlsxBtn2');
  if (exportBtn2) {
    exportBtn2.addEventListener('click', exportToXLSX);
  }

  // Cloud Sync Modal events
  const cloudModal = document.getElementById('cloudModal');
  const cloudInput = document.getElementById('firebaseDbUrlInput');

  document.getElementById('cloudSyncBtn').addEventListener('click', () => {
    cloudInput.value = cloudState.dbUrl || DEFAULT_CLOUD_DB_URL || '';
    cloudModal.classList.remove('hidden');
  });

  document.getElementById('closeCloudModalBtn').addEventListener('click', () => {
    cloudModal.classList.add('hidden');
  });

  cloudModal.addEventListener('click', (e) => {
    if (e.target === cloudModal) cloudModal.classList.add('hidden');
  });

  document.getElementById('saveCloudConfigBtn').addEventListener('click', async () => {
    const url = cloudInput.value.trim();
    if (!url) {
      showToast('Vui lòng dán đường dẫn Firebase Realtime Database URL!');
      return;
    }
    await connectCloudSync(url, true);
    cloudModal.classList.add('hidden');
  });

  document.getElementById('pushLocalToCloudBtn').addEventListener('click', async () => {
    const url = normalizeFirebaseUrl(cloudInput.value);
    if (!url) {
      showToast('Vui lòng nhập đường dẫn Firebase URL trước!');
      return;
    }
    cloudState.dbUrl = url;
    localStorage.setItem(STORAGE_KEYS.CLOUD_URL, url);
    await pushStateToCloud(true);
    await connectCloudSync(url, false);
    cloudModal.classList.add('hidden');
  });

  document.getElementById('disconnectCloudBtn').addEventListener('click', () => {
    connectCloudSync('', false);
    cloudInput.value = '';
    cloudModal.classList.add('hidden');
    showToast('Đã ngắt kết nối Cloud (chỉ lưu trên thiết bị này).');
  });
}

document.addEventListener('DOMContentLoaded', () => {
  loadState();
  initEvents();
  renderAll();

  const urlParams = new URLSearchParams(window.location.search);
  const queryCloudUrl = urlParams.get('cloud');
  const initialCloudUrl =
    queryCloudUrl || DEFAULT_CLOUD_DB_URL || localStorage.getItem(STORAGE_KEYS.CLOUD_URL) || '';
  if (initialCloudUrl) {
    connectCloudSync(initialCloudUrl, false);
  }
});
