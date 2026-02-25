import  { FastifyInstance }  from "fastify";

export async function dashboard(fastify: FastifyInstance) {
fastify.get("/dashboard", async (request, reply) => {
  const query = request.query as { queueName?: string };
  const queueName = query.queueName || "default";

  const html = `
    <!doctype html>
    <html>
      <head>
        <title>Queue Dashboard - ${queueName}</title>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
          /* (same CSS as before, unchanged) */
          /* ... keep your existing CSS block here ... */

          /* ADD styles for jobs table & chart */

          .jobs-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 12px;
            margin-top: 4px;
          }
          .jobs-table th,
          .jobs-table td {
            padding: 6px 8px;
            border-bottom: 1px solid rgba(255,255,255,0.04);
            text-align: left;
          }
          .jobs-table th {
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--muted);
          }
          .jobs-table tr:last-child td {
            border-bottom: none;
          }
          .jobs-table tbody tr:hover {
            background: rgba(255,255,255,0.02);
          }
          .badge {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 2px 8px;
            border-radius: 999px;
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
          }
          .badge--completed { background: rgba(77,255,136,0.12); color: var(--success); }
          .badge--failed { background: rgba(255,77,77,0.12); color: var(--danger); }
          .badge--active { background: rgba(255,214,102,0.12); color: #ffd666; }
          .badge--waiting { background: rgba(77,163,255,0.12); color: var(--accent); }

          .badge--prio-high { background: rgba(255,128,171,0.16); color: #ff80ab; }
          .badge--prio-medium { background: rgba(130,177,255,0.16); color: #82b1ff; }
          .badge--prio-low { background: rgba(178,223,219,0.12); color: #b2dfdb; }

          .chart-wrapper {
            position: relative;
            height: 180px;
            margin-top: 8px;
          }
        </style>
      </head>
      <body>
        <!-- SAME BODY SHELL AS BEFORE (header, controls, main-grid etc.) -->
        <!-- Only difference: right column shows chart + jobs table -->

        <div class="page">
          <div class="shell">
            <div class="app">
              <!-- header omitted for brevity, keep yours as-is -->
              <!-- ... paste the same header section you already have ... -->

              <div class="header">
                <!-- (use the same header HTML from previous version) -->
              </div>

              <div class="main-grid">
                <!-- LEFT CARD: metrics + raw JSON (same as before) -->
                <!-- ... reuse your existing left card HTML ... -->

                <!-- RIGHT CARD: chart + job history -->
                <div class="card">
                  <div class="card-header">
                    <div>
                      <div class="card-title">Activity</div>
                      <div class="card-sub">Throughput and recent jobs</div>
                    </div>
                  </div>

                  <div>
                    <div style="font-size:11px; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:4px; color:var(--muted);">
                      Throughput (completed / active)
                    </div>
                    <div class="chart-wrapper">
                      <canvas id="throughput-chart"></canvas>
                    </div>
                  </div>

                  <div style="margin-top:14px;">
                    <div class="card-header" style="margin-bottom:4px;">
                      <div class="card-title">Recent jobs</div>
                      <div class="card-sub" id="jobs-count-label">Last 0 jobs</div>
                    </div>
                    <table class="jobs-table">
                      <thead>
                        <tr>
                          <th>ID</th>
                          <th>Status</th>
                          <th>Priority</th>
                          <th>Attempts</th>
                          <th>Updated</th>
                        </tr>
                      </thead>
                      <tbody id="jobs-body">
                        <tr><td colspan="5" style="text-align:center; color:var(--muted); padding:10px 0;">No jobs yet.</td></tr>
                      </tbody>
                    </table>
                  </div>
                </div>
              </div> <!-- main-grid -->
            </div>
          </div>
        </div>

        <script>
          (function() {
            const queueInput = document.getElementById('queue-input');
            const refreshBtn = document.getElementById('refresh-btn');
            const autoBtn = document.getElementById('auto-btn');
            const intervalSelect = document.getElementById('interval-select');

            const queueLabel = document.getElementById('queue-label');
            const mQueueName = document.getElementById('m-queue-name');
            const apiQueueName = document.getElementById('api-queue-name');

            const mWaiting = document.getElementById('m-waiting');
            const mWaitingHigh = document.getElementById('m-waiting-high');
            const mWaitingMedium = document.getElementById('m-waiting-medium');
            const mWaitingLow = document.getElementById('m-waiting-low');
            const mActive = document.getElementById('m-active');
            const mDelayed = document.getElementById('m-delayed');
            const mCompleted = document.getElementById('m-completed');
            const mFailed = document.getElementById('m-failed');
            const lastUpdated = document.getElementById('last-updated');
            const statusIndicator = document.getElementById('status-indicator');

            const jsonOutput = document.getElementById('json-output');
            const jsonToggle = document.getElementById('json-toggle');
            const jsonContainer = document.getElementById('json-container');
            const chevron = document.getElementById('chevron');

            const jobsBody = document.getElementById('jobs-body');
            const jobsCountLabel = document.getElementById('jobs-count-label');

            const chartCanvas = document.getElementById('throughput-chart');
            let throughputChart = null;
            const chartLabels = [];
            const chartCompleted = [];
            const chartActive = [];

            let auto = true;
            let timer = null;

            function setStatus(ok, message) {
              statusIndicator.innerHTML = '';
              const dot = document.createElement('span');
              dot.className = 'status-dot ' + (ok ? 'status-dot--ok' : 'status-dot--error');
              const text = document.createElement('span');
              text.textContent = message;
              statusIndicator.appendChild(dot);
              statusIndicator.appendChild(text);
            }

            function formatTime(date) {
              return date.toLocaleTimeString(undefined, {
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
              });
            }

            function formatShortTime(ms) {
              const d = new Date(ms);
              return d.toLocaleTimeString(undefined, {
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
              });
            }

            function initChart() {
              if (!chartCanvas) return;
              const ctx = chartCanvas.getContext('2d');
              throughputChart = new Chart(ctx, {
                type: 'line',
                data: {
                  labels: chartLabels,
                  datasets: [
                    {
                      label: 'Completed',
                      data: chartCompleted,
                      borderWidth: 2,
                      tension: 0.2
                    },
                    {
                      label: 'Active',
                      data: chartActive,
                      borderWidth: 2,
                      tension: 0.2
                    }
                  ]
                },
                options: {
                  responsive: true,
                  maintainAspectRatio: false,
                  plugins: {
                    legend: {
                      labels: { color: '#c7c9e6', font: { size: 11 } }
                    }
                  },
                  scales: {
                    x: {
                      ticks: { color: '#8b90aa', font: { size: 10 } },
                      grid: { color: 'rgba(255,255,255,0.03)' }
                    },
                    y: {
                      ticks: { color: '#8b90aa', font: { size: 10 } },
                      grid: { color: 'rgba(255,255,255,0.03)' },
                      beginAtZero: true,
                      suggestedMax: 5
                    }
                  },
                  animation: false
                }
              });
            }

            function updateChart(metrics) {
              if (!throughputChart) return;
              const label = formatTime(new Date());
              chartLabels.push(label);
              chartCompleted.push(metrics.completed ?? 0);
              chartActive.push(metrics.active ?? 0);

              const maxPoints = 20;
              if (chartLabels.length > maxPoints) {
                chartLabels.shift();
                chartCompleted.shift();
                chartActive.shift();
              }

              throughputChart.update();
            }

            async function fetchHistory() {
              const qName = queueInput.value.trim() || 'default';
              const url = '/jobs?queueName=' + encodeURIComponent(qName) + '&limit=10';
              try {
                const res = await fetch(url);
                if (!res.ok) throw new Error('HTTP ' + res.status);
                const json = await res.json();
                const jobs = json.jobs || [];

                jobsBody.innerHTML = '';
                if (jobs.length === 0) {
                  const row = document.createElement('tr');
                  row.innerHTML = '<td colspan="5" style="text-align:center; color:var(--muted); padding:10px 0;">No jobs yet.</td>';
                  jobsBody.appendChild(row);
                } else {
                  jobs.forEach((job) => {
                    const row = document.createElement('tr');
                    const shortId = String(job.id || '').slice(0, 8) + '…';

                    const status = (job.status || '').toLowerCase();
                    let statusClass = 'badge--waiting';
                    if (status === 'completed') statusClass = 'badge--completed';
                    else if (status === 'failed') statusClass = 'badge--failed';
                    else if (status === 'active') statusClass = 'badge--active';

                    const prio = (job.priority || '').toLowerCase();
                    let prioClass = 'badge--prio-medium';
                    if (prio === 'high') prioClass = 'badge--prio-high';
                    else if (prio === 'low') prioClass = 'badge--prio-low';

                    const attemptsStr = (job.attempts ?? 0) + '/' + (job.maxAttempts ?? 0);
                    const updatedStr = job.updatedAt ? formatShortTime(job.updatedAt) : '–';

                    row.innerHTML = \`
                      <td><code>\${shortId}</code></td>
                      <td><span class="badge \${statusClass}">\${status || 'unknown'}</span></td>
                      <td><span class="badge \${prioClass}">\${prio || '-'}</span></td>
                      <td>\${attemptsStr}</td>
                      <td>\${updatedStr}</td>
                    \`;
                    jobsBody.appendChild(row);
                  });
                }

                jobsCountLabel.textContent = 'Last ' + jobs.length + ' jobs';
              } catch (err) {
                console.error('Failed to fetch jobs', err);
              }
            }

            async function fetchMetrics() {
              const qName = queueInput.value.trim() || 'default';
              const url = '/metrics?queueName=' + encodeURIComponent(qName);

              try {
                const res = await fetch(url);
                if (!res.ok) {
                  throw new Error('HTTP ' + res.status);
                }
                const json = await res.json();

                // Update metrics
                mWaiting.textContent = json.waiting ?? 0;
                mWaitingHigh.textContent = json.waitingHigh ?? 0;
                mWaitingMedium.textContent = json.waitingMedium ?? 0;
                mWaitingLow.textContent = json.waitingLow ?? 0;
                mActive.textContent = json.active ?? 0;
                mDelayed.textContent = json.delayed ?? 0;
                mCompleted.textContent = json.completed ?? 0;
                mFailed.textContent = json.failed ?? 0;

                mQueueName.textContent = json.queueName ?? qName;
                queueLabel.innerHTML = 'Queue: <code>' + (json.queueName ?? qName) + '</code>';
                apiQueueName.textContent = json.queueName ?? qName;

                lastUpdated.textContent = 'Last updated: ' + formatTime(new Date());
                jsonOutput.textContent = JSON.stringify(json, null, 2);

                updateChart(json);
                setStatus(true, 'Connected to /metrics');

                // also refresh job history
                fetchHistory();
              } catch (err) {
                console.error('Failed to fetch metrics', err);
                setStatus(false, 'Error reaching /metrics');
              }
            }

            function scheduleAuto() {
              if (timer) clearInterval(timer);
              if (!auto) return;
              const interval = Number(intervalSelect.value) || 2000;
              timer = setInterval(fetchMetrics, interval);
            }

            refreshBtn.addEventListener('click', function() {
              fetchMetrics();
            });

            autoBtn.addEventListener('click', function() {
              auto = !auto;
              if (auto) {
                autoBtn.classList.add('pill-toggle--on');
              } else {
                autoBtn.classList.remove('pill-toggle--on');
              }
              scheduleAuto();
            });

            intervalSelect.addEventListener('change', function() {
              scheduleAuto();
            });

            jsonToggle.addEventListener('click', function() {
              const isHidden = jsonContainer.style.display === 'none';
              jsonContainer.style.display = isHidden ? 'block' : 'none';
              if (isHidden) {
                chevron.classList.add('chevron--open');
              } else {
                chevron.classList.remove('chevron--open');
              }
            });

            // Start open
            jsonContainer.style.display = 'block';
            chevron.classList.add('chevron--open');

            initChart();
            fetchMetrics();
            scheduleAuto();
          })();
        </script>
      </body>
    </html>
  `;

  reply.header("Content-Type", "text/html; charset=utf-8");
  return reply.send(html);
}
);}
