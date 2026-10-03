import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  clearEvents,
  getConfig,
  getEvents,
  getConsumerConfig,
  getJobs,
  getStats,
  parsePastedLines,
  publishBulk,
  requeueEvent,
} from './api.js'

const nf = new Intl.NumberFormat()

function Bars({ counts, partitions }) {
  const entries = []
  for (let p = 0; p < partitions; p++) entries.push([String(p), counts?.[String(p)] ?? 0])
  // Show partitions the data actually reached even if config says fewer.
  Object.keys(counts ?? {}).forEach((p) => {
    if (!entries.find(([k]) => k === p)) entries.push([p, counts[p]])
  })
  const max = Math.max(1, ...entries.map(([, n]) => n))
  return (
    <div className="bars">
      {entries.map(([p, n]) => (
        <div className="bar-row" key={p}>
          <span className="bar-label">P{p}</span>
          <div className="bar-track">
            <div className="bar-fill" style={{ width: `${(n / max) * 100}%` }} />
          </div>
          <span className="bar-n">{nf.format(n)}</span>
        </div>
      ))}
    </div>
  )
}

function PublishPanel({ config, onPublished }) {
  const [mode, setMode] = useState('GENERATE')
  const [count, setCount] = useState(100)
  const [keyStrategy, setKeyStrategy] = useState('CYCLE')
  const [keyCount, setKeyCount] = useState(5)
  const [keyPrefix, setKeyPrefix] = useState('order-')
  const [fixedKey, setFixedKey] = useState('hot-key')
  const [valuePrefix, setValuePrefix] = useState('evt')
  const [pasted, setPasted] = useState('order-1|{"status":"CREATED"}\norder-2|{"status":"PAID"}\n{"note":"no key on this one"}')
  const [busy, setBusy] = useState(false)
  const [result, setResult] = useState(null)
  const [error, setError] = useState('')

  const pastedEvents = useMemo(() => parsePastedLines(pasted), [pasted])
  const total = mode === 'GENERATE' ? Number(count) || 0 : pastedEvents.length

  async function submit(e) {
    e.preventDefault()
    setBusy(true)
    setError('')
    setResult(null)
    try {
      const request =
        mode === 'GENERATE'
          ? {
              mode,
              count: Number(count),
              keyStrategy,
              keyCount: Number(keyCount),
              keyPrefix,
              fixedKey,
              valuePrefix,
            }
          : { mode, events: pastedEvents }
      const job = await publishBulk(request)
      setResult(job)
      onPublished()
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <section className="card">
      <h2>Publish events</h2>
      <p className="muted">
        Topic <code>{config?.topic ?? '…'}</code> · {config?.partitions ?? '…'} partitions
      </p>

      <div className="tabs" role="tablist">
        <button type="button" role="tab" aria-selected={mode === 'GENERATE'}
          className={mode === 'GENERATE' ? 'tab active' : 'tab'} onClick={() => setMode('GENERATE')}>
          Generate
        </button>
        <button type="button" role="tab" aria-selected={mode === 'PASTE'}
          className={mode === 'PASTE' ? 'tab active' : 'tab'} onClick={() => setMode('PASTE')}>
          Paste events
        </button>
      </div>

      <form onSubmit={submit}>
        {mode === 'GENERATE' ? (
          <>
            <label>
              How many events
              <input type="number" min="1" max={config?.maxBulkEvents ?? 50000} value={count}
                onChange={(e) => setCount(e.target.value)} required />
            </label>
            <div className="chips">
              {[100, 1000, 10000].map((n) => (
                <button type="button" key={n} className="chip" onClick={() => setCount(n)}>
                  {nf.format(n)}
                </button>
              ))}
            </div>

            <label>
              Key strategy
              <select value={keyStrategy} onChange={(e) => setKeyStrategy(e.target.value)}>
                <option value="UNIQUE">Unique key per event (even spread)</option>
                <option value="CYCLE">Cycle through N keys (per-key ordering)</option>
                <option value="FIXED">One key for all (hot partition)</option>
                <option value="NONE">No key (sticky batches)</option>
              </select>
            </label>

            {keyStrategy === 'CYCLE' && (
              <label>
                Number of distinct keys
                <input type="number" min="1" max="10000" value={keyCount}
                  onChange={(e) => setKeyCount(e.target.value)} />
              </label>
            )}
            {(keyStrategy === 'UNIQUE' || keyStrategy === 'CYCLE') && (
              <label>
                Key prefix
                <input value={keyPrefix} onChange={(e) => setKeyPrefix(e.target.value)} />
              </label>
            )}
            {keyStrategy === 'FIXED' && (
              <label>
                The key
                <input value={fixedKey} onChange={(e) => setFixedKey(e.target.value)} />
              </label>
            )}
            <label>
              Value id prefix
              <input value={valuePrefix} onChange={(e) => setValuePrefix(e.target.value)} />
            </label>
          </>
        ) : (
          <label>
            <span>One event per line — <code>key|value</code>, or just a value for no key</span>
            <span className="muted small">
              Values must be JSON objects. Anything else is accepted by Kafka but fails processing: it is
              retried with backoff, then marked DEAD and sent to the dead-letter topic.
            </span>
            <textarea rows="9" value={pasted} onChange={(e) => setPasted(e.target.value)} spellCheck="false" />
            <span className="muted">{nf.format(pastedEvents.length)} event(s) parsed</span>
          </label>
        )}

        <button className="primary" type="submit" disabled={busy || total < 1}>
          {busy ? 'Publishing…' : `Publish ${nf.format(total)} event${total === 1 ? '' : 's'}`}
        </button>
      </form>

      {error && <div className="alert error" role="alert">{error}</div>}

      {result && (
        <div className="result" aria-live="polite">
          <div className="result-head">
            <strong>{nf.format(result.acked)}</strong> of {nf.format(result.requested)} acknowledged by Kafka
            {result.failed > 0 && <span className="badge bad">{nf.format(result.failed)} failed</span>}
            <span className="muted"> · {nf.format(result.durationMs)} ms</span>
          </div>
          <p className="muted small">Where the broker actually stored them:</p>
          <Bars counts={result.partitionCounts} partitions={config?.partitions ?? 0} />
          {result.firstError && <div className="alert error">{result.firstError}</div>}
        </div>
      )}
    </section>
  )
}

function Jobs({ jobs }) {
  return (
    <section className="card">
      <h2>Recent publish jobs</h2>
      {jobs.length === 0 ? (
        <p className="muted">Nothing published yet.</p>
      ) : (
        <ul className="jobs">
          {jobs.map((j) => (
            <li key={j.id}>
              <span>{new Date(j.createdAt).toLocaleTimeString()}</span>
              <span>{j.mode === 'PASTE' ? 'pasted' : j.keyStrategy.toLowerCase()}</span>
              <strong>{nf.format(j.acked)}/{nf.format(j.requested)}</strong>
              <span className={j.failed ? 'badge bad' : 'badge ok'}>{j.failed ? `${j.failed} failed` : 'ok'}</span>
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}

const STATUSES = ['SUCCESS', 'FAILED', 'DEAD']

function StatusBadge({ ev, maxRetries }) {
  const retryNote =
    ev.status === 'FAILED'
      ? `retry ${ev.retries + 1}/${maxRetries ?? '?'} due ${new Date(ev.nextRetryAt).toLocaleTimeString()}`
      : ev.retries > 0
        ? `${ev.retries} retr${ev.retries === 1 ? 'y' : 'ies'}`
        : ''
  const cls = ev.status === 'SUCCESS' ? 'ok' : ev.status === 'FAILED' ? 'warn' : 'bad'
  return (
    <span title={ev.lastError ?? ''}>
      <span className={`badge ${cls}`}>{ev.status}</span>
      {retryNote && <span className="muted small"> {retryNote}</span>}
    </span>
  )
}

function EventsPanel({ config, consumerConfig, stats, refreshKey, onCleared }) {
  const [filters, setFilters] = useState({ key: '', partition: '', q: '', status: '' })
  const [page, setPage] = useState(0)
  const [size, setSize] = useState(25)
  const [data, setData] = useState({ items: [], total: 0 })
  const [auto, setAuto] = useState(true)
  const [err, setErr] = useState('')

  const load = useCallback(async () => {
    try {
      setData(await getEvents({ ...filters, page, size }))
      setErr('')
    } catch (e) {
      setErr(e.message)
    }
  }, [filters, page, size])

  useEffect(() => { load() }, [load, refreshKey])
  useEffect(() => {
    if (!auto) return undefined
    const t = setInterval(load, 2000)
    return () => clearInterval(t)
  }, [auto, load])

  const update = (patch) => { setFilters((f) => ({ ...f, ...patch })); setPage(0) }
  const from = data.total === 0 ? 0 : page * size + 1
  const to = Math.min((page + 1) * size, data.total)
  const lastPage = Math.max(0, Math.ceil(data.total / size) - 1)

  async function clear() {
    if (!window.confirm('Clear the MongoDB copy of consumed events? The records stay in Kafka.')) return
    await clearEvents()
    setPage(0)
    onCleared()
  }

  async function requeue(id) {
    try {
      await requeueEvent(id)
      setErr('')
      load()
      onCleared()
    } catch (e) {
      setErr(e.message)
    }
  }

  return (
    <section className="card">
      <div className="events-head">
        <h2>Consumed events <span className="muted">· stored in MongoDB</span></h2>
        <div className="actions">
          <label className="inline">
            <input type="checkbox" checked={auto} onChange={(e) => setAuto(e.target.checked)} />
            Live (2s)
          </label>
          <button type="button" className="ghost" onClick={load}>Refresh</button>
          <button type="button" className="ghost danger" onClick={clear}>Clear</button>
        </div>
      </div>

      <div className="stats">
        <div className="stat"><span>{nf.format(stats.total ?? 0)}</span> total stored</div>
        <div className="stat-bars"><Bars counts={stats.byPartition} partitions={config?.partitions ?? 0} /></div>
      </div>

      <div className="status-chips" role="group" aria-label="Filter by status">
        <button type="button" className={filters.status === '' ? 'chip active' : 'chip'} onClick={() => update({ status: '' })}>
          All
        </button>
        {STATUSES.map((s) => (
          <button type="button" key={s} className={filters.status === s ? 'chip active' : 'chip'}
            onClick={() => update({ status: filters.status === s ? '' : s })}>
            {s} · {nf.format(stats.byStatus?.[s] ?? 0)}
          </button>
        ))}
      </div>

      <div className="filters">
        <input placeholder="Filter by key…" value={filters.key} onChange={(e) => update({ key: e.target.value })} />
        <select value={filters.partition} onChange={(e) => update({ partition: e.target.value })}>
          <option value="">All partitions</option>
          {Array.from({ length: config?.partitions ?? 0 }, (_, p) => (
            <option key={p} value={p}>Partition {p}</option>
          ))}
        </select>
        <input placeholder="Search in value…" value={filters.q} onChange={(e) => update({ q: e.target.value })} />
      </div>

      {err && <div className="alert error">{err}</div>}

      <div className="table-wrap">
        <table>
          <thead>
            <tr><th>Consumed</th><th>P</th><th>Offset</th><th>Key</th><th>Value</th><th>Status</th></tr>
          </thead>
          <tbody>
            {data.items.length === 0 ? (
              <tr><td colSpan="6" className="empty">No events match. Publish some on the left.</td></tr>
            ) : (
              data.items.map((ev) => (
                <tr key={ev.id}>
                  <td className="nowrap">{new Date(ev.consumedAt).toLocaleTimeString()}</td>
                  <td><span className={`pill p${ev.partition % 6}`}>{ev.partition}</span></td>
                  <td className="mono">{ev.offset}</td>
                  <td className="mono">{ev.key ?? <em className="muted">null</em>}</td>
                  <td className="mono value" title={ev.value}>{ev.value}</td>
                  <td className="nowrap">
                    <StatusBadge ev={ev} maxRetries={consumerConfig?.maxRetries} />
                    {ev.status === 'DEAD' && (
                      <button type="button" className="ghost small-btn" onClick={() => requeue(ev.id)}
                        title={ev.lastError ?? ''}>
                        Requeue
                      </button>
                    )}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      <div className="pager">
        <span className="muted">{nf.format(from)}–{nf.format(to)} of {nf.format(data.total)}</span>
        <div className="actions">
          <select value={size} onChange={(e) => { setSize(Number(e.target.value)); setPage(0) }}>
            {[10, 25, 50, 100].map((n) => <option key={n} value={n}>{n} / page</option>)}
          </select>
          <button type="button" className="ghost" disabled={page === 0} onClick={() => setPage(page - 1)}>Prev</button>
          <button type="button" className="ghost" disabled={page >= lastPage} onClick={() => setPage(page + 1)}>Next</button>
        </div>
      </div>
    </section>
  )
}

export default function App() {
  const [config, setConfig] = useState(null)
  const [consumerConfig, setConsumerConfig] = useState(null)
  const [stats, setStats] = useState({ total: 0, byPartition: {}, byStatus: {} })
  const [jobs, setJobs] = useState([])
  const [refreshKey, setRefreshKey] = useState(0)
  const [online, setOnline] = useState(true)

  const refreshSide = useCallback(async () => {
    try {
      const [s, j] = await Promise.all([getStats(), getJobs()])
      setStats(s)
      setJobs(j)
      setOnline(true)
    } catch {
      setOnline(false)
    }
  }, [])

  useEffect(() => { getConfig().then(setConfig).catch(() => setOnline(false)) }, [])
  useEffect(() => { getConsumerConfig().then(setConsumerConfig).catch(() => setOnline(false)) }, [])
  useEffect(() => {
    refreshSide()
    const t = setInterval(refreshSide, 2000)
    return () => clearInterval(t)
  }, [refreshSide, refreshKey])

  const bump = () => setRefreshKey((k) => k + 1)

  return (
    <div className="app">
      <header>
        <div>
          <h1>Kafka Event Console</h1>
          <p className="muted">Spring Boot · Kafka · MongoDB · React</p>
        </div>
        <div className={online ? 'status ok' : 'status bad'}>
          <span className="dot" />{online ? 'Backend connected' : 'Backend unreachable'}
        </div>
      </header>

      <main>
        <div className="col-left">
          <PublishPanel config={config} onPublished={bump} />
          <Jobs jobs={jobs} />
        </div>
        <div className="col-right">
          <EventsPanel config={config} consumerConfig={consumerConfig} stats={stats} refreshKey={refreshKey} onCleared={bump} />
        </div>
      </main>
    </div>
  )
}
