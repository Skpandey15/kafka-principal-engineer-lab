async function call(path, options = {}) {
  const res = await fetch('/api' + path, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  })
  const body = await res.json().catch(() => null)
  if (!res.ok) {
    throw new Error(body?.error || `${res.status} ${res.statusText}`)
  }
  return body
}

export const getConfig = () => call('/config')
export const getStats = () => call('/events/stats')
export const getConsumerConfig = () => call('/events/config')
export const requeueEvent = (id) => call(`/events/${encodeURIComponent(id)}/requeue`, { method: 'POST' })
export const getJobs = (limit = 8) => call(`/jobs?limit=${limit}`)
export const clearEvents = () => call('/events', { method: 'DELETE' })
export const publishBulk = (request) =>
  call('/publish/bulk', { method: 'POST', body: JSON.stringify(request) })

export function getEvents({ key, partition, q, status, page, size }) {
  const params = new URLSearchParams({ page, size })
  if (key) params.set('key', key)
  if (status) params.set('status', status)
  if (partition !== '' && partition != null) params.set('partition', partition)
  if (q) params.set('q', q)
  return call('/events?' + params.toString())
}

/** One event per non-empty line: "key|value", or just "value" for a keyless event. */
export function parsePastedLines(text) {
  return text
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const i = line.indexOf('|')
      if (i < 0) return { key: null, value: line }
      return { key: line.slice(0, i).trim() || null, value: line.slice(i + 1).trim() }
    })
    .filter((e) => e.value)
}
