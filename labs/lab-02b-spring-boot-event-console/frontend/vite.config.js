import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// `npm run dev` mirrors what nginx does in the container (see nginx/default.conf.template):
// /api/events is the CONSUMER service, everything else under /api is the PRODUCER service.
// Run them locally with: (cd producer && ./gradlew bootRun) on :8080 and
// (cd consumer && ./gradlew bootRun --args='--server.port=8081') on :8081 -- or point the proxy
// anywhere else (a `kubectl port-forward`, say) with PRODUCER_URL / CONSUMER_URL.
//
// Note what the proxy does NOT do: nginx also enforces the login and the rate limits. The dev server
// talks to the services directly, so use it only against services you are allowed to reach.
const producer = process.env.PRODUCER_URL ?? 'http://localhost:8080'
const consumer = process.env.CONSUMER_URL ?? 'http://localhost:8081'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api/events': consumer,
      '/api': producer,
    },
  },
})
