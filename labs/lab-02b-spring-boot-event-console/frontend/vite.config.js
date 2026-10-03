import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// `npm run dev` mirrors what nginx does in the container (see nginx/default.conf.template):
// /api/events is the CONSUMER service, everything else under /api is the PRODUCER service.
// Run them locally with: (cd producer && ./gradlew bootRun) on :8080 and
// (cd consumer && ./gradlew bootRun --args='--server.port=8081') on :8081.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api/events': 'http://localhost:8081',
      '/api': 'http://localhost:8080',
    },
  },
})
