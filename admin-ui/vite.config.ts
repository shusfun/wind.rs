import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    host: '127.0.0.1',
    port: 9527,
    strictPort: true,
    proxy: {
      '/health': 'http://127.0.0.1:3003',
      '/setup': 'http://127.0.0.1:3003',
      '/auth': 'http://127.0.0.1:3003',
      '/admin': 'http://127.0.0.1:3003',
      '/v1': 'http://127.0.0.1:3003',
    },
  },
});
