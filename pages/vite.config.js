import { resolve } from 'path';
import { defineConfig } from 'vite';
import { splitVendorChunkPlugin } from 'vite';
import lightningcss from 'vite-plugin-lightningcss';
import htmlPurge from 'vite-plugin-purgecss'


export default defineConfig({
  plugins: [
    splitVendorChunkPlugin(),
    htmlPurge({
      safelist: ['strength-meter__bar--ok','strength-meter__bar--alert','strength-meter__bar--warning']
    }),
    lightningcss({
      browserslist: '>= 0.25%',
    }),
  ],

  build: {
    minify: true,
    rollupOptions: {
      input: {
        main: resolve(__dirname, 'index.html'),
        register: resolve(__dirname, 'register/index.html'),
        login: resolve(__dirname, 'login/index.html'),
        check_email: resolve(__dirname, 'check_email/index.html'),
        verify: resolve(__dirname, 'verify/index.html'),
      },
    },
  },
});
