import { resolve } from 'path';
import { defineConfig } from 'vite';
import { splitVendorChunkPlugin } from 'vite';
import lightningcss from 'vite-plugin-lightningcss';
import htmlPurge from 'vite-plugin-purgecss'


export default defineConfig({
  plugins: [
    splitVendorChunkPlugin(),
    htmlPurge(),
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
      },
    },
  },
});
