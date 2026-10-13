import { defineConfig } from 'vite'

// Works around a lightningcss minifier crash on this Slidev version's own bundled CSS
// (unrelated to this deck's content) when running `slidev build`.
export default defineConfig({
  build: {
    cssMinify: false,
  },
})
