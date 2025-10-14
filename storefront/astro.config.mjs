// @ts-check
import { defineConfig } from "astro/config";

import tailwindcss from "@tailwindcss/vite";

// https://astro.build/config
export default defineConfig({
  vite: {
    // @ts-expect-error
    plugins: [tailwindcss()],
    resolve: {
      alias: {
        "@core": new URL("../core/src", import.meta.url).pathname,
        "@storefront": new URL("./src", import.meta.url).pathname,
        "@ui": new URL("./src/components/ui", import.meta.url).pathname,
      },
    },
    server: {
      fs: {
        // allow importing from monorepo root
        allow: [new URL("../..", import.meta.url).pathname],
      },
    },
  },
});
