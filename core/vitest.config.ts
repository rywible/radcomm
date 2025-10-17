import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["tests/**/*.spec.ts", "tests/**/*.bench.ts"],
    testTimeout: 30000,
    hookTimeout: 30000,
    pool: "forks",
    poolOptions: {
      forks: {
        singleFork: false,
      },
    },
    // Show console.log output from tests (especially performance metrics)
    silent: false,
    // Show test names
    reporters: ["default"],
    // Benchmark settings
    benchmark: {
      outputFile: "./benchmark-results.json",
    },
  },
});
