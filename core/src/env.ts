// packages/backend/src/env.ts
import path from "node:path";
import { config as loadEnv } from "dotenv";
loadEnv({ path: path.resolve(__dirname, "../..", ".env") });
import { z } from "zod";

const EnvSchema = z.object({
  DATABASE_URL: z.url(),
  NODE_ENV: z
    .enum(["development", "production", "test"])
    .default("development"),
});

export const env = EnvSchema.parse(process.env);
