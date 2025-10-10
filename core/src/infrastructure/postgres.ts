// db.ts
import { drizzle } from "drizzle-orm/node-postgres";
import * as schema from "./orm";
import { env } from "../env";

// Pass both the connection string and schema:
export const db = drizzle(env.DATABASE_URL, { schema });

export type DB = typeof db;
export type TX = Parameters<Parameters<DB["transaction"]>[0]>[0];
