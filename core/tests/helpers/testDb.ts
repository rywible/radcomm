import { drizzle } from "drizzle-orm/node-postgres";
import * as schema from "../../src/infrastructure/orm";
import { sql } from "drizzle-orm";
import type { DB } from "../../src/infrastructure/postgres";
import { Pool } from "pg";

let testDb: DB | undefined = undefined;

export async function getTestDb(): Promise<DB> {
  if (!testDb) {
    const DATABASE_URL = process.env.DATABASE_URL!;
    const pool = new Pool({
      connectionString: DATABASE_URL,
      max: 100, // Max connections per process
      min: 20, // Min connections to keep alive
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    });

    testDb = drizzle(pool, { schema });
  }
  return testDb;
}

export async function cleanDatabase(db: DB) {
  // Truncate all tables in the correct order to respect foreign keys
  await db.execute(sql`TRUNCATE TABLE outbox_dead_letter CASCADE`);
  await db.execute(sql`TRUNCATE TABLE outbox CASCADE`);
  await db.execute(sql`TRUNCATE TABLE inbox CASCADE`);
  await db.execute(sql`TRUNCATE TABLE events CASCADE`);
  await db.execute(sql`TRUNCATE TABLE collection_detail_view CASCADE`);
  await db.execute(sql`TRUNCATE TABLE collection_list_view CASCADE`);
  await db.execute(sql`TRUNCATE TABLE product_detail_view CASCADE`);
  await db.execute(sql`TRUNCATE TABLE product_list_view CASCADE`);
}

export async function closeDatabase() {
  testDb = undefined;
}
