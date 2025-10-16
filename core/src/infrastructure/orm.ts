import {
  integer,
  jsonb,
  pgTable,
  timestamp,
  uuid,
  varchar,
  primaryKey,
} from "drizzle-orm/pg-core";

export const EventsTable = pgTable(
  "events",
  {
    createdAt: timestamp("created_at", { withTimezone: true }).notNull(),
    eventName: varchar("event_name", { length: 255 }).notNull(),
    correlationId: uuid("correlation_id").notNull(),
    aggregateId: uuid("aggregate_id").notNull(),
    version: integer("version").notNull(),
    payload: jsonb("payload").notNull(),
  },
  (table) => [primaryKey({ columns: [table.aggregateId, table.version] })]
);

export const OutboxTable = pgTable("outbox", {
  id: uuid("id").primaryKey(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull(),
  event: jsonb("event").notNull(),
});
