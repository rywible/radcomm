import {
  index,
  integer,
  jsonb,
  pgTable,
  timestamp,
  uniqueIndex,
  uuid,
  varchar,
} from "drizzle-orm/pg-core";

export const EventsTable = pgTable(
  "events",
  {
    id: uuid("id").primaryKey(),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull(),
    eventName: varchar("event_name", { length: 255 }).notNull(),
    correlationId: uuid("correlation_id").notNull(),
    aggregateId: uuid("aggregate_id").notNull(),
    version: integer("version").notNull(),
    payload: jsonb("payload").notNull(),
  },
  (table) => [
    uniqueIndex("events_aggregate_id_version_unique").on(
      table.aggregateId,
      table.version
    ),
  ]
);
