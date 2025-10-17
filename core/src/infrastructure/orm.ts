import {
  integer,
  jsonb,
  pgTable,
  timestamp,
  uuid,
  varchar,
  primaryKey,
  index,
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

// Outbox for integration events
export const OutboxTable = pgTable(
  "outbox",
  {
    id: uuid("id").primaryKey(),
    status: varchar("status", { length: 50 }).notNull(),
    leasedAt: timestamp("leased_at", { withTimezone: true }),
    nextAvailableAt: timestamp("next_available_at", { withTimezone: true }),
    attempts: integer("attempts").notNull(),
    event: jsonb("event").notNull(),
  },
  (table) => [
    // Primary scheduling index for batch selection
    index("idx_outbox_scheduling").on(
      table.status,
      table.nextAvailableAt,
      table.id
    ),
    // Index for stale lease detection
    index("idx_outbox_stale_leases").on(table.status, table.leasedAt),
    // Index for status-only queries
    index("idx_outbox_status").on(table.status),
    // Index for attempts-based queries (monitoring)
    index("idx_outbox_attempts").on(table.attempts),
  ]
);

export const OutboxDeadLetterTable = pgTable("outbox_dead_letter", {
  id: uuid("id").primaryKey(),
  failedAt: timestamp("failed_at", { withTimezone: true }).notNull(),
  event: jsonb("event").notNull(),
  lastError: varchar("last_error", { length: 255 }).notNull(),
});

export const InboxTable = pgTable("inbox", {
  id: uuid("id").primaryKey(),
});

// Read Models
export const ProductListViewTable = pgTable("product_list_view", {
  productId: uuid("product_id").primaryKey(),
  title: varchar("title", { length: 255 }).notNull(),
  slug: varchar("slug", { length: 255 }).notNull().unique(),
  description: varchar("description").notNull(),
  status: varchar("status", { length: 50 }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull(),
  variantCount: integer("variant_count").notNull().default(0),
  collections: jsonb("collections").notNull().default("[]"),
});

export const ProductDetailViewTable = pgTable("product_detail_view", {
  variantId: uuid("variant_id").primaryKey(),
  productId: uuid("product_id").notNull(),
  productTitle: varchar("product_title", { length: 255 }).notNull(),
  productSlug: varchar("product_slug", { length: 255 }).notNull(),
  productDescription: varchar("product_description").notNull(),
  productStatus: varchar("product_status", { length: 50 }).notNull(),
  productCreatedAt: timestamp("product_created_at", {
    withTimezone: true,
  }).notNull(),
  sku: varchar("sku", { length: 255 }).notNull().unique(),
  priceCents: integer("price_cents").notNull(),
  size: varchar("size", { length: 100 }).notNull(),
  color: varchar("color", { length: 100 }).notNull(),
  imageUrl: varchar("image_url").notNull(),
  variantStatus: varchar("variant_status", { length: 50 }).notNull(),
});

export const CollectionListViewTable = pgTable("collection_list_view", {
  collectionId: uuid("collection_id").primaryKey(),
  name: varchar("name", { length: 255 }).notNull(),
  slug: varchar("slug", { length: 255 }).notNull().unique(),
  description: varchar("description").notNull(),
  status: varchar("status", { length: 50 }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull(),
  productCount: integer("product_count").notNull().default(0),
});

export const CollectionDetailViewTable = pgTable(
  "collection_detail_view",
  {
    collectionId: uuid("collection_id").notNull(),
    productId: uuid("product_id").notNull(),
    collectionName: varchar("collection_name", { length: 255 }).notNull(),
    collectionSlug: varchar("collection_slug", { length: 255 }).notNull(),
    collectionDescription: varchar("collection_description").notNull(),
    collectionStatus: varchar("collection_status", { length: 50 }).notNull(),
    collectionCreatedAt: timestamp("collection_created_at", {
      withTimezone: true,
    }).notNull(),
    productTitle: varchar("product_title", { length: 255 }).notNull(),
    productSlug: varchar("product_slug", { length: 255 }).notNull(),
    productStatus: varchar("product_status", { length: 50 }).notNull(),
    variantCount: integer("variant_count").notNull().default(0),
  },
  (table) => [primaryKey({ columns: [table.collectionId, table.productId] })]
);
