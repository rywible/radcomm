import type { DomainEvent } from "../domain/_base/domainEvent";
import type { IntegrationEvent } from "../integrationEvents/_base";
import { EventsTable, OutboxTable } from "./orm";
import { eq, asc } from "drizzle-orm";
import type { TX } from "./postgres";
import {
  ProductCreatedEvent,
  ProductVariantLinkedEvent,
  ProductArchivedEvent,
} from "../domain/product/events";
import {
  ProductVariantCreatedEvent,
  ProductVariantArchivedEvent,
} from "../domain/productVariant/events";
import {
  ProductCreatedIntegrationEvent,
  ProductArchivedIntegrationEvent,
} from "../integrationEvents/product";
import {
  ProductVariantCreatedIntegrationEvent,
  ProductVariantArchivedIntegrationEvent,
} from "../integrationEvents/productVariant";

type TransactionalClient = Pick<TX, "insert" | "select" | "update" | "delete">;

const UNIQUE_VIOLATION_ERROR_CODE = "23505";

const isUniqueConstraintViolation = (
  error: unknown,
  code: string
): error is { code: string } =>
  typeof error === "object" &&
  error !== null &&
  "code" in error &&
  typeof (error as { code: unknown }).code === "string" &&
  (error as { code: string }).code === code;

export class EventVersionConflictError extends Error {
  constructor(aggregateId: string, version: number) {
    super(
      `Event version ${version} already exists for aggregate ${aggregateId}`
    );
    this.name = "EventVersionConflictError";
  }
}

export class EventRepository {
  private db: TransactionalClient;

  constructor(db: TransactionalClient) {
    this.db = db;
  }

  async add(
    event: DomainEvent<string, Record<string, unknown>>
  ): Promise<void> {
    const eventEntity: typeof EventsTable.$inferInsert = {
      createdAt: event.createdAt,
      eventName: event.eventName,
      correlationId: event.correlationId,
      aggregateId: event.aggregateId,
      version: event.version,
      payload: event.payload,
    };
    try {
      await this.db.insert(EventsTable).values(eventEntity);
    } catch (error) {
      if (isUniqueConstraintViolation(error, UNIQUE_VIOLATION_ERROR_CODE)) {
        throw new EventVersionConflictError(event.aggregateId, event.version);
      }
      throw error;
    }
  }

  async findByAggregateId(
    aggregateId: string
  ): Promise<Array<DomainEvent<string, Record<string, unknown>>>> {
    const events = await this.db
      .select()
      .from(EventsTable)
      .where(eq(EventsTable.aggregateId, aggregateId))
      .orderBy(asc(EventsTable.version));

    return events.map((event: typeof EventsTable.$inferSelect) => {
      switch (event.eventName) {
        case "ProductCreated":
          return new ProductCreatedEvent({
            createdAt: event.createdAt,
            aggregateId: event.aggregateId,
            correlationId: event.correlationId,
            version: event.version,
            payload: event.payload as {
              title: string;
              description: string;
              slug: string;
              collectionIds: string[];
              variantIds: string[];
            },
            committed: true,
          });
        case "ProductVariantLinked":
          return new ProductVariantLinkedEvent({
            createdAt: event.createdAt,
            aggregateId: event.aggregateId,
            correlationId: event.correlationId,
            version: event.version,
            payload: event.payload as {
              variantId: string;
            },
            committed: true,
          });
        case "ProductArchived":
          return new ProductArchivedEvent({
            createdAt: event.createdAt,
            aggregateId: event.aggregateId,
            correlationId: event.correlationId,
            version: event.version,
            payload: {},
            committed: true,
          });
        case "ProductVariantCreated":
          return new ProductVariantCreatedEvent({
            createdAt: event.createdAt,
            aggregateId: event.aggregateId,
            correlationId: event.correlationId,
            version: event.version,
            payload: event.payload as {
              productId: string;
              sku: string;
              priceCents: number;
              imageUrl: string;
              size: string;
              color: string;
              quantity: number;
            },
            committed: true,
          });
        case "ProductVariantArchived":
          const payload = event.payload as { sku: string };
          return new ProductVariantArchivedEvent({
            createdAt: event.createdAt,
            aggregateId: event.aggregateId,
            correlationId: event.correlationId,
            version: event.version,
            payload: {},
            committed: true,
          });
        default:
          throw new Error(`Unknown event type: ${event.eventName}`);
      }
    });
  }
}

export class OutboxRepository {
  private db: TransactionalClient;

  constructor(db: TransactionalClient) {
    this.db = db;
  }

  async add(
    integrationEvent: IntegrationEvent<string, Record<string, unknown>>
  ): Promise<void> {
    const outboxMessage: typeof OutboxTable.$inferInsert = {
      id: integrationEvent.eventId,
      createdAt: integrationEvent.occurredAt,
      event: integrationEvent,
    };

    await this.db.insert(OutboxTable).values(outboxMessage);
  }
}
