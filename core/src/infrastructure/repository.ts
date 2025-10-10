import type { DomainEvent } from "../domain/_base/domainEvent";
import { EventsTable } from "./orm";
import { eq, asc } from "drizzle-orm";
import type { TX } from "./postgres";
import { OrderRequestedEvent } from "../domain/order/events";

type TransactionalClient = Pick<TX, "insert" | "select">;

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
      id: event.id,
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

    return events.map((event) => {
      switch (event.eventName) {
        case "OrderRequested":
          return new OrderRequestedEvent({
            id: event.id,
            createdAt: event.createdAt,
            aggregateId: event.aggregateId,
            correlationId: event.correlationId,
            version: event.version,
            payload: event.payload as {
              customerId: string;
              items: Array<{ sku: string; qty: number }>;
            },
          });
        default:
          throw new Error(`Unknown event type: ${event.eventName}`);
      }
    });
  }
}
