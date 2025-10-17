import { v4 as uuidv4 } from "uuid";
import type { IntegrationEvent } from "../../src/integrationEvents/_base";
import type { DB } from "../../src/infrastructure/postgres";
import { OutboxTable } from "../../src/infrastructure/orm";

export interface CreateOutboxMessageOptions {
  id?: string;
  status?: string;
  leasedAt?: Date | null;
  nextAvailableAt?: Date | null;
  attempts?: number;
  event?: IntegrationEvent<string, Record<string, unknown>>;
}

export async function createOutboxMessage(
  db: DB,
  options: CreateOutboxMessageOptions = {}
): Promise<string> {
  const id = options.id || uuidv4();
  const event = options.event || createTestIntegrationEvent();

  await db.insert(OutboxTable).values({
    id,
    status: options.status || "pending",
    leasedAt: options.leasedAt !== undefined ? options.leasedAt : null,
    nextAvailableAt:
      options.nextAvailableAt !== undefined ? options.nextAvailableAt : null,
    attempts: options.attempts || 0,
    event: event as any,
  });

  return id;
}

export function createTestIntegrationEvent(
  overrides: Partial<IntegrationEvent<string, Record<string, unknown>>> = {}
): IntegrationEvent<string, Record<string, unknown>> {
  return {
    eventId: uuidv4(),
    eventName: "product.created",
    occurredAt: new Date(),
    correlationId: uuidv4(),
    payload: {
      productId: uuidv4(),
      title: "Test Product",
      description: "Test Description",
      slug: "test-product",
      status: "active",
    },
    ...overrides,
  };
}

export function createTestIntegrationEventWithName(
  eventName: string
): IntegrationEvent<string, Record<string, unknown>> {
  return createTestIntegrationEvent({ eventName });
}

export async function createMultipleOutboxMessages(
  db: DB,
  count: number,
  options: CreateOutboxMessageOptions = {}
): Promise<string[]> {
  const ids: string[] = [];
  for (let i = 0; i < count; i++) {
    const id = await createOutboxMessage(db, {
      ...options,
      event: createTestIntegrationEvent({
        payload: { index: i },
      }),
    });
    ids.push(id);
  }
  return ids;
}
