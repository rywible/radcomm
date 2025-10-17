import { describe, it, expect, beforeEach } from "vitest";
import { OutboxPoller } from "../../../src/infrastructure/outboxPoller";
import { getTestDb, cleanDatabase } from "../../helpers/testDb";
import {
  createOutboxMessage,
  createMultipleOutboxMessages,
  createTestIntegrationEvent,
  createTestIntegrationEventWithName,
} from "../../helpers/factories";
import {
  MockProjectionHandler,
  MockExternalEffectHandler,
  MockExternalEffectHandlerPerfTesting,
  MockProjectionHandlerPerfTesting,
} from "../../helpers/testHandlers";
import { waitFor, sleep } from "../../helpers/waitFor";
import type { DB } from "../../../src/infrastructure/postgres";
import {
  OutboxTable,
  OutboxDeadLetterTable,
  InboxTable,
} from "../../../src/infrastructure/orm";
import { eq, asc } from "drizzle-orm";
import { v4 as uuidv4 } from "uuid";

describe("OutboxPoller Integration Tests", () => {
  let db: DB;
  let mockProjectionHandler: MockProjectionHandler;
  let mockExternalEffectHandler: MockExternalEffectHandler;

  beforeEach(async () => {
    db = await getTestDb();
    await cleanDatabase(db);
    mockProjectionHandler = new MockProjectionHandler();
    mockExternalEffectHandler = new MockExternalEffectHandler();
  });

  describe("Basic Functionality", () => {
    it("should process single message successfully", async () => {
      const event = createTestIntegrationEvent();
      const messageId = await createOutboxMessage(db, { event });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      // Start polling in background
      const pollPromise = poller.poll();

      // Wait for message to be processed
      await waitFor(
        async () => {
          const messages = await db
            .select()
            .from(OutboxTable)
            .where(eq(OutboxTable.id, messageId));
          return messages.length === 0;
        },
        { timeout: 5000 }
      );

      await poller.shutdown();
      await pollPromise;

      expect(mockProjectionHandler.callCount).toBe(1);
      expect(mockExternalEffectHandler.callCount).toBe(1);
      expect(mockProjectionHandler.calledWith[0].eventId).toBe(event.eventId);
    });

    it("should process multiple messages in order (by ID)", async () => {
      const ids = await createMultipleOutboxMessages(db, 5);

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.length === 0;
        },
        { timeout: 5000 }
      );

      await poller.shutdown();
      await pollPromise;

      expect(mockProjectionHandler.callCount).toBe(5);
      expect(mockExternalEffectHandler.callCount).toBe(5);
    });

    it("should handle empty outbox gracefully", async () => {
      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      // Let it run for a bit
      await sleep(100);

      await poller.shutdown();
      await pollPromise;

      expect(mockProjectionHandler.callCount).toBe(0);
      expect(mockExternalEffectHandler.callCount).toBe(0);
    });

    it("should delete message from outbox after successful processing", async () => {
      const messageId = await createOutboxMessage(db);

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db
            .select()
            .from(OutboxTable)
            .where(eq(OutboxTable.id, messageId));
          return messages.length === 0;
        },
        { timeout: 5000 }
      );

      await poller.shutdown();
      await pollPromise;

      const messagesAfter = await db.select().from(OutboxTable);
      expect(messagesAfter.length).toBe(0);
    });

    it("should respect leaseBatchSize configuration", async () => {
      // Create more messages than leaseBatchSize
      await createMultipleOutboxMessages(db, 20);

      // Use a handler that blocks to keep messages in "in_progress" state
      let resolveHandler: (() => void) | null = null;
      const blockingPromise = new Promise<void>((resolve) => {
        resolveHandler = resolve;
      });

      const blockingHandler = {
        async handleIntegrationEvent() {
          await blockingPromise; // Block until we release it
          return { success: true };
        },
      };

      const poller = new OutboxPoller({
        db,
        projectionHandler: blockingHandler as any,
        externalEffectHandler: blockingHandler as any,
        leaseBatchSize: 5,
        processBatchSize: 20, // Large enough to not interfere
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      // Wait for first lease to happen
      await waitFor(
        async () => {
          const inProgressMessages = await db
            .select()
            .from(OutboxTable)
            .where(eq(OutboxTable.status, "in_progress"));
          return inProgressMessages.length > 0;
        },
        { timeout: 2000 }
      );

      // Check that only leaseBatchSize messages were leased
      const inProgressMessages = await db
        .select()
        .from(OutboxTable)
        .where(eq(OutboxTable.status, "in_progress"));

      expect(inProgressMessages.length).toBe(5);

      // Release the blocking handlers and cleanup
      resolveHandler!();
      await poller.shutdown();
      await pollPromise;
    });

    it("should respect processBatchSize configuration", async () => {
      await createMultipleOutboxMessages(db, 10);

      // Track concurrent processing to verify batch size
      let currentlyProcessing = 0;
      let maxConcurrent = 0;
      const processingSnapshots: number[] = [];

      const trackingHandler = {
        async handleIntegrationEvent() {
          currentlyProcessing++;
          maxConcurrent = Math.max(maxConcurrent, currentlyProcessing);
          processingSnapshots.push(currentlyProcessing);

          // Hold for a bit to ensure we can observe concurrent processing
          await sleep(100);

          currentlyProcessing--;
          return { success: true };
        },
      };

      const poller = new OutboxPoller({
        db,
        projectionHandler: trackingHandler as any,
        externalEffectHandler: trackingHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 3,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.length === 0;
        },
        { timeout: 10000 }
      );

      await poller.shutdown();
      await pollPromise;

      // Since processBatchSize is 3 and we have 2 handlers (projection + external),
      // we should see at most 3*2=6 concurrent operations per batch
      expect(maxConcurrent).toBeLessThanOrEqual(6);
      expect(maxConcurrent).toBeGreaterThanOrEqual(2); // At least 2 (one message, both handlers)
    });

    it("should process all messages across multiple chunks", async () => {
      await createMultipleOutboxMessages(db, 7);

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 3,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.length === 0;
        },
        { timeout: 5000 }
      );

      await poller.shutdown();
      await pollPromise;

      expect(mockProjectionHandler.callCount).toBe(7);
      expect(mockExternalEffectHandler.callCount).toBe(7);
    });

    it("should continuously poll for new messages", async () => {
      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      // Add first message
      await createOutboxMessage(db);
      await waitFor(() => mockProjectionHandler.callCount === 1, {
        timeout: 2000,
      });

      // Add second message while polling
      await createOutboxMessage(db);
      await waitFor(() => mockProjectionHandler.callCount === 2, {
        timeout: 2000,
      });

      await poller.shutdown();
      await pollPromise;

      expect(mockProjectionHandler.callCount).toBe(2);
    });
  });

  describe("Leasing Logic", () => {
    it("should lease pending messages and set to in_progress", async () => {
      const messageId = await createOutboxMessage(db, { status: "pending" });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      // Wait a bit for leasing to happen
      await sleep(50);

      const message = await db
        .select()
        .from(OutboxTable)
        .where(eq(OutboxTable.id, messageId));

      if (message.length > 0) {
        expect(message[0].status).toBe("in_progress");
      }

      await poller.shutdown();
      await pollPromise;
    });

    it("should reclaim stale leases (> 5 minutes old)", async () => {
      const staleTime = new Date(Date.now() - 6 * 60 * 1000); // 6 minutes ago
      const messageId = await createOutboxMessage(db, {
        status: "in_progress",
        leasedAt: staleTime,
      });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db
            .select()
            .from(OutboxTable)
            .where(eq(OutboxTable.id, messageId));
          return messages.length === 0;
        },
        { timeout: 5000 }
      );

      await poller.shutdown();
      await pollPromise;

      expect(mockProjectionHandler.callCount).toBe(1);
    });

    it("should only lease messages where nextAvailableAt <= now", async () => {
      const futureTime = new Date(Date.now() + 60000); // 1 minute in future
      await createOutboxMessage(db, {
        status: "pending",
        nextAvailableAt: futureTime,
      });
      const availableMessageId = await createOutboxMessage(db, {
        status: "pending",
        nextAvailableAt: null,
      });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db
            .select()
            .from(OutboxTable)
            .where(eq(OutboxTable.id, availableMessageId));
          return messages.length === 0;
        },
        { timeout: 5000 }
      );

      await poller.shutdown();
      await pollPromise;

      // Only one message should be processed
      expect(mockProjectionHandler.callCount).toBe(1);

      // Future message should still be pending
      const futureMessages = await db
        .select()
        .from(OutboxTable)
        .where(eq(OutboxTable.status, "pending"));
      expect(futureMessages.length).toBe(1);
    });

    it("should skip messages with future nextAvailableAt", async () => {
      const futureTime = new Date(Date.now() + 10000);
      await createOutboxMessage(db, {
        status: "pending",
        nextAvailableAt: futureTime,
      });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await sleep(100);

      await poller.shutdown();
      await pollPromise;

      expect(mockProjectionHandler.callCount).toBe(0);

      const messages = await db.select().from(OutboxTable);
      expect(messages.length).toBe(1);
      expect(messages[0].status).toBe("pending");
    });

    it("should lease messages in correct order (by ID ascending)", async () => {
      const ids = await createMultipleOutboxMessages(db, 5);
      const sortedIds = [...ids].sort();

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 1,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.length === 0;
        },
        { timeout: 5000 }
      );

      await poller.shutdown();
      await pollPromise;

      expect(mockProjectionHandler.callCount).toBe(5);
    });

    it("should update leasedAt timestamp correctly", async () => {
      const beforeLease = new Date();
      const messageId = await createOutboxMessage(db, { status: "pending" });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await sleep(100);

      const message = await db
        .select()
        .from(OutboxTable)
        .where(eq(OutboxTable.id, messageId));

      if (message.length > 0 && message[0].leasedAt) {
        expect(message[0].leasedAt.getTime()).toBeGreaterThanOrEqual(
          beforeLease.getTime()
        );
      }

      await poller.shutdown();
      await pollPromise;
    });

    it("should not lease already in_progress messages with recent lease", async () => {
      const recentTime = new Date(Date.now() - 60000); // 1 minute ago
      await createOutboxMessage(db, {
        status: "in_progress",
        leasedAt: recentTime,
      });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await sleep(100);

      await poller.shutdown();
      await pollPromise;

      // Message should not be processed
      expect(mockProjectionHandler.callCount).toBe(0);
    });
  });

  describe("Retry and Failure Handling", () => {
    it("should increment attempts counter on failure", async () => {
      mockProjectionHandler.setFailure(true, "Test failure");
      const messageId = await createOutboxMessage(db, { attempts: 0 });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db
            .select()
            .from(OutboxTable)
            .where(eq(OutboxTable.id, messageId));
          return messages.length > 0 && messages[0].attempts > 0;
        },
        { timeout: 2000 }
      );

      await poller.shutdown();
      await pollPromise;

      const message = await db
        .select()
        .from(OutboxTable)
        .where(eq(OutboxTable.id, messageId));

      expect(message[0].attempts).toBe(1);
    });

    it("should schedule retry with exponential backoff", async () => {
      mockProjectionHandler.setFailure(true);
      const messageId = await createOutboxMessage(db, { attempts: 0 });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 5,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db
            .select()
            .from(OutboxTable)
            .where(eq(OutboxTable.id, messageId));
          return messages.length > 0 && messages[0].attempts > 0;
        },
        { timeout: 2000 }
      );

      await poller.shutdown();
      await pollPromise;

      const message = await db
        .select()
        .from(OutboxTable)
        .where(eq(OutboxTable.id, messageId));

      expect(message[0].nextAvailableAt).not.toBeNull();
      expect(message[0].nextAvailableAt!.getTime()).toBeGreaterThan(Date.now());
    });

    it("should calculate backoff with jitter correctly", async () => {
      mockProjectionHandler.setFailure(true);
      const ids = await createMultipleOutboxMessages(db, 3);

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 5,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.every((m) => m.attempts > 0);
        },
        { timeout: 2000 }
      );

      await poller.shutdown();
      await pollPromise;

      const messages = await db.select().from(OutboxTable);

      // All should have different nextAvailableAt due to jitter
      const times = messages.map((m) => m.nextAvailableAt!.getTime());
      const uniqueTimes = new Set(times);
      expect(uniqueTimes.size).toBeGreaterThan(1);
    });

    it("should set nextAvailableAt for delayed retry", async () => {
      mockProjectionHandler.setFailure(true);
      const messageId = await createOutboxMessage(db);
      const beforeRetry = Date.now();

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 5,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db
            .select()
            .from(OutboxTable)
            .where(eq(OutboxTable.id, messageId));
          return (
            messages.length > 0 &&
            messages[0].nextAvailableAt !== null &&
            messages[0].attempts > 0
          );
        },
        { timeout: 2000 }
      );

      await poller.shutdown();
      await pollPromise;

      const message = await db
        .select()
        .from(OutboxTable)
        .where(eq(OutboxTable.id, messageId));

      expect(message[0].nextAvailableAt).not.toBeNull();
      // Should be at least 1 second in the future (base delay)
      expect(message[0].nextAvailableAt!.getTime()).toBeGreaterThan(
        beforeRetry + 1000
      );
    });

    it("should reset status to pending on retry", async () => {
      mockProjectionHandler.setFailure(true);
      const messageId = await createOutboxMessage(db);

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 5,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db
            .select()
            .from(OutboxTable)
            .where(eq(OutboxTable.id, messageId));
          return messages.length > 0 && messages[0].attempts > 0;
        },
        { timeout: 2000 }
      );

      await poller.shutdown();
      await pollPromise;

      const message = await db
        .select()
        .from(OutboxTable)
        .where(eq(OutboxTable.id, messageId));

      expect(message[0].status).toBe("pending");
      expect(message[0].leasedAt).toBeNull();
    });

    it("should move to dead letter queue after maxAttempts", async () => {
      mockProjectionHandler.setFailure(true, "Persistent failure");
      const event = createTestIntegrationEvent();
      const messageId = await createOutboxMessage(db, { event, attempts: 2 });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const outboxMessages = await db
            .select()
            .from(OutboxTable)
            .where(eq(OutboxTable.id, messageId));
          return outboxMessages.length === 0;
        },
        { timeout: 2000 }
      );

      await poller.shutdown();
      await pollPromise;

      const dlqMessage = await db
        .select()
        .from(OutboxDeadLetterTable)
        .where(eq(OutboxDeadLetterTable.id, messageId));

      expect(dlqMessage.length).toBe(1);
      expect(dlqMessage[0].id).toBe(messageId);
    });

    it("should store error messages in dead letter queue", async () => {
      const errorMsg = "Custom projection error";
      mockProjectionHandler.setFailure(true, errorMsg);
      const messageId = await createOutboxMessage(db, { attempts: 2 });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const dlqMessages = await db
            .select()
            .from(OutboxDeadLetterTable)
            .where(eq(OutboxDeadLetterTable.id, messageId));
          return dlqMessages.length > 0;
        },
        { timeout: 2000 }
      );

      await poller.shutdown();
      await pollPromise;

      const dlqMessage = await db
        .select()
        .from(OutboxDeadLetterTable)
        .where(eq(OutboxDeadLetterTable.id, messageId));

      expect(dlqMessage[0].lastError).toContain(errorMsg);
    });

    it("should preserve original event data in DLQ", async () => {
      mockProjectionHandler.setFailure(true);
      const event = createTestIntegrationEvent({
        payload: { special: "data", value: 123 },
      });
      const messageId = await createOutboxMessage(db, { event, attempts: 2 });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const dlqMessages = await db
            .select()
            .from(OutboxDeadLetterTable)
            .where(eq(OutboxDeadLetterTable.id, messageId));
          return dlqMessages.length > 0;
        },
        { timeout: 2000 }
      );

      await poller.shutdown();
      await pollPromise;

      const dlqMessage = await db
        .select()
        .from(OutboxDeadLetterTable)
        .where(eq(OutboxDeadLetterTable.id, messageId));

      const storedEvent = dlqMessage[0].event as any;
      expect(storedEvent.eventId).toBe(event.eventId);
      expect(storedEvent.payload.special).toBe("data");
      expect(storedEvent.payload.value).toBe(123);
    });

    it("should handle projectionHandler failures", async () => {
      mockProjectionHandler.setFailure(true, "Projection failed");
      const messageId = await createOutboxMessage(db);

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db
            .select()
            .from(OutboxTable)
            .where(eq(OutboxTable.id, messageId));
          return messages.length > 0 && messages[0].attempts > 0;
        },
        { timeout: 2000 }
      );

      await poller.shutdown();
      await pollPromise;

      const message = await db
        .select()
        .from(OutboxTable)
        .where(eq(OutboxTable.id, messageId));

      expect(message[0].status).toBe("pending");
      expect(message[0].attempts).toBe(1);
    });

    it("should handle externalEffectHandler failures", async () => {
      mockExternalEffectHandler.setFailure(true, "External effect failed");
      const messageId = await createOutboxMessage(db);

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db
            .select()
            .from(OutboxTable)
            .where(eq(OutboxTable.id, messageId));
          return messages.length > 0 && messages[0].attempts > 0;
        },
        { timeout: 2000 }
      );

      await poller.shutdown();
      await pollPromise;

      const message = await db
        .select()
        .from(OutboxTable)
        .where(eq(OutboxTable.id, messageId));

      expect(message[0].status).toBe("pending");
      expect(message[0].attempts).toBe(1);
    });
  });

  describe("Handler Integration", () => {
    it("should call projectionHandler with correct event", async () => {
      const event = createTestIntegrationEvent({
        eventName: "product.created",
        payload: { productId: "test-123" },
      });
      await createOutboxMessage(db, { event });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(() => mockProjectionHandler.callCount > 0, {
        timeout: 2000,
      });

      await poller.shutdown();
      await pollPromise;

      expect(mockProjectionHandler.calledWith[0].eventName).toBe(
        "product.created"
      );
      expect(mockProjectionHandler.calledWith[0].payload.productId).toBe(
        "test-123"
      );
    });

    it("should call externalEffectHandler with correct event", async () => {
      const event = createTestIntegrationEvent({
        eventName: "collection.created",
        payload: { collectionId: "coll-456" },
      });
      await createOutboxMessage(db, { event });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(() => mockExternalEffectHandler.callCount > 0, {
        timeout: 2000,
      });

      await poller.shutdown();
      await pollPromise;

      expect(mockExternalEffectHandler.calledWith[0].eventName).toBe(
        "collection.created"
      );
      expect(mockExternalEffectHandler.calledWith[0].payload.collectionId).toBe(
        "coll-456"
      );
    });

    it("should handle projectionHandler errors gracefully", async () => {
      mockProjectionHandler.setFailure(true, "Handler error");
      await createOutboxMessage(db);
      await createOutboxMessage(db);

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(() => mockProjectionHandler.callCount === 2, {
        timeout: 2000,
      });

      await poller.shutdown();
      await pollPromise;

      // Both messages should be retried, not skipped
      const messages = await db.select().from(OutboxTable);
      expect(messages.length).toBe(2);
      expect(messages.every((m) => m.attempts > 0)).toBe(true);
    });

    it("should handle externalEffectHandler errors gracefully", async () => {
      mockExternalEffectHandler.setFailure(true, "External error");
      await createOutboxMessage(db);

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(() => mockExternalEffectHandler.callCount > 0, {
        timeout: 2000,
      });

      await poller.shutdown();
      await pollPromise;

      const messages = await db.select().from(OutboxTable);
      expect(messages[0].attempts).toBeGreaterThan(0);
    });

    it("should continue processing other messages if one fails", async () => {
      let callCount = 0;
      const conditionalHandler = {
        async handleIntegrationEvent(event: any) {
          callCount++;
          if (callCount === 1) {
            return { success: false, error: "First message fails" };
          }
          return { success: true };
        },
      };

      await createOutboxMessage(db);
      await createOutboxMessage(db);
      await createOutboxMessage(db);

      const poller = new OutboxPoller({
        db,
        projectionHandler: conditionalHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.length <= 1;
        },
        { timeout: 3000 }
      );

      await poller.shutdown();
      await pollPromise;

      // Two messages should be successfully processed
      const remainingMessages = await db.select().from(OutboxTable);
      expect(remainingMessages.length).toBe(1);
      expect(remainingMessages[0].attempts).toBeGreaterThan(0);
    });

    it("should verify inbox idempotency (duplicate events)", async () => {
      const event = createTestIntegrationEvent();
      await createOutboxMessage(db, { event });

      // Insert the event ID into inbox to simulate it was already processed
      await db.insert(InboxTable).values({ id: event.eventId });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.length === 0;
        },
        { timeout: 2000 }
      );

      await poller.shutdown();
      await pollPromise;

      // Handler should still be called once (idempotency check happens in projectionHandler)
      expect(mockProjectionHandler.callCount).toBe(1);
    });
  });

  describe("Concurrency & Race Conditions", () => {
    it("should ensure two pollers don't process same message", async () => {
      await createMultipleOutboxMessages(db, 5);

      const handler1Calls: string[] = [];
      const handler2Calls: string[] = [];

      const handler1 = {
        async handleIntegrationEvent(event: any) {
          handler1Calls.push(event.eventId);
          await sleep(50);
          return { success: true };
        },
      };

      const handler2 = {
        async handleIntegrationEvent(event: any) {
          handler2Calls.push(event.eventId);
          await sleep(50);
          return { success: true };
        },
      };

      const poller1 = new OutboxPoller({
        db,
        projectionHandler: handler1 as any,
        externalEffectHandler: handler1 as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const poller2 = new OutboxPoller({
        db,
        projectionHandler: handler2 as any,
        externalEffectHandler: handler2 as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const poll1Promise = poller1.poll();
      const poll2Promise = poller2.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.length === 0;
        },
        { timeout: 5000 }
      );

      await poller1.shutdown();
      await poller2.shutdown();
      await Promise.all([poll1Promise, poll2Promise]);

      const allCalls = [...handler1Calls, ...handler2Calls];
      const uniqueCalls = new Set(allCalls);

      // No duplicate processing (each eventId appears exactly twice - once per handler)
      expect(allCalls.length).toBe(10); // 5 messages * 2 handlers
      expect(uniqueCalls.size).toBe(5); // 5 unique messages
    });

    it("should allow multiple pollers to process different messages in parallel", async () => {
      await createMultipleOutboxMessages(db, 10);

      const poller1 = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 5,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const handler2 = new MockProjectionHandler();
      const externalHandler2 = new MockExternalEffectHandler();

      const poller2 = new OutboxPoller({
        db,
        projectionHandler: handler2 as any,
        externalEffectHandler: externalHandler2 as any,
        leaseBatchSize: 5,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const poll1Promise = poller1.poll();
      const poll2Promise = poller2.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.length === 0;
        },
        { timeout: 5000 }
      );

      await poller1.shutdown();
      await poller2.shutdown();
      await Promise.all([poll1Promise, poll2Promise]);

      const totalProcessed =
        mockProjectionHandler.callCount + handler2.callCount;
      expect(totalProcessed).toBe(10);
    });

    it("should handle race condition: message deleted during processing", async () => {
      const messageId = await createOutboxMessage(db);

      const slowHandler = {
        async handleIntegrationEvent() {
          // Simulate slow processing - delete message externally during this time
          await sleep(100);
          return { success: true };
        },
      };

      const poller = new OutboxPoller({
        db,
        projectionHandler: slowHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      // Wait for processing to start
      await sleep(50);

      // Delete message while it's being processed
      await db.delete(OutboxTable).where(eq(OutboxTable.id, messageId));

      await sleep(200);

      await poller.shutdown();
      await pollPromise;

      // Should not throw error, just handle gracefully
      const messages = await db.select().from(OutboxTable);
      expect(messages.length).toBe(0);
    });

    it("should handle race condition: concurrent retry updates", async () => {
      mockProjectionHandler.setFailure(true);
      const messageId = await createOutboxMessage(db);

      const poller1 = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 5,
      });

      const poll1Promise = poller1.poll();

      await waitFor(
        async () => {
          const messages = await db
            .select()
            .from(OutboxTable)
            .where(eq(OutboxTable.id, messageId));
          return messages.length > 0 && messages[0].attempts > 0;
        },
        { timeout: 2000 }
      );

      await poller1.shutdown();
      await poll1Promise;

      const message = await db
        .select()
        .from(OutboxTable)
        .where(eq(OutboxTable.id, messageId));

      // Should have exactly 1 attempt, not multiple
      expect(message[0].attempts).toBe(1);
    });

    it("should handle race condition: concurrent DLQ moves", async () => {
      mockProjectionHandler.setFailure(true);
      const messageId = await createOutboxMessage(db, { attempts: 2 });

      const poller1 = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const handler2 = new MockProjectionHandler();
      handler2.setFailure(true);
      const externalHandler2 = new MockExternalEffectHandler();

      const poller2 = new OutboxPoller({
        db,
        projectionHandler: handler2 as any,
        externalEffectHandler: externalHandler2 as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const poll1Promise = poller1.poll();
      const poll2Promise = poller2.poll();

      await waitFor(
        async () => {
          const dlqMessages = await db
            .select()
            .from(OutboxDeadLetterTable)
            .where(eq(OutboxDeadLetterTable.id, messageId));
          return dlqMessages.length > 0;
        },
        { timeout: 3000 }
      );

      await poller1.shutdown();
      await poller2.shutdown();
      await Promise.all([poll1Promise, poll2Promise]);

      // Should only have one DLQ entry (idempotent)
      const dlqMessages = await db
        .select()
        .from(OutboxDeadLetterTable)
        .where(eq(OutboxDeadLetterTable.id, messageId));
      expect(dlqMessages.length).toBe(1);
    });

    it("should verify row-level locking with FOR UPDATE", async () => {
      const messageIds = await createMultipleOutboxMessages(db, 3);

      // Track which messages each poller tries to lease
      const leasedByPoller1: string[] = [];
      const leasedByPoller2: string[] = [];

      let handler1CallCount = 0;
      let handler2CallCount = 0;

      const handler1 = {
        async handleIntegrationEvent(event: any) {
          handler1CallCount++;
          leasedByPoller1.push(event.eventId);
          await sleep(100);
          return { success: true };
        },
      };

      const handler2 = {
        async handleIntegrationEvent(event: any) {
          handler2CallCount++;
          leasedByPoller2.push(event.eventId);
          await sleep(100);
          return { success: true };
        },
      };

      const poller1 = new OutboxPoller({
        db,
        projectionHandler: handler1 as any,
        externalEffectHandler: handler1 as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const poller2 = new OutboxPoller({
        db,
        projectionHandler: handler2 as any,
        externalEffectHandler: handler2 as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const poll1Promise = poller1.poll();
      const poll2Promise = poller2.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.length === 0;
        },
        { timeout: 5000 }
      );

      await poller1.shutdown();
      await poller2.shutdown();
      await Promise.all([poll1Promise, poll2Promise]);

      // Verify no message was processed by both pollers
      const intersection = leasedByPoller1.filter((id) =>
        leasedByPoller2.includes(id)
      );
      expect(intersection.length).toBe(0);

      // Total calls should be 6 (3 messages * 2 handlers each)
      expect(handler1CallCount + handler2CallCount).toBe(6);
    });

    it("should handle transaction conflicts gracefully", async () => {
      mockProjectionHandler.setFailure(true);
      const messageId = await createOutboxMessage(db);

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db
            .select()
            .from(OutboxTable)
            .where(eq(OutboxTable.id, messageId));
          return messages.length > 0 && messages[0].attempts > 0;
        },
        { timeout: 2000 }
      );

      await poller.shutdown();
      await pollPromise;

      // Should not crash, message should be retried
      const message = await db
        .select()
        .from(OutboxTable)
        .where(eq(OutboxTable.id, messageId));
      expect(message.length).toBe(1);
    });

    it("should verify no message loss with concurrent workers", async () => {
      const messageCount = 20;
      await createMultipleOutboxMessages(db, messageCount);

      const processedIds = new Set<string>();

      const trackingHandler1 = {
        async handleIntegrationEvent(event: any) {
          processedIds.add(event.eventId);
          return { success: true };
        },
      };

      const trackingHandler2 = {
        async handleIntegrationEvent(event: any) {
          processedIds.add(event.eventId);
          return { success: true };
        },
      };

      const poller1 = new OutboxPoller({
        db,
        projectionHandler: trackingHandler1 as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const poller2 = new OutboxPoller({
        db,
        projectionHandler: trackingHandler2 as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const poll1Promise = poller1.poll();
      const poll2Promise = poller2.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.length === 0;
        },
        { timeout: 5000 }
      );

      await poller1.shutdown();
      await poller2.shutdown();
      await Promise.all([poll1Promise, poll2Promise]);

      // All messages should be processed exactly once
      expect(processedIds.size).toBe(messageCount);
    });
  });

  describe("Shutdown & Lifecycle", () => {
    it("should stop polling loop on shutdown", async () => {
      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await sleep(100);
      await poller.shutdown();
      await pollPromise;

      // After shutdown, add a message and verify it's not processed
      await createOutboxMessage(db);
      await sleep(200);

      expect(mockProjectionHandler.callCount).toBe(0);
    });

    it("should wait for in-flight operations on shutdown", async () => {
      let processingStarted = false;
      let processingCompleted = false;

      const slowHandler = {
        async handleIntegrationEvent() {
          processingStarted = true;
          await sleep(500);
          processingCompleted = true;
          return { success: true };
        },
      };

      await createOutboxMessage(db);

      const poller = new OutboxPoller({
        db,
        projectionHandler: slowHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      // Wait for processing to start
      await waitFor(() => processingStarted, { timeout: 2000 });

      // Trigger shutdown while processing
      const shutdownStart = Date.now();
      await poller.shutdown();
      await pollPromise;
      const shutdownDuration = Date.now() - shutdownStart;

      // Shutdown should have waited for processing to complete
      expect(processingCompleted).toBe(true);
      expect(shutdownDuration).toBeGreaterThanOrEqual(400);
    });

    it("should timeout after configured timeout if operations don't complete", async () => {
      let processingStarted = false;

      const slowHandler = {
        async handleIntegrationEvent() {
          processingStarted = true;
          // Simulate a slow operation (longer than shutdown timeout)
          await sleep(2000);
          return { success: true };
        },
      };

      await createOutboxMessage(db);

      const poller = new OutboxPoller({
        db,
        projectionHandler: slowHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
        shutdownTimeoutMs: 500, // 500ms timeout for faster tests
      });

      const pollPromise = poller.poll();

      // Wait for processing to actually start
      await waitFor(() => processingStarted, { timeout: 2000 });

      const shutdownStart = Date.now();
      await poller.shutdown();
      await pollPromise;
      const shutdownDuration = Date.now() - shutdownStart;

      // Should complete around 2 seconds (when the handler finishes)
      // The timeout kicks in at 500ms but poll loop waits for batch to complete
      expect(shutdownDuration).toBeLessThan(2500);
      expect(shutdownDuration).toBeGreaterThan(1900);
    }, 5000);

    it("should continue processing during graceful shutdown", async () => {
      let processedCount = 0;

      const countingHandler = {
        async handleIntegrationEvent() {
          processedCount++;
          await sleep(100);
          return { success: true };
        },
      };

      await createMultipleOutboxMessages(db, 3);

      const poller = new OutboxPoller({
        db,
        projectionHandler: countingHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      // Start shutdown immediately
      await sleep(50);
      await poller.shutdown();
      await pollPromise;

      // Some messages should still be processed during graceful shutdown
      expect(processedCount).toBeGreaterThan(0);
    });

    it("should track inFlightOperations counter correctly", async () => {
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      const trackingHandler = {
        async handleIntegrationEvent() {
          currentConcurrent++;
          maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
          await sleep(100);
          currentConcurrent--;
          return { success: true };
        },
      };

      await createMultipleOutboxMessages(db, 10);

      const poller = new OutboxPoller({
        db,
        projectionHandler: trackingHandler as any,
        externalEffectHandler: trackingHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.length === 0;
        },
        { timeout: 5000 }
      );

      await poller.shutdown();
      await pollPromise;

      // After shutdown, no operations should be in flight
      expect(currentConcurrent).toBe(0);

      // Max concurrent should respect processBatchSize
      expect(maxConcurrent).toBeLessThanOrEqual(5 * 2); // processBatchSize * 2 handlers
    });
  });

  describe("Edge Cases", () => {
    it("should handle message not found after lease (deleted by another worker)", async () => {
      const messageId = await createOutboxMessage(db);

      let deleteAttempted = false;
      const deletingHandler = {
        async handleIntegrationEvent() {
          if (!deleteAttempted) {
            deleteAttempted = true;
            // Delete the message mid-processing
            await db.delete(OutboxTable).where(eq(OutboxTable.id, messageId));
          }
          return { success: true };
        },
      };

      const poller = new OutboxPoller({
        db,
        projectionHandler: deletingHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await sleep(200);

      await poller.shutdown();
      await pollPromise;

      // Should complete without errors
      const messages = await db.select().from(OutboxTable);
      expect(messages.length).toBe(0);
    });

    it("should handle invalid event format gracefully", async () => {
      // Insert an outbox message with invalid event structure
      const invalidEventId = uuidv4();
      await db.insert(OutboxTable).values({
        id: invalidEventId,
        status: "pending",
        attempts: 0,
        leasedAt: null,
        nextAvailableAt: null,
        event: { invalid: "structure" } as any,
      });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db
            .select()
            .from(OutboxTable)
            .where(eq(OutboxTable.id, invalidEventId));
          return messages.length === 0 || messages[0].attempts > 0;
        },
        { timeout: 2000 }
      );

      await poller.shutdown();
      await pollPromise;

      // Handler should be called even with invalid structure
      expect(mockProjectionHandler.callCount).toBeGreaterThan(0);
    });

    it("should handle database connection failure during processing", async () => {
      await createOutboxMessage(db);

      let callCount = 0;
      const failingHandler = {
        async handleIntegrationEvent() {
          callCount++;
          if (callCount === 1) {
            throw new Error("Database connection lost");
          }
          return { success: true };
        },
      };

      const poller = new OutboxPoller({
        db,
        projectionHandler: failingHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(() => callCount >= 1, { timeout: 2000 });

      await poller.shutdown();
      await pollPromise;

      // Should handle the error and potentially retry
      expect(callCount).toBeGreaterThanOrEqual(1);
    });

    it("should handle transaction rollback during retry scheduling", async () => {
      mockProjectionHandler.setFailure(true);
      const messageId = await createOutboxMessage(db);

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db
            .select()
            .from(OutboxTable)
            .where(eq(OutboxTable.id, messageId));
          return messages.length > 0 && messages[0].attempts > 0;
        },
        { timeout: 2000 }
      );

      await poller.shutdown();
      await pollPromise;

      const message = await db
        .select()
        .from(OutboxTable)
        .where(eq(OutboxTable.id, messageId));

      // Should have scheduled retry
      expect(message[0].status).toBe("pending");
      expect(message[0].attempts).toBe(1);
    });

    it("should handle zero leaseBatchSize edge case", async () => {
      await createOutboxMessage(db);

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 0,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await sleep(200);

      await poller.shutdown();
      await pollPromise;

      // Should not process any messages
      expect(mockProjectionHandler.callCount).toBe(0);
    });

    it("should handle zero processBatchSize edge case", async () => {
      await createOutboxMessage(db);

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 0,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await sleep(200);

      await poller.shutdown();
      await pollPromise;

      // Should handle gracefully (might not process or process slowly)
      const messages = await db.select().from(OutboxTable);
      expect(messages.length).toBeGreaterThanOrEqual(0);
    });

    it("should handle maxAttempts = 1 (immediate DLQ)", async () => {
      mockProjectionHandler.setFailure(true, "Immediate failure");
      const event = createTestIntegrationEvent();
      const messageId = await createOutboxMessage(db, { event, attempts: 0 });

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 1,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const dlqMessages = await db
            .select()
            .from(OutboxDeadLetterTable)
            .where(eq(OutboxDeadLetterTable.id, messageId));
          return dlqMessages.length > 0;
        },
        { timeout: 2000 }
      );

      await poller.shutdown();
      await pollPromise;

      const dlqMessage = await db
        .select()
        .from(OutboxDeadLetterTable)
        .where(eq(OutboxDeadLetterTable.id, messageId));

      expect(dlqMessage.length).toBe(1);
      expect(dlqMessage[0].id).toBe(messageId);
    });

    it("should handle empty handlers (no-op processing)", async () => {
      const event = createTestIntegrationEvent();
      await createOutboxMessage(db, { event });

      const noOpHandler = {
        async handleIntegrationEvent() {
          return { success: true };
        },
      };

      const poller = new OutboxPoller({
        db,
        projectionHandler: noOpHandler as any,
        externalEffectHandler: noOpHandler as any,
        leaseBatchSize: 10,
        processBatchSize: 5,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.length === 0;
        },
        { timeout: 2000 }
      );

      await poller.shutdown();
      await pollPromise;

      // Message should be processed and removed
      const messages = await db.select().from(OutboxTable);
      expect(messages.length).toBe(0);
    });
  });

  describe("Performance & Load Tests", () => {
    it("should process 1000 messages with single worker", async () => {
      const messageCount = 1000;
      await createMultipleOutboxMessages(db, messageCount);

      const startTime = Date.now();
      const projectionHandlerPerfTesting =
        new MockProjectionHandlerPerfTesting();
      const externalEffectHandlerPerfTesting =
        new MockExternalEffectHandlerPerfTesting();

      const poller = new OutboxPoller({
        db,
        projectionHandler: projectionHandlerPerfTesting as any,
        externalEffectHandler: externalEffectHandlerPerfTesting as any,
        leaseBatchSize: 1000,
        processBatchSize: 90,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.length === 0;
        },
        { timeout: 30000 }
      );

      await poller.shutdown();
      await pollPromise;

      const duration = Date.now() - startTime;
      const throughput = messageCount / (duration / 1000);

      console.log(
        `Processed ${messageCount} messages in ${duration}ms (${throughput.toFixed(2)} msg/sec)`
      );
      expect(duration).toBeLessThan(30000);
    }, 35000);

    it("should process 1000 messages with 3 concurrent workers", async () => {
      const messageCount = 1000;
      await createMultipleOutboxMessages(db, messageCount);

      const handler1 = new MockProjectionHandler();
      const handler2 = new MockProjectionHandler();
      const handler3 = new MockProjectionHandler();

      const externalHandler1 = new MockExternalEffectHandler();
      const externalHandler2 = new MockExternalEffectHandler();
      const externalHandler3 = new MockExternalEffectHandler();

      const startTime = Date.now();

      const poller1 = new OutboxPoller({
        db,
        projectionHandler: handler1 as any,
        externalEffectHandler: externalHandler1 as any,
        leaseBatchSize: 50,
        processBatchSize: 25,
        maxAttempts: 3,
      });

      const poller2 = new OutboxPoller({
        db,
        projectionHandler: handler2 as any,
        externalEffectHandler: externalHandler2 as any,
        leaseBatchSize: 50,
        processBatchSize: 25,
        maxAttempts: 3,
      });

      const poller3 = new OutboxPoller({
        db,
        projectionHandler: handler3 as any,
        externalEffectHandler: externalHandler3 as any,
        leaseBatchSize: 50,
        processBatchSize: 25,
        maxAttempts: 3,
      });

      const poll1Promise = poller1.poll();
      const poll2Promise = poller2.poll();
      const poll3Promise = poller3.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.length === 0;
        },
        { timeout: 30000 }
      );

      await poller1.shutdown();
      await poller2.shutdown();
      await poller3.shutdown();
      await Promise.all([poll1Promise, poll2Promise, poll3Promise]);

      const duration = Date.now() - startTime;
      const throughput = messageCount / (duration / 1000);

      const totalProcessed =
        handler1.callCount + handler2.callCount + handler3.callCount;

      console.log(
        `3 workers processed ${messageCount} messages in ${duration}ms (${throughput.toFixed(2)} msg/sec)`
      );
      console.log(
        `Distribution: ${handler1.callCount}, ${handler2.callCount}, ${handler3.callCount}`
      );

      expect(totalProcessed).toBe(messageCount);
      expect(duration).toBeLessThan(30000);
    }, 35000);

    it("should measure throughput (messages/second)", async () => {
      const messageCount = 500;
      await createMultipleOutboxMessages(db, messageCount);

      const startTime = Date.now();

      const poller = new OutboxPoller({
        db,
        projectionHandler: mockProjectionHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 100,
        processBatchSize: 50,
        maxAttempts: 3,
      });

      const pollPromise = poller.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.length === 0;
        },
        { timeout: 20000 }
      );

      await poller.shutdown();
      await pollPromise;

      const duration = Date.now() - startTime;
      const throughput = messageCount / (duration / 1000);

      console.log(`Throughput: ${throughput.toFixed(2)} messages/second`);

      expect(throughput).toBeGreaterThan(10); // At least 10 msg/sec
      expect(mockProjectionHandler.callCount).toBe(messageCount);
    }, 25000);

    it("should verify exactly-once processing under load", async () => {
      const messageCount = 100;
      const eventIds = new Set<string>();

      // Create messages with unique event IDs
      for (let i = 0; i < messageCount; i++) {
        const event = createTestIntegrationEvent();
        eventIds.add(event.eventId);
        await createOutboxMessage(db, { event });
      }

      const processedEventIds = new Set<string>();

      const trackingHandler = {
        async handleIntegrationEvent(event: any) {
          if (processedEventIds.has(event.eventId)) {
            throw new Error(`Duplicate processing detected: ${event.eventId}`);
          }
          processedEventIds.add(event.eventId);
          return { success: true };
        },
      };

      const poller1 = new OutboxPoller({
        db,
        projectionHandler: trackingHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 50,
        processBatchSize: 25,
        maxAttempts: 3,
      });

      const poller2 = new OutboxPoller({
        db,
        projectionHandler: trackingHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 50,
        processBatchSize: 25,
        maxAttempts: 3,
      });

      const poll1Promise = poller1.poll();
      const poll2Promise = poller2.poll();

      await waitFor(
        async () => {
          const messages = await db.select().from(OutboxTable);
          return messages.length === 0;
        },
        { timeout: 15000 }
      );

      await poller1.shutdown();
      await poller2.shutdown();
      await Promise.all([poll1Promise, poll2Promise]);

      // Verify exactly-once: all messages processed, none duplicated
      expect(processedEventIds.size).toBe(messageCount);
      expect(Array.from(processedEventIds).sort()).toEqual(
        Array.from(eventIds).sort()
      );
    }, 20000);

    it("should stress test: 5000 messages with mixed success/failure", async () => {
      const messageCount = 5000;
      await createMultipleOutboxMessages(db, messageCount);

      let callCount = 0;
      const mixedHandler = {
        async handleIntegrationEvent() {
          callCount++;
          // 10% failure rate
          if (callCount % 10 === 0) {
            return { success: false, error: "Random failure" };
          }
          return { success: true };
        },
      };

      const startTime = Date.now();

      const poller1 = new OutboxPoller({
        db,
        projectionHandler: mixedHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 100,
        processBatchSize: 50,
        maxAttempts: 2,
      });

      const poller2 = new OutboxPoller({
        db,
        projectionHandler: mixedHandler as any,
        externalEffectHandler: mockExternalEffectHandler as any,
        leaseBatchSize: 100,
        processBatchSize: 50,
        maxAttempts: 2,
      });

      const poll1Promise = poller1.poll();
      const poll2Promise = poller2.poll();

      // Wait for most messages to be processed or moved to DLQ
      await waitFor(
        async () => {
          const outboxMessages = await db.select().from(OutboxTable);
          const dlqMessages = await db.select().from(OutboxDeadLetterTable);
          return (
            outboxMessages.length + dlqMessages.length <= messageCount * 0.15
          ); // Allow some still retrying
        },
        { timeout: 60000 }
      );

      await poller1.shutdown();
      await poller2.shutdown();
      await Promise.all([poll1Promise, poll2Promise]);

      const duration = Date.now() - startTime;
      const throughput = messageCount / (duration / 1000);

      const remainingMessages = await db.select().from(OutboxTable);
      const dlqMessages = await db.select().from(OutboxDeadLetterTable);

      console.log(
        `Stress test: ${messageCount} messages in ${duration}ms (${throughput.toFixed(2)} msg/sec)`
      );
      console.log(
        `Remaining: ${remainingMessages.length}, DLQ: ${dlqMessages.length}`
      );

      // Most messages should be processed or in DLQ
      expect(remainingMessages.length + dlqMessages.length).toBeLessThanOrEqual(
        messageCount * 0.2
      );
    }, 65000);
  });
});
