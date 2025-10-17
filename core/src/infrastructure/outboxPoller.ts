import type { IntegrationEvent } from "../integrationEvents/_base";
import type { DB, TX } from "./postgres";
import { OutboxTable, OutboxDeadLetterTable } from "./orm";
import { eq, asc, or, and, lt, inArray, isNull } from "drizzle-orm";
import { ProjectionHandler } from "../views/projections/projectionHandler";
import { ExternalEffectHandler } from "./externalEffectHandler";

type TransactionalClient = Pick<TX, "insert" | "select" | "update" | "delete">;

type OutboxPollerProps = {
  db: DB;
  projectionHandler: ProjectionHandler;
  externalEffectHandler: ExternalEffectHandler;
  leaseBatchSize: number;
  processBatchSize: number;
  maxAttempts: number;
  shutdownTimeoutMs?: number;
};

class OutboxPoller {
  private leaseBatchSize: number;
  private processBatchSize: number;
  private db: DB;
  private projectionHandler: ProjectionHandler;
  private externalEffectHandler: ExternalEffectHandler;
  private maxAttempts: number;
  private isShuttingDown: boolean = false;
  private inFlightOperations: number = 0;
  private pollIntervalMs: number;
  private shutdownTimeoutMs: number;

  constructor({
    db,
    projectionHandler,
    externalEffectHandler,
    leaseBatchSize,
    processBatchSize,
    maxAttempts = 6,
    shutdownTimeoutMs = 30000,
  }: OutboxPollerProps) {
    this.db = db;
    this.projectionHandler = projectionHandler;
    this.leaseBatchSize = leaseBatchSize;
    this.processBatchSize = processBatchSize;
    this.externalEffectHandler = externalEffectHandler;
    this.maxAttempts = maxAttempts;
    this.shutdownTimeoutMs = shutdownTimeoutMs;
    this.pollIntervalMs = 20; // 20ms max cycle time (processing + wait)
  }

  private async getPendingIdsWithLock(
    tx: TransactionalClient
  ): Promise<string[]> {
    const now = new Date();
    const staleLeaseCutoff = new Date(now.getTime() - 300000); // 5 minutes ago
    const results = await tx
      .select({ id: OutboxTable.id })
      .from(OutboxTable)
      .where(
        or(
          and(
            eq(OutboxTable.status, "pending"),
            or(
              isNull(OutboxTable.nextAvailableAt),
              lt(OutboxTable.nextAvailableAt, now)
            )
          ),
          and(
            eq(OutboxTable.status, "in_progress"),
            lt(OutboxTable.leasedAt, staleLeaseCutoff)
          )
        )
      )
      .orderBy(asc(OutboxTable.id))
      .for("update")
      .limit(this.leaseBatchSize)
      .execute();
    return results.map((r) => r.id);
  }

  private async leaseMessages(): Promise<string[]> {
    // Handle edge case: if leaseBatchSize is 0 or negative, don't lease anything
    if (this.leaseBatchSize <= 0) {
      return [];
    }

    return await this.db.transaction(async (tx) => {
      const pendingIds = await this.getPendingIdsWithLock(tx);

      if (pendingIds.length === 0) {
        return pendingIds;
      }

      await tx
        .update(OutboxTable)
        .set({ status: "in_progress", leasedAt: new Date() })
        .where(inArray(OutboxTable.id, pendingIds))
        .execute();
      return pendingIds;
    });
  }

  private createChunksToProcess(ids: string[]): string[][] {
    const chunks: string[][] = [];

    // Handle edge case: if processBatchSize is 0 or negative, don't process anything
    if (this.processBatchSize <= 0) {
      return chunks;
    }

    for (let i = 0; i < ids.length; i += this.processBatchSize) {
      chunks.push(ids.slice(i, i + this.processBatchSize));
    }
    return chunks;
  }

  private async processBatch() {
    const leasedIds = await this.leaseMessages();
    const chunksToProcess = this.createChunksToProcess(leasedIds);
    for (const chunk of chunksToProcess) {
      await Promise.all(
        chunk.map(async (id) => {
          this.inFlightOperations++;
          try {
            await this.processMessage(id);
          } catch (error) {
            console.error(`Error processing message ${id}: ${error}`);
          } finally {
            this.inFlightOperations--;
          }
        })
      );
    }
  }

  async poll() {
    console.log("OutboxPoller: Starting poll loop");
    while (!this.isShuttingDown) {
      const cycleStartTime = Date.now();

      try {
        await this.processBatch();
      } catch (error) {
        console.error(`OutboxPoller: Error in processBatch: ${error}`);
      }

      // Calculate remaining time to wait to reach target cycle time
      const elapsedMs = Date.now() - cycleStartTime;
      const remainingMs = Math.max(0, this.pollIntervalMs - elapsedMs);

      if (remainingMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, remainingMs));
      }
    }

    console.log(
      "OutboxPoller: Poll loop stopped, waiting for in-flight operations to complete"
    );
    await this.waitForInFlightOperations();
    console.log("OutboxPoller: All operations completed, shutdown complete");
  }

  private async waitForInFlightOperations(): Promise<void> {
    const checkIntervalMs = 100; // Check every 100ms
    const startTime = Date.now();

    while (this.inFlightOperations > 0) {
      if (Date.now() - startTime > this.shutdownTimeoutMs) {
        console.warn(
          `OutboxPoller: Shutdown timeout reached with ${this.inFlightOperations} operations still in-flight`
        );
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, checkIntervalMs));
    }
  }

  async shutdown(): Promise<void> {
    console.log("OutboxPoller: Shutdown requested");
    this.isShuttingDown = true;
  }

  private async fetchOutboxMessage(outboxMessageId: string) {
    const outboxMessageResult = await this.db
      .select()
      .from(OutboxTable)
      .where(eq(OutboxTable.id, outboxMessageId))
      .execute();
    if (outboxMessageResult.length === 0) {
      return null;
    }
    return outboxMessageResult[0];
  }

  private async handleIntegrationEvent(
    integrationEvent: IntegrationEvent<string, Record<string, unknown>>
  ): Promise<{ success: true } | { success: false; errors: string[] }> {
    const [projectionHandlerResult, externalEffectHandlerResult] =
      await Promise.all([
        this.projectionHandler.handleIntegrationEvent(integrationEvent),
        this.externalEffectHandler.handleIntegrationEvent(integrationEvent),
      ]);

    const hasErrors =
      projectionHandlerResult.success !== true ||
      externalEffectHandlerResult.success !== true;

    if (hasErrors) {
      const errors: string[] = [
        `ProjectionHandler Error: ${projectionHandlerResult.error ?? "None"}`,
        `ExternalEffectHandler Error: ${externalEffectHandlerResult.error ?? "None"}`,
      ];
      return { success: false, errors };
    }

    return { success: true };
  }

  private calculateBackoffDelay(attempts: number): Date {
    const baseDelayMs = 1000; // 1 second base
    const maxDelayMs = 300000; // 5 minutes max
    const exponentialDelay = Math.min(
      baseDelayMs * Math.pow(2, attempts),
      maxDelayMs
    );
    const jitterMs = Math.random() * 1000; // 0-1 second jitter
    const totalDelayMs = exponentialDelay + jitterMs;
    return new Date(Date.now() + totalDelayMs);
  }

  private async moveToDeadLetterQueue(
    tx: TransactionalClient,
    outboxMessageId: string,
    integrationEvent: IntegrationEvent<string, Record<string, unknown>>,
    errors: string[]
  ) {
    await tx
      .delete(OutboxTable)
      .where(eq(OutboxTable.id, outboxMessageId))
      .execute();

    await tx
      .insert(OutboxDeadLetterTable)
      .values({
        id: outboxMessageId,
        failedAt: new Date(),
        event: integrationEvent,
        lastError: errors.join(" | "),
      })
      .onConflictDoNothing() // Idempotent if another worker already moved it
      .execute();
  }

  private async scheduleRetry(
    tx: TransactionalClient,
    outboxMessageId: string,
    attempts: number
  ) {
    const nextAvailableAt = this.calculateBackoffDelay(attempts);

    await tx
      .update(OutboxTable)
      .set({
        status: "pending",
        attempts,
        nextAvailableAt,
        leasedAt: null,
      })
      .where(eq(OutboxTable.id, outboxMessageId))
      .execute();
  }

  private async handleMessageFailure(
    outboxMessageId: string,
    integrationEvent: IntegrationEvent<string, Record<string, unknown>>,
    errors: string[]
  ) {
    await this.db.transaction(async (tx) => {
      // Re-select the row with FOR UPDATE to prevent race conditions
      const [currentMessage] = await tx
        .select({
          attempts: OutboxTable.attempts,
        })
        .from(OutboxTable)
        .where(eq(OutboxTable.id, outboxMessageId))
        .for("update")
        .execute();

      if (!currentMessage) {
        // Message was already processed by another worker
        return;
      }

      const attempts = currentMessage.attempts + 1;

      if (attempts >= this.maxAttempts) {
        await this.moveToDeadLetterQueue(
          tx,
          outboxMessageId,
          integrationEvent,
          errors
        );
      } else {
        await this.scheduleRetry(tx, outboxMessageId, attempts);
      }
    });
  }

  private async markMessageAsProcessed(outboxMessageId: string) {
    await this.db
      .delete(OutboxTable)
      .where(eq(OutboxTable.id, outboxMessageId))
      .execute();
  }

  private async processMessage(outboxMessageId: string) {
    const outboxMessage = await this.fetchOutboxMessage(outboxMessageId);
    if (!outboxMessage) {
      return;
    }

    const integrationEvent = outboxMessage.event as IntegrationEvent<
      string,
      Record<string, unknown>
    >;

    const result = await this.handleIntegrationEvent(integrationEvent);

    if (!result.success) {
      await this.handleMessageFailure(
        outboxMessageId,
        integrationEvent,
        result.errors
      );
      return;
    }

    await this.markMessageAsProcessed(outboxMessageId);
  }
}

export { OutboxPoller };
