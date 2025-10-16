import { OutboxPoller } from "@core/infrastructure/outboxPoller";
import { db } from "@core/infrastructure/postgres";
import type { IntegrationEvent } from "@core/integrationEvents/_base";

// Example handler that processes integration events
async function handleIntegrationEvent(
  event: IntegrationEvent<string, Record<string, unknown>>
): Promise<void> {
  console.log(`Processing event: ${event.eventName}`, {
    eventId: event.eventId,
    correlationId: event.correlationId,
    occurredAt: event.occurredAt,
  });

  // TODO: Implement actual event processing logic here
  // This could include:
  // - Publishing to a message broker (RabbitMQ, Kafka, etc.)
  // - Calling external APIs
  // - Updating other systems
  // - Triggering webhooks

  // For now, just log the event
  console.log("Event payload:", event.payload);
}

// Create and start the outbox poller
const poller = new OutboxPoller(db, {
  batchSize: 1000,
  pollIntervalMs: 20, // Poll every 20ms for low latency
  errorHandler: async (error, message) => {
    console.error(
      `Failed to process outbox message ${message.id}:`,
      error.message
    );
    console.error("Event details:", {
      eventName: message.event.eventName,
      eventId: message.event.eventId,
      correlationId: message.event.correlationId,
    });
    // TODO: Implement error handling strategy
    // - Send to dead letter queue
    // - Alert monitoring system
    // - Retry with exponential backoff
  },
});

// Handle graceful shutdown
process.on("SIGINT", () => {
  console.log("Received SIGINT, shutting down gracefully...");
  poller.stop();
  process.exit(0);
});

process.on("SIGTERM", () => {
  console.log("Received SIGTERM, shutting down gracefully...");
  poller.stop();
  process.exit(0);
});

// Start the poller
poller.start(handleIntegrationEvent).catch((error) => {
  console.error("Fatal error in outbox poller:", error);
  process.exit(1);
});

console.log("Outbox worker started");
