import { OutboxPoller } from "../core/src/infrastructure/outboxPoller";
import { db } from "../core/src/infrastructure/postgres";
import { ProjectionHandler } from "../core/src/views/projections/projectionHandler";
import { ProductProjection } from "../core/src/views/projections/productProjection";
import { ProductVariantProjection } from "../core/src/views/projections/productVariantProjection";
import { CollectionProjection } from "../core/src/views/projections/collectionProjection";
import { ExternalEffectHandler } from "../core/src/infrastructure/externalEffectHandler";

// Initialize handlers
const projectionHandler = new ProjectionHandler({
  db,
  productProjectionFactory: ProductProjection,
  productVariantProjectionFactory: ProductVariantProjection,
  collectionProjectionFactory: CollectionProjection,
});
const externalEffectHandler = new ExternalEffectHandler();

// Create the outbox poller
const poller = new OutboxPoller({
  db,
  projectionHandler,
  externalEffectHandler,
  leaseBatchSize: 100, // Lease up to 100 messages at a time
  processBatchSize: 10, // Process 10 messages in parallel
  maxAttempts: 6, // Maximum retry attempts before dead letter queue
});

// Handle graceful shutdown
async function gracefulShutdown(signal: string) {
  console.log(`Received ${signal}, initiating graceful shutdown...`);
  await poller.shutdown();
  console.log("Shutdown complete");
  process.exit(0);
}

process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));

// Handle uncaught errors
process.on("uncaughtException", (error) => {
  console.error("Uncaught exception:", error);
  gracefulShutdown("uncaughtException");
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("Unhandled rejection at:", promise, "reason:", reason);
  gracefulShutdown("unhandledRejection");
});

// Start the poller
console.log("Starting outbox worker...");
poller.poll().catch((error) => {
  console.error("Fatal error in outbox poller:", error);
  process.exit(1);
});
