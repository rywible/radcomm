import { UnitOfWork } from "../unitOfWork";
import { ArchiveProductVariantCommand } from "./commands";
import { ProductVariantAggregate } from "@core/domain/productVariant/aggregate";
import { SkuIndexAggregate } from "@core/domain/skuIndex/aggregate";
import { ProductVariantArchivedIntegrationEvent } from "@core/integration/events/productVariant";
import { randomUUID } from "crypto";

export class ArchiveProductVariantService {
  private unitOfWork: UnitOfWork;

  constructor(unitOfWork: UnitOfWork) {
    this.unitOfWork = unitOfWork;
  }

  async execute(command: ArchiveProductVariantCommand) {
    await this.unitOfWork.withTransaction(
      async ({ eventRepository, outboxRepository }) => {
        // Load and archive the product variant
        const events = await eventRepository.findByAggregateId(
          command.variantId
        );
        const productVariantAggregate =
          ProductVariantAggregate.loadFromHistory(events);

        const sku = productVariantAggregate.getSku();

        productVariantAggregate.archive();

        // Save product variant events
        for (const event of productVariantAggregate.events) {
          if (!event.committed) {
            await eventRepository.add(event);
          }
        }

        // Load SKU index and release the SKU
        const skuIndexEvents = await eventRepository.findByAggregateId(sku);

        if (skuIndexEvents.length === 0) {
          throw new Error(`SKU index not found for SKU: ${sku}`);
        }

        const skuIndexAggregate =
          SkuIndexAggregate.loadFromHistory(skuIndexEvents);
        skuIndexAggregate.release();

        // Save SKU index events
        for (const event of skuIndexAggregate.events) {
          if (!event.committed) {
            await eventRepository.add(event);
          }
        }

        // Publish integration event to outbox for external consumers
        const integrationEvent = new ProductVariantArchivedIntegrationEvent({
          eventId: randomUUID(),
          occurredAt: new Date(),
          correlationId: command.variantId,
          payload: {
            variantId: command.variantId,
            sku,
          },
        });

        await outboxRepository.add(integrationEvent);
      }
    );
  }
}
