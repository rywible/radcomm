import { UnitOfWork } from "../unitOfWork";
import { CreateProductVariantCommand } from "./commands";
import { ProductVariantAggregate } from "@core/domain/productVariant/aggregate";
import { SkuIndexAggregate } from "@core/domain/skuIndex/aggregate";
import { ProductVariantCreatedIntegrationEvent } from "@core/integration/events/productVariant";
import { randomUUID } from "crypto";

export class CreateProductVariantService {
  private unitOfWork: UnitOfWork;

  constructor(unitOfWork: UnitOfWork) {
    this.unitOfWork = unitOfWork;
  }

  async execute(command: CreateProductVariantCommand) {
    await this.unitOfWork.withTransaction(
      async ({ eventRepository, outboxRepository }) => {
        // Check if SKU aggregate exists
        const skuIndexEvents = await eventRepository.findByAggregateId(
          command.sku
        );

        let skuIndexAggregate: SkuIndexAggregate;

        if (skuIndexEvents.length > 0) {
          // SKU aggregate exists, load it and check if it's available
          skuIndexAggregate = SkuIndexAggregate.loadFromHistory(skuIndexEvents);
          if (skuIndexAggregate.isReserved()) {
            throw new Error(`SKU ${command.sku} is already reserved`);
          }
          // SKU was previously released, re-reserve it
          skuIndexAggregate.reserve();
        } else {
          // Create new SKU index aggregate to reserve the SKU
          skuIndexAggregate = SkuIndexAggregate.create({
            id: command.sku,
            correlationId: command.productId,
            createdAt: command.createdAt,
          });
        }

        // Save SKU index events
        for (const event of skuIndexAggregate.events) {
          if (!event.committed) {
            await eventRepository.add(event);
          }
        }

        // Create the product variant
        const productVariantAggregate = ProductVariantAggregate.create({
          id: command.variantId,
          productId: command.productId,
          correlationId: command.productId,
          createdAt: command.createdAt,
          sku: command.sku,
          priceCents: command.priceCents,
          imageUrl: command.imageUrl,
          size: command.size,
          color: command.color,
        });

        // Save product variant events
        for (const event of productVariantAggregate.events) {
          if (!event.committed) {
            await eventRepository.add(event);
          }
        }

        // Publish integration event to outbox for external consumers
        const integrationEvent = new ProductVariantCreatedIntegrationEvent({
          eventId: randomUUID(),
          occurredAt: command.createdAt,
          correlationId: command.productId,
          payload: {
            variantId: command.variantId,
            productId: command.productId,
            sku: command.sku,
            priceUsd: (command.priceCents / 100).toFixed(2),
            imageUrl: command.imageUrl,
            attributes: {
              size: command.size,
              color: command.color,
            },
          },
        });

        await outboxRepository.add(integrationEvent);
      }
    );
  }
}
