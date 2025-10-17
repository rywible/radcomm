import { UnitOfWork } from "../../infrastructure/unitOfWork";
import { CreateProductVariantCommand } from "./commands";
import { ProductVariantAggregate } from "@core/domain/productVariant/aggregate";
import { SkuIndexAggregate } from "@core/domain/skuIndex/aggregate";
import { DomainEventMapper } from "../domainEventMapper";

export class CreateProductVariantService {
  private unitOfWork: UnitOfWork;
  private eventMapper: DomainEventMapper;

  constructor(unitOfWork: UnitOfWork, eventMapper: DomainEventMapper) {
    this.unitOfWork = unitOfWork;
    this.eventMapper = eventMapper;
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
        for (const event of skuIndexAggregate.uncommittedEvents) {
          await eventRepository.add(event);

          const integrationEvents = this.eventMapper.toIntegrationEvents(event);
          for (const integrationEvent of integrationEvents) {
            await outboxRepository.add(integrationEvent);
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
        for (const event of productVariantAggregate.uncommittedEvents) {
          await eventRepository.add(event);

          const integrationEvents = this.eventMapper.toIntegrationEvents(event);
          for (const integrationEvent of integrationEvents) {
            await outboxRepository.add(integrationEvent);
          }
        }
      }
    );
  }
}
