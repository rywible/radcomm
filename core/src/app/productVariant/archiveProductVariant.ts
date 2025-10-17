import { UnitOfWork } from "../../infrastructure/unitOfWork";
import { ArchiveProductVariantCommand } from "./commands";
import { ProductVariantAggregate } from "@core/domain/productVariant/aggregate";
import { SkuIndexAggregate } from "@core/domain/skuIndex/aggregate";
import { DomainEventMapper } from "../domainEventMapper";

export class ArchiveProductVariantService {
  private unitOfWork: UnitOfWork;
  private eventMapper: DomainEventMapper;

  constructor(unitOfWork: UnitOfWork, eventMapper: DomainEventMapper) {
    this.unitOfWork = unitOfWork;
    this.eventMapper = eventMapper;
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
        for (const event of productVariantAggregate.uncommittedEvents) {
          await eventRepository.add(event);

          const integrationEvents = this.eventMapper.toIntegrationEvents(event);
          for (const integrationEvent of integrationEvents) {
            await outboxRepository.add(integrationEvent);
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
        for (const event of skuIndexAggregate.uncommittedEvents) {
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
