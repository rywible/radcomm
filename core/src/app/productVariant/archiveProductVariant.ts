import { UnitOfWork } from "../unitOfWork";
import { ArchiveProductVariantCommand } from "./commands";
import { ProductVariantAggregate } from "@core/domain/productVariant/aggregate";

export class ArchiveProductVariantService {
  private unitOfWork: UnitOfWork;

  constructor(unitOfWork: UnitOfWork) {
    this.unitOfWork = unitOfWork;
  }

  async execute(command: ArchiveProductVariantCommand) {
    await this.unitOfWork.withTransaction(async ({ eventRepository }) => {
      const events = await eventRepository.findByAggregateId(command.variantId);
      const productVariantAggregate =
        ProductVariantAggregate.loadFromHistory(events);
      productVariantAggregate.archive();
      for (const event of productVariantAggregate.events) {
        if (!event.committed) {
          await eventRepository.add(event);
        }
      }
    });
  }
}
