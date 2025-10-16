import { UnitOfWork } from "../unitOfWork";
import { ArchiveProductCommand } from "./commands";
import { ProductAggregate } from "@core/domain/product/aggregate";

export class ArchiveProductService {
  private unitOfWork: UnitOfWork;

  constructor(unitOfWork: UnitOfWork) {
    this.unitOfWork = unitOfWork;
  }

  async execute(command: ArchiveProductCommand) {
    await this.unitOfWork.withTransaction(async ({ eventRepository }) => {
      const events = await eventRepository.findByAggregateId(command.productId);
      const productAggregate = ProductAggregate.loadFromHistory(events);

      productAggregate.archive();

      for (const event of productAggregate.events) {
        if (!event.committed) {
          await eventRepository.add(event);
        }
      }
    });
  }
}
