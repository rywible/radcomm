import { UnitOfWork } from "../unitOfWork";
import { DeleteProductCommand } from "./commands";
import { ProductAggregate } from "@core/domain/product/aggregate";

export class DeleteProductService {
  private unitOfWork: UnitOfWork;

  constructor(unitOfWork: UnitOfWork) {
    this.unitOfWork = unitOfWork;
  }

  async execute(command: DeleteProductCommand) {
    await this.unitOfWork.withTransaction(async ({ eventRepository }) => {
      const events = await eventRepository.findByAggregateId(command.productId);
      const productAggregate = ProductAggregate.loadFromHistory(events);

      productAggregate.delete();

      for (const event of productAggregate.events) {
        if (!event.committed) {
          await eventRepository.add(event);
        }
      }
    });
  }
}
