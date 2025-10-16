import { UnitOfWork } from "../unitOfWork";
import { CreateProductCommand } from "./commands";
import { ProductAggregate } from "@core/domain/product/aggregate";

export class CreateProductService {
  private unitOfWork: UnitOfWork;

  constructor(unitOfWork: UnitOfWork) {
    this.unitOfWork = unitOfWork;
  }

  async execute(command: CreateProductCommand) {
    await this.unitOfWork.withTransaction(async ({ eventRepository }) => {
      const productAggregate = ProductAggregate.create({
        id: command.productId,
        correlationId: command.correlationId,
        createdAt: command.createdAt,
        title: command.title,
        description: command.description,
        slug: command.slug,
        collectionIds: command.collectionIds,
        variantIds: command.variantIds,
      });
      for (const event of productAggregate.events) {
        if (!event.committed) {
          await eventRepository.add(event);
        }
      }
    });
  }
}
