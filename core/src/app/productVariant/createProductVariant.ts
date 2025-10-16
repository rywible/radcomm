import { UnitOfWork } from "../unitOfWork";
import { CreateProductVariantCommand } from "./commands";
import { ProductVariantAggregate } from "@core/domain/productVariant/aggregate";

export class CreateProductVariantService {
  private unitOfWork: UnitOfWork;

  constructor(unitOfWork: UnitOfWork) {
    this.unitOfWork = unitOfWork;
  }

  async execute(command: CreateProductVariantCommand) {
    await this.unitOfWork.withTransaction(async ({ eventRepository }) => {
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
      for (const event of productVariantAggregate.events) {
        if (!event.committed) {
          await eventRepository.add(event);
        }
      }
    });
  }
}
