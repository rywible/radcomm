import { UnitOfWork } from "../../infrastructure/unitOfWork";
import { CreateProductCommand } from "./commands";
import { ProductAggregate } from "@core/domain/product/aggregate";
import { DomainEventMapper } from "../domainEventMapper";

export class CreateProductService {
  private unitOfWork: UnitOfWork;
  private eventMapper: DomainEventMapper;

  constructor(unitOfWork: UnitOfWork, eventMapper: DomainEventMapper) {
    this.unitOfWork = unitOfWork;
    this.eventMapper = eventMapper;
  }

  async execute(command: CreateProductCommand) {
    await this.unitOfWork.withTransaction(
      async ({ eventRepository, outboxRepository }) => {
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

        for (const event of productAggregate.uncommittedEvents) {
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
