import { UnitOfWork } from "../../infrastructure/unitOfWork";
import { ArchiveProductCommand } from "./commands";
import { ProductAggregate } from "@core/domain/product/aggregate";
import { DomainEventMapper } from "../domainEventMapper";

export class ArchiveProductService {
  private unitOfWork: UnitOfWork;
  private eventMapper: DomainEventMapper;

  constructor(unitOfWork: UnitOfWork, eventMapper: DomainEventMapper) {
    this.unitOfWork = unitOfWork;
    this.eventMapper = eventMapper;
  }

  async execute(command: ArchiveProductCommand) {
    await this.unitOfWork.withTransaction(
      async ({ eventRepository, outboxRepository }) => {
        const events = await eventRepository.findByAggregateId(
          command.productId
        );
        const productAggregate = ProductAggregate.loadFromHistory(events);

        productAggregate.archive();

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
