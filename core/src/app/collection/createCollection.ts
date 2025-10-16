import { UnitOfWork } from "../unitOfWork";
import { CreateCollectionCommand } from "./commands";
import { CollectionAggregate } from "@core/domain/collection/aggregate";
import { DomainEventMapper } from "../domainEventMapper";

export class CreateCollectionService {
  private unitOfWork: UnitOfWork;
  private eventMapper: DomainEventMapper;

  constructor(unitOfWork: UnitOfWork, eventMapper: DomainEventMapper) {
    this.unitOfWork = unitOfWork;
    this.eventMapper = eventMapper;
  }

  async execute(command: CreateCollectionCommand) {
    await this.unitOfWork.withTransaction(
      async ({ eventRepository, outboxRepository }) => {
        const collectionAggregate = CollectionAggregate.create({
          id: command.collectionId,
          correlationId: command.correlationId,
          createdAt: command.createdAt,
          name: command.name,
          description: command.description,
          slug: command.slug,
          productIds: command.productIds,
        });

        for (const event of collectionAggregate.uncommittedEvents) {
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
