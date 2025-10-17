import { UnitOfWork } from "../../infrastructure/unitOfWork";
import { ArchiveCollectionCommand } from "./commands";
import { CollectionAggregate } from "@core/domain/collection/aggregate";
import { DomainEventMapper } from "../domainEventMapper";

export class ArchiveCollectionService {
  private unitOfWork: UnitOfWork;
  private eventMapper: DomainEventMapper;

  constructor(unitOfWork: UnitOfWork, eventMapper: DomainEventMapper) {
    this.unitOfWork = unitOfWork;
    this.eventMapper = eventMapper;
  }

  async execute(command: ArchiveCollectionCommand) {
    await this.unitOfWork.withTransaction(
      async ({ eventRepository, outboxRepository }) => {
        const events = await eventRepository.findByAggregateId(
          command.collectionId
        );

        if (events.length === 0) {
          throw new Error(
            `Collection with ID ${command.collectionId} not found`
          );
        }

        const collectionAggregate = CollectionAggregate.loadFromHistory(events);

        if (collectionAggregate.isArchived()) {
          throw new Error(
            `Collection with ID ${command.collectionId} is already archived`
          );
        }

        collectionAggregate.archive();

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
