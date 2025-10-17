import { CreateCollectionService } from "@core/app/collection/createCollection";
import { ArchiveCollectionService } from "@core/app/collection/archiveCollection";
import {
  CreateCollectionCommand,
  ArchiveCollectionCommand,
} from "@core/app/collection/commands";
import { UnitOfWork } from "@core/infrastructure/unitOfWork";
import {
  EventRepository,
  OutboxRepository,
} from "@core/infrastructure/repositories";
import { db } from "@core/infrastructure/postgres";
import { tryCatch } from "../response";
import { DomainEventMapper } from "@core/app/domainEventMapper";

export const createCollection = async (command: CreateCollectionCommand) => {
  const eventMapper = new DomainEventMapper();
  const unitOfWork = new UnitOfWork(db, EventRepository, OutboxRepository);
  const createCollectionService = new CreateCollectionService(
    unitOfWork,
    eventMapper
  );
  return await tryCatch(
    async () => await createCollectionService.execute(command)
  );
};

export const archiveCollection = async (command: ArchiveCollectionCommand) => {
  const eventMapper = new DomainEventMapper();
  const unitOfWork = new UnitOfWork(db, EventRepository, OutboxRepository);
  const archiveCollectionService = new ArchiveCollectionService(
    unitOfWork,
    eventMapper
  );
  return await tryCatch(
    async () => await archiveCollectionService.execute(command)
  );
};
