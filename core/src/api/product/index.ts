import { CreateProductService } from "@core/app/product/createProduct";
import { ArchiveProductService } from "@core/app/product/archiveProduct";
import {
  CreateProductCommand,
  ArchiveProductCommand,
} from "@core/app/product/commands";
import { UnitOfWork } from "@core/infrastructure/unitOfWork";
import {
  EventRepository,
  OutboxRepository,
} from "@core/infrastructure/repositories";
import { db } from "@core/infrastructure/postgres";
import { tryCatch } from "../response";
import { DomainEventMapper } from "@core/app/domainEventMapper";

export const createProduct = async (command: CreateProductCommand) => {
  const eventMapper = new DomainEventMapper();
  const unitOfWork = new UnitOfWork(db, EventRepository, OutboxRepository);
  const createProductService = new CreateProductService(
    unitOfWork,
    eventMapper
  );
  return await tryCatch(
    async () => await createProductService.execute(command)
  );
};

export const archiveProduct = async (command: ArchiveProductCommand) => {
  const eventMapper = new DomainEventMapper();
  const unitOfWork = new UnitOfWork(db, EventRepository, OutboxRepository);
  const archiveProductService = new ArchiveProductService(
    unitOfWork,
    eventMapper
  );
  return await tryCatch(
    async () => await archiveProductService.execute(command)
  );
};
