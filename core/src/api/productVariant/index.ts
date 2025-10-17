import { CreateProductVariantService } from "@core/app/productVariant/createProductVariant";
import { ArchiveProductVariantService } from "@core/app/productVariant/archiveProductVariant";
import {
  CreateProductVariantCommand,
  ArchiveProductVariantCommand,
} from "@core/app/productVariant/commands";
import { UnitOfWork } from "@core/infrastructure/unitOfWork";
import {
  EventRepository,
  OutboxRepository,
} from "@core/infrastructure/repositories";
import { db } from "@core/infrastructure/postgres";
import { tryCatch } from "../response";
import { DomainEventMapper } from "@core/app/domainEventMapper";

export const createProductVariant = async (
  command: CreateProductVariantCommand
) => {
  const eventMapper = new DomainEventMapper();
  const unitOfWork = new UnitOfWork(db, EventRepository, OutboxRepository);
  const createProductVariantService = new CreateProductVariantService(
    unitOfWork,
    eventMapper
  );
  return await tryCatch(
    async () => await createProductVariantService.execute(command)
  );
};

export const archiveProductVariant = async (
  command: ArchiveProductVariantCommand
) => {
  const eventMapper = new DomainEventMapper();
  const unitOfWork = new UnitOfWork(db, EventRepository, OutboxRepository);
  const archiveProductVariantService = new ArchiveProductVariantService(
    unitOfWork,
    eventMapper
  );
  return await tryCatch(
    async () => await archiveProductVariantService.execute(command)
  );
};
