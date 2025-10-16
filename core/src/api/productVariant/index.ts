import { CreateProductVariantService } from "@core/app/productVariant/createProductVariant";
import { ArchiveProductVariantService } from "@core/app/productVariant/archiveProductVariant";
import {
  CreateProductVariantCommand,
  ArchiveProductVariantCommand,
} from "@core/app/productVariant/commands";
import { UnitOfWork } from "@core/app/unitOfWork";
import { EventRepository } from "@core/infrastructure/repository";
import { db } from "@core/infrastructure/postgres";
import { tryCatch } from "../response";

export const createProductVariant = async (
  command: CreateProductVariantCommand
) => {
  const unitOfWork = new UnitOfWork(db, EventRepository);
  const createProductVariantService = new CreateProductVariantService(
    unitOfWork
  );
  return await tryCatch(
    async () => await createProductVariantService.execute(command)
  );
};

export const archiveProductVariant = async (
  command: ArchiveProductVariantCommand
) => {
  const unitOfWork = new UnitOfWork(db, EventRepository);
  const archiveProductVariantService = new ArchiveProductVariantService(
    unitOfWork
  );
  return await tryCatch(
    async () => await archiveProductVariantService.execute(command)
  );
};
