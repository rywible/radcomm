import { CreateProductService } from "@core/app/product/createProduct";
import { CreateProductCommand } from "@core/app/product/commands";
import { UnitOfWork } from "@core/app/unitOfWork";
import { EventRepository } from "@core/infrastructure/repository";
import { db } from "@core/infrastructure/postgres";
import { tryCatch } from "../response";

export const createProduct = async (command: CreateProductCommand) => {
  const unitOfWork = new UnitOfWork(db, EventRepository);
  const createProductService = new CreateProductService(unitOfWork);
  return await tryCatch(
    async () => await createProductService.execute(command)
  );
};
