import { CreateProductService } from "@core/app/product/createProduct";
import { DeleteProductService } from "@core/app/product/deleteProduct";
import {
  CreateProductCommand,
  DeleteProductCommand,
} from "@core/app/product/commands";
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

export const deleteProduct = async (command: DeleteProductCommand) => {
  const unitOfWork = new UnitOfWork(db, EventRepository);
  const deleteProductService = new DeleteProductService(unitOfWork);
  return await tryCatch(
    async () => await deleteProductService.execute(command)
  );
};
