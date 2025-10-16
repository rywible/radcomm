import type { TX } from "../../infrastructure/postgres";
import type { IntegrationEvent } from "../../integrationEvents/_base";
import {
  ProductCreatedIntegrationEvent,
  ProductArchivedIntegrationEvent,
} from "../../integrationEvents/product";
import {
  ProductVariantCreatedIntegrationEvent,
  ProductVariantArchivedIntegrationEvent,
} from "../../integrationEvents/productVariant";
import {
  CollectionCreatedIntegrationEvent,
  CollectionArchivedIntegrationEvent,
} from "../../integrationEvents/collection";
import { ProductProjection } from "./productProjection";
import { ProductVariantProjection } from "./productVariantProjection";
import { CollectionProjection } from "./collectionProjection";

type TransactionalClient = Pick<TX, "insert" | "select" | "update" | "delete">;

export class ProjectionService {
  private productProjection: ProductProjection;
  private productVariantProjection: ProductVariantProjection;
  private collectionProjection: CollectionProjection;

  constructor() {
    this.productProjection = new ProductProjection();
    this.productVariantProjection = new ProductVariantProjection();
    this.collectionProjection = new CollectionProjection();
  }

  async handleIntegrationEvent(
    event: IntegrationEvent<string, Record<string, unknown>>,
    tx: TransactionalClient
  ): Promise<void> {
    switch (event.eventName) {
      case "product.created":
        await this.productProjection.handleProductCreated(
          event as ProductCreatedIntegrationEvent,
          tx
        );
        break;

      case "product.archived":
        await this.productProjection.handleProductArchived(
          event as ProductArchivedIntegrationEvent,
          tx
        );
        break;

      case "productVariant.created":
        await this.productVariantProjection.handleProductVariantCreated(
          event as ProductVariantCreatedIntegrationEvent,
          tx
        );
        break;

      case "productVariant.archived":
        await this.productVariantProjection.handleProductVariantArchived(
          event as ProductVariantArchivedIntegrationEvent,
          tx
        );
        break;

      case "collection.created":
        await this.collectionProjection.handleCollectionCreated(
          event as CollectionCreatedIntegrationEvent,
          tx
        );
        break;

      case "collection.archived":
        await this.collectionProjection.handleCollectionArchived(
          event as CollectionArchivedIntegrationEvent,
          tx
        );
        break;

      default:
        // Unknown event - skip or log
        console.warn(`No projection handler for event: ${event.eventName}`);
    }
  }
}
