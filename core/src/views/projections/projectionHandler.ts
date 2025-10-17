import type { IntegrationEvent } from "../../integrationEvents/_base";
import { CollectionProjection } from "./collectionProjection";
import { ProductProjection } from "./productProjection";
import { ProductVariantProjection } from "./productVariantProjection";
import type { DB, TX } from "../../infrastructure/postgres";
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
import { InboxTable } from "../../infrastructure/orm";
import { eq } from "drizzle-orm";

type TransactionalClient = Pick<TX, "insert" | "select" | "update" | "delete">;

type ProjectionHandlerProps = {
  db: DB;
  productProjectionFactory: typeof ProductProjection;
  productVariantProjectionFactory: typeof ProductVariantProjection;
  collectionProjectionFactory: typeof CollectionProjection;
};

class ProjectionHandlerResult {
  public success: boolean;
  public error?: string;

  constructor({ success, error }: { success: boolean; error?: string }) {
    this.success = success;
    if (error !== undefined) {
      this.error = error;
    }
  }
}

export class ProjectionHandler {
  private db: DB;
  private productProjectionFactory: typeof ProductProjection;
  private productVariantProjectionFactory: typeof ProductVariantProjection;
  private collectionProjectionFactory: typeof CollectionProjection;

  constructor({
    db,
    productProjectionFactory,
    productVariantProjectionFactory,
    collectionProjectionFactory,
  }: ProjectionHandlerProps) {
    this.db = db;
    this.productProjectionFactory = productProjectionFactory;
    this.productVariantProjectionFactory = productVariantProjectionFactory;
    this.collectionProjectionFactory = collectionProjectionFactory;
  }

  private async routeEvent(
    event: IntegrationEvent<string, Record<string, unknown>>,
    tx: TransactionalClient
  ) {
    switch (event.eventName) {
      case "product.created": {
        const productProjection = new this.productProjectionFactory(tx);
        await productProjection.handleProductCreated(
          event as ProductCreatedIntegrationEvent
        );
        break;
      }
      case "product.archived": {
        const productProjection = new this.productProjectionFactory(tx);
        await productProjection.handleProductArchived(
          event as ProductArchivedIntegrationEvent
        );
        break;
      }
      case "product_variant.created": {
        const productVariantProjection =
          new this.productVariantProjectionFactory(tx);
        await productVariantProjection.handleProductVariantCreated(
          event as ProductVariantCreatedIntegrationEvent
        );
        break;
      }
      case "product_variant.archived": {
        const productVariantProjection =
          new this.productVariantProjectionFactory(tx);
        await productVariantProjection.handleProductVariantArchived(
          event as ProductVariantArchivedIntegrationEvent
        );
        break;
      }
      case "collection.created": {
        const collectionProjection = new this.collectionProjectionFactory(tx);
        await collectionProjection.handleCollectionCreated(
          event as CollectionCreatedIntegrationEvent
        );
        break;
      }
      case "collection.archived": {
        const collectionProjection = new this.collectionProjectionFactory(tx);
        await collectionProjection.handleCollectionArchived(
          event as CollectionArchivedIntegrationEvent
        );
        break;
      }
      default:
        console.warn(`Unknown integration event: ${event.eventName}`);
        break;
    }
  }

  async handleIntegrationEvent(
    event: IntegrationEvent<string, Record<string, unknown>>
  ): Promise<ProjectionHandlerResult> {
    try {
      return await this.db.transaction(async (tx) => {
        const inboxMessage = await tx
          .select()
          .from(InboxTable)
          .where(eq(InboxTable.id, event.eventId))
          .execute();
        if (inboxMessage[0]) {
          return new ProjectionHandlerResult({ success: true });
        }
        await tx.insert(InboxTable).values({ id: event.eventId }).execute();
        await this.routeEvent(event, tx);
        return new ProjectionHandlerResult({ success: true });
      });
    } catch (error) {
      return new ProjectionHandlerResult({
        success: false,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }
}
