import type { DB, TX } from "../../infrastructure/postgres";
import {
  CollectionListViewTable,
  CollectionDetailViewTable,
  ProductListViewTable,
} from "../../infrastructure/orm";
import {
  CollectionCreatedIntegrationEvent,
  CollectionArchivedIntegrationEvent,
} from "../../integrationEvents/collection";
import { eq, sql } from "drizzle-orm";

type TransactionalClient = Pick<TX, "insert" | "select" | "update" | "delete">;

export class CollectionProjection {
  private db: TransactionalClient;

  constructor(db: TransactionalClient) {
    this.db = db;
  }

  async handleCollectionCreated(
    event: CollectionCreatedIntegrationEvent
  ): Promise<void> {
    const { collectionId, name, slug, description, productIds } = event.payload;

    // Insert into collection_list_view
    await this.db.insert(CollectionListViewTable).values({
      collectionId,
      name,
      slug,
      description,
      status: "active",
      createdAt: event.occurredAt,
      productCount: productIds.length,
    });

    // For each product, insert into collection_detail_view and update product_list_view
    for (const productId of productIds) {
      const product = await this.db
        .select()
        .from(ProductListViewTable)
        .where(eq(ProductListViewTable.productId, productId));

      if (product[0]) {
        // Insert into collection_detail_view
        await this.db.insert(CollectionDetailViewTable).values({
          collectionId,
          productId,
          collectionName: name,
          collectionSlug: slug,
          collectionDescription: description,
          collectionStatus: "active",
          collectionCreatedAt: event.occurredAt,
          productTitle: product[0].title,
          productSlug: product[0].slug,
          productStatus: product[0].status,
          variantCount: product[0].variantCount,
        });

        // Update product_list_view.collections JSONB array
        await this.db
          .update(ProductListViewTable)
          .set({
            collections: sql`${ProductListViewTable.collections} || ${JSON.stringify([{ id: collectionId, name, slug }])}::jsonb`,
          })
          .where(eq(ProductListViewTable.productId, productId));
      }
    }
  }

  async handleCollectionArchived(
    event: CollectionArchivedIntegrationEvent
  ): Promise<void> {
    const { collectionId } = event.payload;

    // Update collection_list_view
    await this.db
      .update(CollectionListViewTable)
      .set({ status: "archived" })
      .where(eq(CollectionListViewTable.collectionId, collectionId));

    // Update collection_detail_view
    await this.db
      .update(CollectionDetailViewTable)
      .set({ collectionStatus: "archived" })
      .where(eq(CollectionDetailViewTable.collectionId, collectionId));

    // Update product_list_view.collections JSONB: mark collection as archived
    // We'll filter out the archived collection from the array
    const affectedProducts = await this.db
      .select({ productId: CollectionDetailViewTable.productId })
      .from(CollectionDetailViewTable)
      .where(eq(CollectionDetailViewTable.collectionId, collectionId));

    for (const { productId } of affectedProducts) {
      await this.db
        .update(ProductListViewTable)
        .set({
          collections: sql`(
            SELECT jsonb_agg(elem)
            FROM jsonb_array_elements(${ProductListViewTable.collections}) elem
            WHERE elem->>'id' != ${collectionId}
          )`,
        })
        .where(eq(ProductListViewTable.productId, productId));
    }
  }
}
