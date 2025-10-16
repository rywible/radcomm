import type { TX } from "../../infrastructure/postgres";
import {
  ProductListViewTable,
  ProductDetailViewTable,
  CollectionListViewTable,
  CollectionDetailViewTable,
} from "../../infrastructure/orm";
import {
  ProductCreatedIntegrationEvent,
  ProductArchivedIntegrationEvent,
} from "../../integrationEvents/product";
import { eq, sql } from "drizzle-orm";

type TransactionalClient = Pick<TX, "insert" | "select" | "update" | "delete">;

export class ProductProjection {
  async handleProductCreated(
    event: ProductCreatedIntegrationEvent,
    tx: TransactionalClient
  ): Promise<void> {
    const { productId, title, slug, description, collectionIds } =
      event.payload;

    // Fetch collection data for the collections JSONB array
    const collections = await tx
      .select({
        id: CollectionListViewTable.collectionId,
        name: CollectionListViewTable.name,
        slug: CollectionListViewTable.slug,
      })
      .from(CollectionListViewTable)
      .where(
        sql`${CollectionListViewTable.collectionId} = ANY(${collectionIds})`
      );

    // Insert into product_list_view
    await tx.insert(ProductListViewTable).values({
      productId,
      title,
      slug,
      description,
      status: "active",
      createdAt: event.occurredAt,
      variantCount: 0,
      collections: collections,
    });

    // Insert into collection_detail_view for each collection
    for (const collectionId of collectionIds) {
      const collection = await tx
        .select()
        .from(CollectionListViewTable)
        .where(eq(CollectionListViewTable.collectionId, collectionId));

      if (collection[0]) {
        await tx.insert(CollectionDetailViewTable).values({
          collectionId,
          productId,
          collectionName: collection[0].name,
          collectionSlug: collection[0].slug,
          collectionDescription: collection[0].description,
          collectionStatus: collection[0].status,
          collectionCreatedAt: collection[0].createdAt,
          productTitle: title,
          productSlug: slug,
          productStatus: "active",
          variantCount: 0,
        });
      }
    }
  }

  async handleProductArchived(
    event: ProductArchivedIntegrationEvent,
    tx: TransactionalClient
  ): Promise<void> {
    const { productId } = event.payload;

    // Update product_list_view
    await tx
      .update(ProductListViewTable)
      .set({ status: "archived" })
      .where(eq(ProductListViewTable.productId, productId));

    // Update product_detail_view
    await tx
      .update(ProductDetailViewTable)
      .set({ productStatus: "archived" })
      .where(eq(ProductDetailViewTable.productId, productId));

    // Update collection_detail_view
    await tx
      .update(CollectionDetailViewTable)
      .set({ productStatus: "archived" })
      .where(eq(CollectionDetailViewTable.productId, productId));
  }
}
