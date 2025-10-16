import type { TX } from "../../infrastructure/postgres";
import {
  ProductListViewTable,
  ProductDetailViewTable,
  CollectionDetailViewTable,
} from "../../infrastructure/orm";
import {
  ProductVariantCreatedIntegrationEvent,
  ProductVariantArchivedIntegrationEvent,
} from "../../integrationEvents/productVariant";
import { eq, and, sql } from "drizzle-orm";

type TransactionalClient = Pick<TX, "insert" | "select" | "update" | "delete">;

export class ProductVariantProjection {
  async handleProductVariantCreated(
    event: ProductVariantCreatedIntegrationEvent,
    tx: TransactionalClient
  ): Promise<void> {
    const { variantId, productId, sku, priceUsd, imageUrl, attributes } =
      event.payload;
    const priceCents = Math.round(parseFloat(priceUsd) * 100);

    // Fetch product info from product_list_view
    const product = await tx
      .select()
      .from(ProductListViewTable)
      .where(eq(ProductListViewTable.productId, productId));

    if (!product[0]) {
      throw new Error(`Product ${productId} not found in product_list_view`);
    }

    // Insert into product_detail_view
    await tx.insert(ProductDetailViewTable).values({
      variantId,
      productId,
      productTitle: product[0].title,
      productSlug: product[0].slug,
      productDescription: product[0].description,
      productStatus: product[0].status,
      productCreatedAt: product[0].createdAt,
      sku,
      priceCents,
      size: attributes.size,
      color: attributes.color,
      imageUrl,
      variantStatus: "active",
    });

    // Update product_list_view: increment variant_count
    await tx
      .update(ProductListViewTable)
      .set({
        variantCount: sql`${ProductListViewTable.variantCount} + 1`,
      })
      .where(eq(ProductListViewTable.productId, productId));

    // Update collection_detail_view: recalculate variant_count
    const activeVariantCount = await tx
      .select({ count: sql<number>`count(*)::int` })
      .from(ProductDetailViewTable)
      .where(
        and(
          eq(ProductDetailViewTable.productId, productId),
          eq(ProductDetailViewTable.variantStatus, "active")
        )
      );

    await tx
      .update(CollectionDetailViewTable)
      .set({
        variantCount: activeVariantCount[0]?.count ?? 0,
      })
      .where(eq(CollectionDetailViewTable.productId, productId));
  }

  async handleProductVariantArchived(
    event: ProductVariantArchivedIntegrationEvent,
    tx: TransactionalClient
  ): Promise<void> {
    const { variantId } = event.payload;

    // Get the product ID before updating
    const variant = await tx
      .select({ productId: ProductDetailViewTable.productId })
      .from(ProductDetailViewTable)
      .where(eq(ProductDetailViewTable.variantId, variantId));

    if (!variant[0]) {
      throw new Error(`Variant ${variantId} not found in product_detail_view`);
    }

    const productId = variant[0].productId;

    // Update product_detail_view
    await tx
      .update(ProductDetailViewTable)
      .set({ variantStatus: "archived" })
      .where(eq(ProductDetailViewTable.variantId, variantId));

    // Recalculate and update product_list_view variant_count
    const activeVariantCount = await tx
      .select({ count: sql<number>`count(*)::int` })
      .from(ProductDetailViewTable)
      .where(
        and(
          eq(ProductDetailViewTable.productId, productId),
          eq(ProductDetailViewTable.variantStatus, "active")
        )
      );

    await tx
      .update(ProductListViewTable)
      .set({
        variantCount: activeVariantCount[0]?.count ?? 0,
      })
      .where(eq(ProductListViewTable.productId, productId));

    // Recalculate and update collection_detail_view variant_count
    await tx
      .update(CollectionDetailViewTable)
      .set({
        variantCount: activeVariantCount[0]?.count ?? 0,
      })
      .where(eq(CollectionDetailViewTable.productId, productId));
  }
}
