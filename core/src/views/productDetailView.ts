import type { DB } from "../infrastructure/postgres";
import { ProductDetailViewTable } from "../infrastructure/orm";
import { eq } from "drizzle-orm";

export type ProductDetailViewType = typeof ProductDetailViewTable.$inferSelect;

export class ProductDetailView {
  private db: DB;

  constructor(db: DB) {
    this.db = db;
  }

  async findByProductId(productId: string): Promise<ProductDetailViewType[]> {
    return await this.db
      .select()
      .from(ProductDetailViewTable)
      .where(eq(ProductDetailViewTable.productId, productId));
  }

  async findByVariantId(
    variantId: string
  ): Promise<ProductDetailViewType | null> {
    const results = await this.db
      .select()
      .from(ProductDetailViewTable)
      .where(eq(ProductDetailViewTable.variantId, variantId));

    return results[0] ?? null;
  }

  async findBySku(sku: string): Promise<ProductDetailViewType | null> {
    const results = await this.db
      .select()
      .from(ProductDetailViewTable)
      .where(eq(ProductDetailViewTable.sku, sku));

    return results[0] ?? null;
  }
}
