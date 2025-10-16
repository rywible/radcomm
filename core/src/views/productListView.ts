import type { DB } from "../infrastructure/postgres";
import { ProductListViewTable } from "../infrastructure/orm";
import { eq } from "drizzle-orm";

export type ProductListViewType = typeof ProductListViewTable.$inferSelect;

type FindAllFilters = {
  status?: string;
  limit?: number;
  offset?: number;
};

export class ProductListView {
  private db: DB;

  constructor(db: DB) {
    this.db = db;
  }

  async findAll(filters?: FindAllFilters): Promise<ProductListViewType[]> {
    let query = this.db.select().from(ProductListViewTable);

    if (filters?.status) {
      query = query.where(
        eq(ProductListViewTable.status, filters.status)
      ) as any;
    }

    if (filters?.limit) {
      query = query.limit(filters.limit) as any;
    }

    if (filters?.offset) {
      query = query.offset(filters.offset) as any;
    }

    return await query;
  }

  async findById(productId: string): Promise<ProductListViewType | null> {
    const results = await this.db
      .select()
      .from(ProductListViewTable)
      .where(eq(ProductListViewTable.productId, productId));

    return results[0] ?? null;
  }

  async findBySlug(slug: string): Promise<ProductListViewType | null> {
    const results = await this.db
      .select()
      .from(ProductListViewTable)
      .where(eq(ProductListViewTable.slug, slug));

    return results[0] ?? null;
  }
}
