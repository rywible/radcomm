import type { DB } from "../infrastructure/postgres";
import { CollectionListViewTable } from "../infrastructure/orm";
import { eq } from "drizzle-orm";

export type CollectionListViewType =
  typeof CollectionListViewTable.$inferSelect;

type FindAllFilters = {
  status?: string;
  limit?: number;
  offset?: number;
};

export class CollectionListView {
  private db: DB;

  constructor(db: DB) {
    this.db = db;
  }

  async findAll(filters?: FindAllFilters): Promise<CollectionListViewType[]> {
    let query = this.db.select().from(CollectionListViewTable);

    if (filters?.status) {
      query = query.where(
        eq(CollectionListViewTable.status, filters.status)
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

  async findById(collectionId: string): Promise<CollectionListViewType | null> {
    const results = await this.db
      .select()
      .from(CollectionListViewTable)
      .where(eq(CollectionListViewTable.collectionId, collectionId));

    return results[0] ?? null;
  }

  async findBySlug(slug: string): Promise<CollectionListViewType | null> {
    const results = await this.db
      .select()
      .from(CollectionListViewTable)
      .where(eq(CollectionListViewTable.slug, slug));

    return results[0] ?? null;
  }
}
