import type { DB } from "../infrastructure/postgres";
import { CollectionDetailViewTable } from "../infrastructure/orm";
import { eq } from "drizzle-orm";

export type CollectionDetailViewType =
  typeof CollectionDetailViewTable.$inferSelect;

export class CollectionDetailView {
  private db: DB;

  constructor(db: DB) {
    this.db = db;
  }

  async findByCollectionId(
    collectionId: string
  ): Promise<CollectionDetailViewType[]> {
    return await this.db
      .select()
      .from(CollectionDetailViewTable)
      .where(eq(CollectionDetailViewTable.collectionId, collectionId));
  }

  async findByProductId(
    productId: string
  ): Promise<CollectionDetailViewType[]> {
    return await this.db
      .select()
      .from(CollectionDetailViewTable)
      .where(eq(CollectionDetailViewTable.productId, productId));
  }
}
