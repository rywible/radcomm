import { z } from "zod";

export const CreateCollectionCommand = z.object({
  collectionId: z.string(),
  correlationId: z.string(),
  createdAt: z.date(),
  name: z.string().min(1),
  description: z.string(),
  slug: z.string().min(1),
  productIds: z.array(z.string()),
});

export type CreateCollectionCommand = z.infer<typeof CreateCollectionCommand>;

export const ArchiveCollectionCommand = z.object({
  collectionId: z.string(),
});

export type ArchiveCollectionCommand = z.infer<typeof ArchiveCollectionCommand>;
