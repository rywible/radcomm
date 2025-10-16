import { z } from "zod";

export const CreateProductCommand = z.object({
  productId: z.string(),
  correlationId: z.string(),
  createdAt: z.date(),
  title: z.string().min(1),
  description: z.string(),
  slug: z.string().min(1),
  collectionIds: z.array(z.string()),
  variantIds: z.array(z.string()),
});

export type CreateProductCommand = z.infer<typeof CreateProductCommand>;

export const ArchiveProductCommand = z.object({
  productId: z.string(),
});

export type ArchiveProductCommand = z.infer<typeof ArchiveProductCommand>;
