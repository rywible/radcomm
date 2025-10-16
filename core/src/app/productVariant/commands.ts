import { z } from "zod";

export const CreateProductVariantCommand = z.object({
  variantId: z.string(),
  productId: z.string(),
  createdAt: z.date(),
  sku: z.string(),
  priceCents: z.number().int().positive(),
  imageUrl: z.string().url(),
  size: z.string(),
  color: z.string(),
});

export type CreateProductVariantCommand = z.infer<
  typeof CreateProductVariantCommand
>;

export const ArchiveProductVariantCommand = z.object({
  variantId: z.string(),
});

export type ArchiveProductVariantCommand = z.infer<
  typeof ArchiveProductVariantCommand
>;
