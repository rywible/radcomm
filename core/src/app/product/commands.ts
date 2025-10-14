import { z } from "zod";

const ProductVariantSchema = z.object({
  id: z.string(),
  sku: z.string(),
  priceCents: z.number().int().positive(),
  imageUrl: z.url(),
  size: z.string(),
  color: z.string(),
  quantity: z.number().int().nonnegative(),
});

export const CreateProductCommand = z.object({
  productId: z.string(),
  correlationId: z.string(),
  createdAt: z.date(),
  title: z.string().min(1),
  description: z.string(),
  slug: z.string().min(1),
  collectionIds: z.array(z.string()),
  variants: z.array(ProductVariantSchema),
});

export type CreateProductCommand = z.infer<typeof CreateProductCommand>;

export const DeleteProductCommand = z.object({
  productId: z.string(),
});

export type DeleteProductCommand = z.infer<typeof DeleteProductCommand>;
