type ProductVariantParams = {
  id: string;
  sku: string;
  priceCents: number;
  imageUrl: string;
  size: string;
  color: string;
  quantity: number;
};

export class ProductVariant {
  public readonly id: string;
  public readonly sku: string;
  public readonly priceCents: number;
  public readonly imageUrl: string;
  public readonly size: string;
  public readonly color: string;
  public readonly quantity: number;

  constructor({
    id,
    sku,
    priceCents,
    imageUrl,
    size,
    color,
    quantity,
  }: ProductVariantParams) {
    this.id = id;
    this.sku = sku;
    this.priceCents = priceCents;
    this.imageUrl = imageUrl;
    this.size = size;
    this.color = color;
    this.quantity = quantity;
  }
}
