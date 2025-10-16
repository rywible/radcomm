import type { DomainEvent } from "../_base/domainEvent";

type ProductVariantCreatedEventPayload = {
  productId: string;
  sku: string;
  priceCents: number;
  imageUrl: string;
  size: string;
  color: string;
};

type ProductVariantCreatedEventType = DomainEvent<
  "ProductVariantCreated",
  ProductVariantCreatedEventPayload
>;

type ProductVariantCreatedEventParams = {
  createdAt: Date;
  aggregateId: string;
  correlationId: string;
  version: number;
  payload: ProductVariantCreatedEventPayload;
  committed: boolean;
};

export class ProductVariantCreatedEvent
  implements ProductVariantCreatedEventType
{
  createdAt: Date;
  eventName = "ProductVariantCreated" as const;
  correlationId: string;
  aggregateId: string;
  version: number;
  payload: ProductVariantCreatedEventPayload;
  committed: boolean;

  constructor({
    createdAt,
    aggregateId,
    correlationId,
    version,
    payload,
    committed,
  }: ProductVariantCreatedEventParams) {
    this.createdAt = createdAt;
    this.correlationId = correlationId;
    this.aggregateId = aggregateId;
    this.version = version;
    this.payload = payload;
    this.committed = committed;
  }
}

type ProductVariantArchivedEventPayload = Record<string, never>;

type ProductVariantArchivedEventType = DomainEvent<
  "ProductVariantArchived",
  ProductVariantArchivedEventPayload
>;

type ProductVariantArchivedEventParams = {
  createdAt: Date;
  aggregateId: string;
  correlationId: string;
  version: number;
  payload: ProductVariantArchivedEventPayload;
  committed: boolean;
};

export class ProductVariantArchivedEvent
  implements ProductVariantArchivedEventType
{
  createdAt: Date;
  eventName = "ProductVariantArchived" as const;
  correlationId: string;
  aggregateId: string;
  version: number;
  payload: ProductVariantArchivedEventPayload;
  committed: boolean;

  constructor({
    createdAt,
    aggregateId,
    correlationId,
    version,
    payload,
    committed,
  }: ProductVariantArchivedEventParams) {
    this.createdAt = createdAt;
    this.correlationId = correlationId;
    this.aggregateId = aggregateId;
    this.version = version;
    this.payload = payload;
    this.committed = committed;
  }
}
