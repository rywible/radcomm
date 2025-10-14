import type { DomainEvent } from "../_base/domainEvent";
import { ProductVariant } from "./entities";

type ProductCreatedEventPayload = {
  title: string;
  description: string;
  slug: string;
  collectionIds: string[];
  variants: ProductVariant[];
};

type ProductCreatedEventType = DomainEvent<
  "ProductCreated",
  ProductCreatedEventPayload
>;

type ProductCreatedEventParams = {
  createdAt: Date;
  aggregateId: string;
  correlationId: string;
  version: number;
  payload: ProductCreatedEventPayload;
  committed: boolean;
};

export class ProductCreatedEvent implements ProductCreatedEventType {
  createdAt: Date;
  eventName = "ProductCreated" as const;
  correlationId: string;
  aggregateId: string;
  version: number;
  payload: ProductCreatedEventPayload;
  committed: boolean;

  constructor({
    createdAt,
    aggregateId,
    correlationId,
    version,
    payload,
    committed,
  }: ProductCreatedEventParams) {
    this.createdAt = createdAt;
    this.correlationId = correlationId;
    this.aggregateId = aggregateId;
    this.version = version;
    this.payload = payload;
    this.committed = committed;
  }
}

type ProductVariantAddedEventPayload = {
  variant: ProductVariant;
};

type ProductVariantAddedEventType = DomainEvent<
  "ProductVariantAdded",
  ProductVariantAddedEventPayload
>;

type ProductVariantAddedEventParams = {
  createdAt: Date;
  aggregateId: string;
  correlationId: string;
  version: number;
  payload: ProductVariantAddedEventPayload;
  committed: boolean;
};

export class ProductVariantAddedEvent implements ProductVariantAddedEventType {
  createdAt: Date;
  eventName = "ProductVariantAdded" as const;
  correlationId: string;
  aggregateId: string;
  version: number;
  payload: ProductVariantAddedEventPayload;
  committed: boolean;

  constructor({
    createdAt,
    aggregateId,
    correlationId,
    version,
    payload,
    committed,
  }: ProductVariantAddedEventParams) {
    this.createdAt = createdAt;
    this.correlationId = correlationId;
    this.aggregateId = aggregateId;
    this.version = version;
    this.payload = payload;
    this.committed = committed;
  }
}

type ProductDeletedEventPayload = Record<string, never>;

type ProductDeletedEventType = DomainEvent<
  "ProductDeleted",
  ProductDeletedEventPayload
>;

type ProductDeletedEventParams = {
  createdAt: Date;
  aggregateId: string;
  correlationId: string;
  version: number;
  payload: ProductDeletedEventPayload;
  committed: boolean;
};

export class ProductDeletedEvent implements ProductDeletedEventType {
  createdAt: Date;
  eventName = "ProductDeleted" as const;
  correlationId: string;
  aggregateId: string;
  version: number;
  payload: ProductDeletedEventPayload;
  committed: boolean;

  constructor({
    createdAt,
    aggregateId,
    correlationId,
    version,
    payload,
    committed,
  }: ProductDeletedEventParams) {
    this.createdAt = createdAt;
    this.correlationId = correlationId;
    this.aggregateId = aggregateId;
    this.version = version;
    this.payload = payload;
    this.committed = committed;
  }
}
