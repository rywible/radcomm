import type { DomainEvent } from "../_base/domainEvent";

type CollectionCreatedEventPayload = {
  name: string;
  slug: string;
  description: string;
  productIds: string[];
};

type CollectionCreatedEventType = DomainEvent<
  "CollectionCreated",
  CollectionCreatedEventPayload
>;

type CollectionCreatedEventParams = {
  createdAt: Date;
  aggregateId: string;
  correlationId: string;
  version: number;
  payload: CollectionCreatedEventPayload;
  committed: boolean;
};

export class CollectionCreatedEvent implements CollectionCreatedEventType {
  createdAt: Date;
  eventName = "CollectionCreated" as const;
  correlationId: string;
  aggregateId: string;
  version: number;
  payload: CollectionCreatedEventPayload;
  committed: boolean;

  constructor({
    createdAt,
    aggregateId,
    correlationId,
    version,
    payload,
    committed,
  }: CollectionCreatedEventParams) {
    this.createdAt = createdAt;
    this.correlationId = correlationId;
    this.aggregateId = aggregateId;
    this.version = version;
    this.payload = payload;
    this.committed = committed;
  }
}

type ProductLinkedEventPayload = {
  productId: string;
};

type ProductLinkedEventType = DomainEvent<
  "ProductLinked",
  ProductLinkedEventPayload
>;

type ProductLinkedEventParams = {
  createdAt: Date;
  aggregateId: string;
  correlationId: string;
  version: number;
  payload: ProductLinkedEventPayload;
  committed: boolean;
};

export class ProductLinkedEvent implements ProductLinkedEventType {
  createdAt: Date;
  eventName = "ProductLinked" as const;
  correlationId: string;
  aggregateId: string;
  version: number;
  payload: ProductLinkedEventPayload;
  committed: boolean;

  constructor({
    createdAt,
    aggregateId,
    correlationId,
    version,
    payload,
    committed,
  }: ProductLinkedEventParams) {
    this.createdAt = createdAt;
    this.correlationId = correlationId;
    this.aggregateId = aggregateId;
    this.version = version;
    this.payload = payload;
    this.committed = committed;
  }
}

type CollectionArchivedEventPayload = Record<string, never>;

type CollectionArchivedEventType = DomainEvent<
  "CollectionArchived",
  CollectionArchivedEventPayload
>;

type CollectionArchivedEventParams = {
  createdAt: Date;
  aggregateId: string;
  correlationId: string;
  version: number;
  payload: CollectionArchivedEventPayload;
  committed: boolean;
};

export class CollectionArchivedEvent implements CollectionArchivedEventType {
  createdAt: Date;
  eventName = "CollectionArchived" as const;
  correlationId: string;
  aggregateId: string;
  version: number;
  payload: CollectionArchivedEventPayload;
  committed: boolean;

  constructor({
    createdAt,
    aggregateId,
    correlationId,
    version,
    payload,
    committed,
  }: CollectionArchivedEventParams) {
    this.createdAt = createdAt;
    this.correlationId = correlationId;
    this.aggregateId = aggregateId;
    this.version = version;
    this.payload = payload;
    this.committed = committed;
  }
}
