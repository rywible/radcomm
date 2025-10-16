import type { DomainEvent } from "../_base/domainEvent";

type SkuIndexCreatedEventPayload = Record<string, never>;

type SkuIndexCreatedEventType = DomainEvent<
  "SkuIndexCreated",
  SkuIndexCreatedEventPayload
>;

type SkuIndexCreatedEventParams = {
  createdAt: Date;
  aggregateId: string;
  correlationId: string;
  version: number;
  payload: SkuIndexCreatedEventPayload;
  committed: boolean;
};

export class SkuIndexCreatedEvent implements SkuIndexCreatedEventType {
  createdAt: Date;
  eventName = "SkuIndexCreated" as const;
  correlationId: string;
  aggregateId: string;
  version: number;
  payload: SkuIndexCreatedEventPayload;
  committed: boolean;

  constructor({
    createdAt,
    aggregateId,
    correlationId,
    version,
    payload,
    committed,
  }: SkuIndexCreatedEventParams) {
    this.createdAt = createdAt;
    this.correlationId = correlationId;
    this.aggregateId = aggregateId;
    this.version = version;
    this.payload = payload;
    this.committed = committed;
  }
}

type SkuReservedEventPayload = Record<string, never>;

type SkuReservedEventType = DomainEvent<"SkuReserved", SkuReservedEventPayload>;

type SkuReservedEventParams = {
  createdAt: Date;
  aggregateId: string;
  correlationId: string;
  version: number;
  payload: SkuReservedEventPayload;
  committed: boolean;
};

export class SkuReservedEvent implements SkuReservedEventType {
  createdAt: Date;
  eventName = "SkuReserved" as const;
  correlationId: string;
  aggregateId: string;
  version: number;
  payload: SkuReservedEventPayload;
  committed: boolean;

  constructor({
    createdAt,
    aggregateId,
    correlationId,
    version,
    payload,
    committed,
  }: SkuReservedEventParams) {
    this.createdAt = createdAt;
    this.correlationId = correlationId;
    this.aggregateId = aggregateId;
    this.version = version;
    this.payload = payload;
    this.committed = committed;
  }
}

type SkuReleasedEventPayload = Record<string, never>;

type SkuReleasedEventType = DomainEvent<"SkuReleased", SkuReleasedEventPayload>;

type SkuReleasedEventParams = {
  createdAt: Date;
  aggregateId: string;
  correlationId: string;
  version: number;
  payload: SkuReleasedEventPayload;
  committed: boolean;
};

export class SkuReleasedEvent implements SkuReleasedEventType {
  createdAt: Date;
  eventName = "SkuReleased" as const;
  correlationId: string;
  aggregateId: string;
  version: number;
  payload: SkuReleasedEventPayload;
  committed: boolean;

  constructor({
    createdAt,
    aggregateId,
    correlationId,
    version,
    payload,
    committed,
  }: SkuReleasedEventParams) {
    this.createdAt = createdAt;
    this.correlationId = correlationId;
    this.aggregateId = aggregateId;
    this.version = version;
    this.payload = payload;
    this.committed = committed;
  }
}
