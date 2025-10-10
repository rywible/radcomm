import type { DomainEvent } from "../_base/domainEvent";

type OrderRequestedEventPayload = {
  customerId: string;
  items: Array<{ sku: string; qty: number }>;
};

type OrderRequestedEventType = DomainEvent<
  "OrderRequested",
  OrderRequestedEventPayload
>;

type OrderRequestedEventParams = {
  id: string;
  createdAt: Date;
  aggregateId: string;
  correlationId: string;
  version: number;
  payload: OrderRequestedEventPayload;
};

export class OrderRequestedEvent implements OrderRequestedEventType {
  id: string;
  createdAt: Date;
  eventName = "OrderRequested" as const;
  correlationId: string;
  aggregateId: string;
  version: number;
  payload: OrderRequestedEventPayload;

  constructor({
    id,
    createdAt,
    aggregateId,
    correlationId,
    version,
    payload,
  }: OrderRequestedEventParams) {
    this.id = id;
    this.createdAt = createdAt;
    this.correlationId = correlationId;
    this.aggregateId = aggregateId;
    this.version = version;
    this.payload = payload;
  }
}
