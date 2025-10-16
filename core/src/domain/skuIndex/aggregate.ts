import type { DomainEvent } from "../_base/domainEvent";
import {
  SkuIndexCreatedEvent,
  SkuReservedEvent,
  SkuReleasedEvent,
} from "./events";

type SkuIndexAggregateParams = {
  id: string;
  correlationId: string;
  createdAt: Date;
  reserved: boolean;
  version: number;
  events: DomainEvent<string, Record<string, unknown>>[];
};

type CreateSkuIndexAggregateParams = {
  id: string;
  correlationId: string;
  createdAt: Date;
};

export class SkuIndexAggregate {
  private id: string;
  private correlationId: string;
  private createdAt: Date;
  private reserved: boolean = false;
  public version: number = 0;
  public events: DomainEvent<string, Record<string, unknown>>[];
  public uncommittedEvents: DomainEvent<string, Record<string, unknown>>[] = [];

  constructor({
    id,
    correlationId,
    createdAt,
    reserved,
    version = 0,
    events,
  }: SkuIndexAggregateParams) {
    this.id = id;
    this.correlationId = correlationId;
    this.createdAt = createdAt;
    this.reserved = reserved;
    this.version = version;
    this.events = events;
  }

  static create({
    id,
    correlationId,
    createdAt,
  }: CreateSkuIndexAggregateParams) {
    const skuIndexAggregate = new SkuIndexAggregate({
      id,
      correlationId,
      createdAt,
      reserved: true,
      version: 0,
      events: [],
    });

    const skuIndexCreatedEvent = new SkuIndexCreatedEvent({
      createdAt,
      correlationId,
      aggregateId: id,
      version: 0,
      payload: {},
      committed: false,
    });

    skuIndexAggregate.uncommittedEvents.push(skuIndexCreatedEvent);
    return skuIndexAggregate;
  }

  reserve() {
    if (this.reserved) {
      throw new Error("SKU is already reserved");
    }

    this.reserved = true;
    this.version++;

    const event = new SkuReservedEvent({
      createdAt: new Date(),
      correlationId: this.correlationId,
      aggregateId: this.id,
      version: this.version,
      payload: {},
      committed: false,
    });
    this.uncommittedEvents.push(event);
  }

  release() {
    if (!this.reserved) {
      throw new Error("SKU is not reserved");
    }

    this.reserved = false;
    this.version++;

    const event = new SkuReleasedEvent({
      createdAt: new Date(),
      correlationId: this.correlationId,
      aggregateId: this.id,
      version: this.version,
      payload: {},
      committed: false,
    });
    this.uncommittedEvents.push(event);
  }

  apply(event: DomainEvent<string, Record<string, unknown>>) {
    switch (event.eventName) {
      case "SkuReserved":
        this.reserved = true;
        break;
      case "SkuReleased":
        this.reserved = false;
        break;
      default:
        throw new Error(`Unknown event type: ${event.eventName}`);
    }
    this.version++;
    this.events.push(event);
  }

  static loadFromHistory(
    events: DomainEvent<string, Record<string, unknown>>[]
  ) {
    if (events.length === 0) {
      throw new Error("Cannot load aggregate from empty event history");
    }

    const firstEvent = events[0]! as SkuIndexCreatedEvent;
    if (firstEvent.eventName !== "SkuIndexCreated") {
      throw new Error("First event must be SkuIndexCreated");
    }

    const skuIndexAggregate = new SkuIndexAggregate({
      id: firstEvent.aggregateId,
      correlationId: firstEvent.correlationId,
      createdAt: firstEvent.createdAt,
      reserved: true,
      version: 0,
      events: [firstEvent],
    });

    for (let i = 1; i < events.length; i++) {
      skuIndexAggregate.apply(events[i]!);
    }

    return skuIndexAggregate;
  }

  getId() {
    return this.id;
  }

  isReserved() {
    return this.reserved;
  }
}
