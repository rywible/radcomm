import type { DomainEvent } from "../_base/domainEvent";
import {
  CollectionCreatedEvent,
  ProductLinkedEvent,
  CollectionArchivedEvent,
} from "./events";

type CollectionAggregateParams = {
  id: string;
  correlationId: string;
  createdAt: Date;
  name: string;
  description: string;
  slug: string;
  productIds: string[];
  version: number;
  events: DomainEvent<string, Record<string, unknown>>[];
};

type CreateCollectionAggregateParams = {
  id: string;
  correlationId: string;
  createdAt: Date;
  name: string;
  description: string;
  slug: string;
  productIds: string[];
};

export class CollectionAggregate {
  private id: string;
  private correlationId: string;
  private createdAt: Date;
  private name: string;
  private description: string;
  private slug: string;
  private productIds: string[];
  private archived: boolean = false;
  public version: number = 0;
  public events: DomainEvent<string, Record<string, unknown>>[];
  public uncommittedEvents: DomainEvent<string, Record<string, unknown>>[] = [];

  constructor({
    id,
    correlationId,
    createdAt,
    name,
    description,
    slug,
    productIds,
    version = 0,
    events,
  }: CollectionAggregateParams) {
    this.id = id;
    this.correlationId = correlationId;
    this.createdAt = createdAt;
    this.name = name;
    this.description = description;
    this.slug = slug;
    this.productIds = productIds;
    this.version = version;
    this.events = events;
  }

  static create({
    id,
    correlationId,
    createdAt,
    name,
    description,
    slug,
    productIds,
  }: CreateCollectionAggregateParams) {
    if (productIds.length === 0) {
      throw new Error("Collection must have at least one product");
    }
    const collectionAggregate = new CollectionAggregate({
      id,
      correlationId,
      createdAt,
      name,
      description,
      slug,
      productIds,
      version: 0,
      events: [],
    });
    const collectionCreatedEvent = new CollectionCreatedEvent({
      createdAt,
      correlationId,
      aggregateId: id,
      version: 0,
      payload: { name, description, slug, productIds },
      committed: false,
    });
    collectionAggregate.uncommittedEvents.push(collectionCreatedEvent);
    return collectionAggregate;
  }

  apply(event: DomainEvent<string, Record<string, unknown>>) {
    switch (event.eventName) {
      case "ProductLinked":
        const productLinkedEvent = event as ProductLinkedEvent;
        this.productIds.push(productLinkedEvent.payload.productId);
        break;
      case "CollectionArchived":
        this.archived = true;
        break;
      default:
        throw new Error(`Unknown event type: ${event.eventName}`);
    }
    this.version++;
    this.events.push(event);
  }

  // TODO: Implement when adding dynamic product linking
  // linkProduct(productId: string) {
  //   this.version++;
  //   const event = new ProductLinkedEvent({
  //     createdAt: new Date(),
  //     correlationId: this.correlationId,
  //     aggregateId: this.id,
  //     version: this.version,
  //     payload: { productId },
  //     committed: false,
  //   });
  //   this.productIds.push(productId);
  //   this.events.push(event);
  //   this.uncommittedEvents.push(event);
  // }

  archive() {
    if (this.archived) {
      throw new Error("Collection is already archived");
    }
    this.archived = true;
    this.version++;
    const event = new CollectionArchivedEvent({
      createdAt: new Date(),
      correlationId: this.correlationId,
      aggregateId: this.id,
      version: this.version,
      payload: {},
      committed: false,
    });
    this.uncommittedEvents.push(event);
  }

  static loadFromHistory(
    events: DomainEvent<string, Record<string, unknown>>[]
  ) {
    if (events.length === 0) {
      throw new Error("Cannot load aggregate from empty event history");
    }

    const firstEvent = events[0]! as CollectionCreatedEvent;
    if (firstEvent.eventName !== "CollectionCreated") {
      throw new Error("First event must be CollectionCreated");
    }

    const collectionAggregate = new CollectionAggregate({
      id: firstEvent.aggregateId,
      correlationId: firstEvent.correlationId,
      createdAt: firstEvent.createdAt,
      name: firstEvent.payload.name,
      description: firstEvent.payload.description,
      slug: firstEvent.payload.slug,
      productIds: [],
      version: 0,
      events: [firstEvent],
    });

    for (let i = 1; i < events.length; i++) {
      collectionAggregate.apply(events[i]!);
    }

    return collectionAggregate;
  }

  getId() {
    return this.id;
  }

  getProductIds() {
    return this.productIds;
  }

  getName() {
    return this.name;
  }

  getDescription() {
    return this.description;
  }

  getSlug() {
    return this.slug;
  }

  isArchived() {
    return this.archived;
  }
}
