import type { DomainEvent } from "../_base/domainEvent";
import {
  ProductCreatedEvent,
  ProductVariantLinkedEvent,
  ProductArchivedEvent,
} from "./events";

type ProductAggregateParams = {
  id: string;
  correlationId: string;
  createdAt: Date;
  title: string;
  description: string;
  slug: string;
  collectionIds: string[];
  variantIds: string[];
  version: number;
  events: DomainEvent<string, Record<string, unknown>>[];
};

type CreateProductAggregateParams = {
  id: string;
  correlationId: string;
  createdAt: Date;
  title: string;
  description: string;
  slug: string;
  collectionIds: string[];
  variantIds: string[];
};

export class ProductAggregate {
  private id: string;
  private correlationId: string;
  private createdAt: Date;
  private title: string;
  private description: string;
  private slug: string;
  private collectionIds: string[];
  private variantIds: string[];
  private archived: boolean = false;
  public version: number = 0;
  public events: DomainEvent<string, Record<string, unknown>>[];
  public uncommittedEvents: DomainEvent<string, Record<string, unknown>>[] = [];

  constructor({
    id,
    correlationId,
    createdAt,
    title,
    description,
    slug,
    collectionIds,
    variantIds,
    version = 0,
    events,
  }: ProductAggregateParams) {
    this.id = id;
    this.correlationId = correlationId;
    this.createdAt = createdAt;
    this.title = title;
    this.description = description;
    this.slug = slug;
    this.collectionIds = collectionIds;
    this.variantIds = variantIds;
    this.version = version;
    this.events = events;
  }

  static create({
    id,
    correlationId,
    createdAt,
    title,
    description,
    slug,
    collectionIds,
    variantIds,
  }: CreateProductAggregateParams) {
    if (variantIds.length === 0) {
      throw new Error("Product must have at least one variant");
    }
    if (collectionIds.length === 0) {
      throw new Error("Product must have at least one collection");
    }
    const productAggregate = new ProductAggregate({
      id,
      correlationId,
      createdAt,
      title,
      description,
      slug,
      collectionIds,
      variantIds,
      version: 0,
      events: [],
    });
    const productCreatedEvent = new ProductCreatedEvent({
      createdAt,
      correlationId,
      aggregateId: id,
      version: 0,
      payload: { title, description, slug, collectionIds, variantIds },
      committed: false,
    });
    productAggregate.uncommittedEvents.push(productCreatedEvent);
    return productAggregate;
  }

  apply(event: DomainEvent<string, Record<string, unknown>>) {
    switch (event.eventName) {
      case "ProductVariantLinked":
        const productVariantLinkedEvent = event as ProductVariantLinkedEvent;
        this.variantIds.push(productVariantLinkedEvent.payload.variantId);
        break;
      case "ProductArchived":
        this.archived = true;
        break;
      default:
        throw new Error(`Unknown event type: ${event.eventName}`);
    }
    this.version++;
    this.events.push(event);
  }

  // TODO: Implement when adding dynamic variant linking
  // linkVariant(variantId: string) {
  //   this.version++;
  //   const event = new ProductVariantLinkedEvent({
  //     createdAt: new Date(),
  //     correlationId: this.correlationId,
  //     aggregateId: this.id,
  //     version: this.version,
  //     payload: { variantId },
  //     committed: false,
  //   });
  //   this.variantIds.push(variantId);
  //   this.events.push(event);
  //   this.uncommittedEvents.push(event);
  // }

  archive() {
    if (this.archived) {
      throw new Error("Product is already archived");
    }
    this.archived = true;
    this.version++;
    const event = new ProductArchivedEvent({
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

    const firstEvent = events[0]! as ProductCreatedEvent;
    if (firstEvent.eventName !== "ProductCreated") {
      throw new Error("First event must be ProductCreated");
    }

    const productAggregate = new ProductAggregate({
      id: firstEvent.aggregateId,
      correlationId: firstEvent.correlationId,
      createdAt: firstEvent.createdAt,
      title: firstEvent.payload.title,
      description: firstEvent.payload.description,
      slug: firstEvent.payload.slug,
      collectionIds: firstEvent.payload.collectionIds,
      variantIds: [],
      version: 0,
      events: [firstEvent],
    });

    for (let i = 1; i < events.length; i++) {
      productAggregate.apply(events[i]!);
    }

    return productAggregate;
  }

  getId() {
    return this.id;
  }

  getVariantIds() {
    return this.variantIds;
  }

  getTitle() {
    return this.title;
  }

  getDescription() {
    return this.description;
  }

  getSlug() {
    return this.slug;
  }

  getCollectionIds() {
    return this.collectionIds;
  }

  isArchived() {
    return this.archived;
  }
}
