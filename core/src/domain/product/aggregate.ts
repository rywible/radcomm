import { ProductVariant } from "./entities";
import type { DomainEvent } from "../_base/domainEvent";
import {
  ProductCreatedEvent,
  ProductVariantAddedEvent,
  ProductDeletedEvent,
} from "./events";

type ProductAggregateParams = {
  id: string;
  correlationId: string;
  createdAt: Date;
  title: string;
  description: string;
  slug: string;
  collectionIds: string[];
  variants: ProductVariant[];
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
  variants: ProductVariant[];
};

export class ProductAggregate {
  private id: string;
  private correlationId: string;
  private createdAt: Date;
  private title: string;
  private description: string;
  private slug: string;
  private collectionIds: string[];
  private variants: ProductVariant[];
  private deleted: boolean = false;
  public events: DomainEvent<string, Record<string, unknown>>[];

  constructor({
    id,
    correlationId,
    createdAt,
    title,
    description,
    slug,
    collectionIds,
    variants,
    events,
  }: ProductAggregateParams) {
    this.id = id;
    this.correlationId = correlationId;
    this.createdAt = createdAt;
    this.title = title;
    this.description = description;
    this.slug = slug;
    this.collectionIds = collectionIds;
    this.variants = variants;
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
    variants,
  }: CreateProductAggregateParams) {
    if (variants.length === 0) {
      throw new Error("Product must have at least one variant");
    }
    if (collectionIds.length === 0) {
      throw new Error("Product must have at least one collection");
    }
    const events = [];
    const productCreatedEvent = new ProductCreatedEvent({
      createdAt,
      correlationId,
      aggregateId: id,
      version: 0,
      payload: {
        title,
        description,
        slug,
        collectionIds,
        variants,
      },
      committed: false,
    });
    events.push(productCreatedEvent);
    for (let i = 0; i < variants.length; i++) {
      const variant = variants[i]!;
      const productVariantAddedEvent = new ProductVariantAddedEvent({
        createdAt,
        correlationId,
        aggregateId: id,
        version: 1 + i,
        payload: { variant },
        committed: false,
      });
      events.push(productVariantAddedEvent);
    }
    return new ProductAggregate({
      id,
      correlationId,
      createdAt,
      title,
      description,
      slug,
      collectionIds,
      variants,
      events,
    });
  }

  apply(event: DomainEvent<string, Record<string, unknown>>) {
    switch (event.eventName) {
      case "ProductVariantAdded":
        const productVariantAddedEvent = event as ProductVariantAddedEvent;
        this.variants.push(productVariantAddedEvent.payload.variant);
        break;
      case "ProductDeleted":
        this.deleted = true;
        break;
      default:
        throw new Error(`Unknown event type: ${event.eventName}`);
    }
    this.events.push(event);
  }

  delete() {
    if (this.deleted) {
      throw new Error("Product is already deleted");
    }
    const latestVersion = this.events.length - 1;
    const productDeletedEvent = new ProductDeletedEvent({
      createdAt: new Date(),
      correlationId: this.correlationId,
      aggregateId: this.id,
      version: latestVersion + 1,
      payload: {},
      committed: false,
    });
    this.apply(productDeletedEvent);
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
      variants: [],
      events: [firstEvent],
    });

    for (let i = 1; i < events.length; i++) {
      productAggregate.apply(events[i]!);
    }

    return productAggregate;
  }
}
