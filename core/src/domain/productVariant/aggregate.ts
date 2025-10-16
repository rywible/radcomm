import type { DomainEvent } from "../_base/domainEvent";
import {
  ProductVariantCreatedEvent,
  ProductVariantArchivedEvent,
} from "./events";

type ProductVariantAggregateParams = {
  id: string;
  correlationId: string;
  createdAt: Date;
  productId: string;
  sku: string;
  priceCents: number;
  imageUrl: string;
  size: string;
  color: string;
  version: number;
  events: DomainEvent<string, Record<string, unknown>>[];
};

type CreateProductVariantAggregateParams = {
  id: string;
  correlationId: string;
  createdAt: Date;
  productId: string;
  sku: string;
  priceCents: number;
  imageUrl: string;
  size: string;
  color: string;
};

export class ProductVariantAggregate {
  private id: string;
  private correlationId: string;
  private createdAt: Date;
  private productId: string;
  private sku: string;
  private priceCents: number;
  private imageUrl: string;
  private size: string;
  private color: string;
  private archived: boolean = false;
  public version: number = 0;
  public events: DomainEvent<string, Record<string, unknown>>[];
  public uncommittedEvents: DomainEvent<string, Record<string, unknown>>[] = [];

  constructor({
    id,
    correlationId,
    createdAt,
    productId,
    sku,
    priceCents,
    imageUrl,
    size,
    color,
    version = 0,
    events,
  }: ProductVariantAggregateParams) {
    this.id = id;
    this.correlationId = correlationId;
    this.createdAt = createdAt;
    this.productId = productId;
    this.sku = sku;
    this.priceCents = priceCents;
    this.imageUrl = imageUrl;
    this.size = size;
    this.color = color;
    this.version = version;
    this.events = events;
  }

  static create({
    id,
    correlationId,
    createdAt,
    productId,
    sku,
    priceCents,
    imageUrl,
    size,
    color,
  }: CreateProductVariantAggregateParams) {
    if (priceCents <= 0) {
      throw new Error("Price must be positive");
    }

    const productVariantAggregate = new ProductVariantAggregate({
      id,
      correlationId,
      createdAt,
      productId,
      sku,
      priceCents,
      imageUrl,
      size,
      color,
      version: 0,
      events: [],
    });

    const productVariantCreatedEvent = new ProductVariantCreatedEvent({
      createdAt,
      correlationId,
      aggregateId: id,
      version: 0,
      payload: {
        productId,
        sku,
        priceCents,
        imageUrl,
        size,
        color,
      },
      committed: false,
    });

    productVariantAggregate.uncommittedEvents.push(productVariantCreatedEvent);
    return productVariantAggregate;
  }

  archive() {
    if (this.archived) {
      throw new Error("Product variant is already archived");
    }

    this.archived = true;
    this.version++;

    const event = new ProductVariantArchivedEvent({
      createdAt: new Date(),
      correlationId: this.correlationId,
      aggregateId: this.id,
      version: this.version,
      payload: {
        sku: this.sku,
      },
      committed: false,
    });
    this.uncommittedEvents.push(event);
  }

  apply(event: DomainEvent<string, Record<string, unknown>>) {
    switch (event.eventName) {
      case "ProductVariantArchived":
        this.archived = true;
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

    const firstEvent = events[0]! as ProductVariantCreatedEvent;
    if (firstEvent.eventName !== "ProductVariantCreated") {
      throw new Error("First event must be ProductVariantCreated");
    }

    const productVariantAggregate = new ProductVariantAggregate({
      id: firstEvent.aggregateId,
      correlationId: firstEvent.correlationId,
      createdAt: firstEvent.createdAt,
      productId: firstEvent.payload.productId,
      sku: firstEvent.payload.sku,
      priceCents: firstEvent.payload.priceCents,
      imageUrl: firstEvent.payload.imageUrl,
      size: firstEvent.payload.size,
      color: firstEvent.payload.color,
      version: 0,
      events: [firstEvent],
    });

    for (let i = 1; i < events.length; i++) {
      productVariantAggregate.apply(events[i]!);
    }

    return productVariantAggregate;
  }

  getId() {
    return this.id;
  }

  getProductId() {
    return this.productId;
  }

  getSku() {
    return this.sku;
  }

  getPriceCents() {
    return this.priceCents;
  }

  getImageUrl() {
    return this.imageUrl;
  }

  getSize() {
    return this.size;
  }

  getColor() {
    return this.color;
  }

  isArchived() {
    return this.archived;
  }
}
