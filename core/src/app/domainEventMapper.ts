import type { DomainEvent } from "@core/domain/_base/domainEvent";
import type { IntegrationEvent } from "@core/integrationEvents/_base";
import {
  ProductCreatedEvent,
  ProductArchivedEvent,
} from "@core/domain/product/events";
import {
  ProductVariantCreatedEvent,
  ProductVariantArchivedEvent,
} from "@core/domain/productVariant/events";
import {
  CollectionCreatedEvent,
  CollectionArchivedEvent,
} from "@core/domain/collection/events";
import {
  ProductCreatedIntegrationEvent,
  ProductArchivedIntegrationEvent,
} from "@core/integrationEvents/product";
import {
  ProductVariantCreatedIntegrationEvent,
  ProductVariantArchivedIntegrationEvent,
} from "@core/integrationEvents/productVariant";
import {
  CollectionCreatedIntegrationEvent,
  CollectionArchivedIntegrationEvent,
} from "@core/integrationEvents/collection";
import { randomUUID } from "crypto";

export class DomainEventMapper {
  toIntegrationEvents(
    domainEvent: DomainEvent<string, Record<string, unknown>>
  ): IntegrationEvent<string, Record<string, unknown>>[] {
    switch (domainEvent.eventName) {
      case "ProductCreated": {
        const event = domainEvent as ProductCreatedEvent;
        return [
          new ProductCreatedIntegrationEvent({
            eventId: randomUUID(),
            occurredAt: event.createdAt,
            correlationId: event.correlationId,
            payload: {
              productId: event.aggregateId,
              title: event.payload.title,
              description: event.payload.description,
              slug: event.payload.slug,
              collectionIds: event.payload.collectionIds,
              variantIds: event.payload.variantIds,
            },
          }),
        ];
      }

      case "ProductArchived": {
        const event = domainEvent as ProductArchivedEvent;
        return [
          new ProductArchivedIntegrationEvent({
            eventId: randomUUID(),
            occurredAt: event.createdAt,
            correlationId: event.correlationId,
            payload: {
              productId: event.aggregateId,
            },
          }),
        ];
      }

      case "ProductVariantCreated": {
        const event = domainEvent as ProductVariantCreatedEvent;
        return [
          new ProductVariantCreatedIntegrationEvent({
            eventId: randomUUID(),
            occurredAt: event.createdAt,
            correlationId: event.correlationId,
            payload: {
              variantId: event.aggregateId,
              productId: event.payload.productId,
              sku: event.payload.sku,
              priceUsd: (event.payload.priceCents / 100).toFixed(2),
              imageUrl: event.payload.imageUrl,
              attributes: {
                size: event.payload.size,
                color: event.payload.color,
              },
            },
          }),
        ];
      }

      case "ProductVariantArchived": {
        const event = domainEvent as ProductVariantArchivedEvent;
        return [
          new ProductVariantArchivedIntegrationEvent({
            eventId: randomUUID(),
            occurredAt: event.createdAt,
            correlationId: event.correlationId,
            payload: {
              variantId: event.aggregateId,
            },
          }),
        ];
      }

      case "CollectionCreated": {
        const event = domainEvent as CollectionCreatedEvent;
        return [
          new CollectionCreatedIntegrationEvent({
            eventId: randomUUID(),
            occurredAt: event.createdAt,
            correlationId: event.correlationId,
            payload: {
              collectionId: event.aggregateId,
              name: event.payload.name,
              description: event.payload.description,
              slug: event.payload.slug,
              productIds: event.payload.productIds,
            },
          }),
        ];
      }

      case "CollectionArchived": {
        const event = domainEvent as CollectionArchivedEvent;
        return [
          new CollectionArchivedIntegrationEvent({
            eventId: randomUUID(),
            occurredAt: event.createdAt,
            correlationId: event.correlationId,
            payload: {
              collectionId: event.aggregateId,
            },
          }),
        ];
      }

      // Internal events that don't produce integration events
      case "ProductVariantLinked":
      case "ProductLinked":
      case "SkuIndexCreated":
      case "SkuReserved":
      case "SkuReleased":
        return [];

      default:
        // Unknown event - log or throw depending on your preference
        console.warn(
          `No integration event mapping for: ${domainEvent.eventName}`
        );
        return [];
    }
  }
}
