import type { IntegrationEvent } from "../integrationEvents/_base";

class ExternalEffectHandlerResult {
  public success: boolean;
  public error?: string;

  constructor({ success, error }: { success: boolean; error?: string }) {
    this.success = success;
    if (error !== undefined) {
      this.error = error;
    }
  }
}

export class ExternalEffectHandler {
  constructor() {}

  private async routeEvent(
    event: IntegrationEvent<string, Record<string, unknown>>
  ) {
    switch (event.eventName) {
      case "product.created":
        break;
      case "product.archived":
        break;
      case "product_variant.created":
        break;
      case "product_variant.archived":
        break;
      case "collection.created":
        break;
      case "collection.archived":
        break;
      default:
        console.warn(`Unknown integration event: ${event.eventName}`);
        break;
    }
  }

  async handleIntegrationEvent(
    event: IntegrationEvent<string, Record<string, unknown>>
  ): Promise<ExternalEffectHandlerResult> {
    try {
      await this.routeEvent(event);
      return new ExternalEffectHandlerResult({ success: true });
    } catch (error) {
      return new ExternalEffectHandlerResult({
        success: false,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }
}
