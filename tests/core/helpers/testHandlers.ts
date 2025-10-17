import type { IntegrationEvent } from "../../../core/src/integrationEvents/_base";

export class ProjectionHandlerResult {
  public success: boolean;
  public error?: string;

  constructor({ success, error }: { success: boolean; error?: string }) {
    this.success = success;
    if (error !== undefined) {
      this.error = error;
    }
  }
}

export class ExternalEffectHandlerResult {
  public success: boolean;
  public error?: string;

  constructor({ success, error }: { success: boolean; error?: string }) {
    this.success = success;
    if (error !== undefined) {
      this.error = error;
    }
  }
}

export class MockProjectionHandler {
  public callCount = 0;
  public calledWith: IntegrationEvent<string, Record<string, unknown>>[] = [];
  private shouldFail = false;
  private errorMessage = "Projection handler failed";

  async handleIntegrationEvent(
    event: IntegrationEvent<string, Record<string, unknown>>
  ): Promise<ProjectionHandlerResult> {
    this.callCount++;
    this.calledWith.push(event);

    if (this.shouldFail) {
      return new ProjectionHandlerResult({
        success: false,
        error: this.errorMessage,
      });
    }

    return new ProjectionHandlerResult({ success: true });
  }

  setFailure(shouldFail: boolean, errorMessage?: string) {
    this.shouldFail = shouldFail;
    if (errorMessage) {
      this.errorMessage = errorMessage;
    }
  }

  reset() {
    this.callCount = 0;
    this.calledWith = [];
    this.shouldFail = false;
  }
}

export class MockExternalEffectHandler {
  public callCount = 0;
  public calledWith: IntegrationEvent<string, Record<string, unknown>>[] = [];
  private shouldFail = false;
  private errorMessage = "External effect handler failed";

  async handleIntegrationEvent(
    event: IntegrationEvent<string, Record<string, unknown>>
  ): Promise<ExternalEffectHandlerResult> {
    this.callCount++;
    this.calledWith.push(event);

    if (this.shouldFail) {
      return new ExternalEffectHandlerResult({
        success: false,
        error: this.errorMessage,
      });
    }

    return new ExternalEffectHandlerResult({ success: true });
  }

  setFailure(shouldFail: boolean, errorMessage?: string) {
    this.shouldFail = shouldFail;
    if (errorMessage) {
      this.errorMessage = errorMessage;
    }
  }

  reset() {
    this.callCount = 0;
    this.calledWith = [];
    this.shouldFail = false;
  }
}

