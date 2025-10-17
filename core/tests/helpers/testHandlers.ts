import type { IntegrationEvent } from "../../src/integrationEvents/_base";
import { sleep } from "./waitFor";

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

export class MockProjectionHandlerPerfTesting {
  async handleIntegrationEvent(
    event: IntegrationEvent<string, Record<string, unknown>>
  ): Promise<ProjectionHandlerResult> {
    // Simulate database writes: 20-80ms with occasional spikes to 150ms
    const baseTime = 50; // 50ms base for DB operations
    const jitter = Math.random() * 60 - 30; // ±30ms jitter
    const spikeChance = Math.random() < 0.1; // 10% chance of spike
    const spikeTime = spikeChance ? Math.random() * 100 : 0; // 0-100ms extra spike

    await sleep(baseTime + jitter + spikeTime);
    return new ProjectionHandlerResult({ success: true });
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

export class MockExternalEffectHandlerPerfTesting {
  async handleIntegrationEvent(
    event: IntegrationEvent<string, Record<string, unknown>>
  ): Promise<ExternalEffectHandlerResult> {
    // Simulate Stripe API calls: 150-400ms with occasional tail latency spikes
    const baseTime = 250; // 250ms base for external API calls
    const jitter = Math.random() * 200 - 100; // ±100ms jitter
    const tailChance = Math.random() < 0.05; // 5% chance of tail latency
    const tailTime = tailChance ? Math.random() * 400 + 200 : 0; // 200-600ms tail spike

    await sleep(baseTime + jitter + tailTime);
    return new ExternalEffectHandlerResult({ success: true });
  }
}
