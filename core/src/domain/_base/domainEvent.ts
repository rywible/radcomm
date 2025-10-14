export type DomainEventPayload = Record<string, unknown>;

export interface DomainEvent<
  Name extends string,
  P extends DomainEventPayload,
> {
  createdAt: Date;
  eventName: Name;
  correlationId: string;
  aggregateId: string;
  version: number;
  payload: P;
  committed: boolean;
}
