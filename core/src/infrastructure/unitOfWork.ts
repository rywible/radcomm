import { EventRepository, OutboxRepository } from "./repositories";
import type { DB } from "./postgres";

type TransactionalDb = Pick<DB, "transaction">;

export class UnitOfWork {
  private db: TransactionalDb;
  private eventRepositoryFactory: typeof EventRepository;
  private outboxRepositoryFactory: typeof OutboxRepository;
  constructor(
    db: TransactionalDb,
    eventRepositoryFactory: typeof EventRepository,
    outboxRepositoryFactory: typeof OutboxRepository
  ) {
    this.db = db;
    this.eventRepositoryFactory = eventRepositoryFactory;
    this.outboxRepositoryFactory = outboxRepositoryFactory;
  }

  async withTransaction<T>(
    work: (context: {
      eventRepository: EventRepository;
      outboxRepository: OutboxRepository;
    }) => Promise<T>
  ): Promise<T> {
    return this.db.transaction(async (tx) => {
      const eventRepository = new this.eventRepositoryFactory(tx);
      const outboxRepository = new this.outboxRepositoryFactory(tx);
      const result = await work({
        eventRepository,
        outboxRepository,
      });
      return result;
    });
  }
}
