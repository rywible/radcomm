import { EventRepository } from "../infrastructure/repository";
import type { DB, TX } from "../infrastructure/postgres";

type TransactionalDb = Pick<DB, "transaction">;

export class UnitOfWork {
  private db: TransactionalDb;
  private eventRepositoryFactory: typeof EventRepository;

  constructor(
    db: TransactionalDb,
    eventRepositoryFactory: typeof EventRepository
  ) {
    this.db = db;
    this.eventRepositoryFactory = eventRepositoryFactory;
  }

  async withTransaction<T>(
    work: (context: { eventRepository: EventRepository }) => Promise<T>
  ): Promise<T> {
    return this.db.transaction(async (tx) => {
      const eventRepository = new this.eventRepositoryFactory(tx);
      const result = await work({ eventRepository });
      return result;
    });
  }
}
