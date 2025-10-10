import { UnitOfWork } from "../unitOfWork";
import { OrderRequestedEvent } from "../../domain/order/events";
import { v7 as uuidv7 } from "uuid";

type ExecuteParams = {
  orderId: string;
  customerId: string;
  items: Array<{ sku: string; qty: number }>;
  correlationId: string;
};

export class CreateOrderService {
  private unitOfWork: UnitOfWork;

  constructor(unitOfWork: UnitOfWork) {
    this.unitOfWork = unitOfWork;
  }

  async execute({ orderId, customerId, items, correlationId }: ExecuteParams) {
    await this.unitOfWork.withTransaction(async ({ eventRepository }) => {
      const existingEvents = await eventRepository.findByAggregateId(orderId);
      if (existingEvents.length > 0) {
        throw new Error(`Order with ID ${orderId} already exists.`);
      }
      const orderRequestedEvent = new OrderRequestedEvent({
        id: uuidv7(),
        createdAt: new Date(),
        aggregateId: orderId,
        correlationId,
        version: 1,
        payload: {
          customerId,
          items,
        },
      });
      await eventRepository.add(orderRequestedEvent);
    });
  }
}
