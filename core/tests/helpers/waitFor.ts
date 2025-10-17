export async function waitFor(
  condition: () => boolean | Promise<boolean>,
  options: {
    timeout?: number;
    interval?: number;
    timeoutMessage?: string;
  } = {}
): Promise<void> {
  const timeout = options.timeout || 5000;
  const interval = options.interval || 50;
  const timeoutMessage = options.timeoutMessage || "Condition not met in time";

  const startTime = Date.now();

  while (true) {
    const result = await condition();
    if (result) {
      return;
    }

    if (Date.now() - startTime > timeout) {
      throw new Error(timeoutMessage);
    }

    await new Promise((resolve) => setTimeout(resolve, interval));
  }
}

export async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
