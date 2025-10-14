export type Success<T> = T extends void
  ? { readonly success: true }
  : { readonly success: true; readonly data: T };

export type Failure<E = Error> = {
  readonly success: false;
  readonly error: E;
};

export type Result<T, E = Error> = Success<T> | Failure<E>;

export const ok = <T = void>(data?: T): Success<T> => {
  if (data === undefined) {
    return { success: true } as Success<T>;
  }
  return { success: true, data } as Success<T>;
};

export const fail = <E = Error>(error: E): Failure<E> => ({
  success: false,
  error,
});

export const tryCatch = async <T = void, E = Error>(
  fn: () => Promise<T> | T,
  onError?: (error: unknown) => E
): Promise<Result<T, E>> => {
  try {
    const data = await fn();
    return ok(data);
  } catch (error) {
    const err = onError ? onError(error) : (error as E);
    return fail(err);
  }
};
