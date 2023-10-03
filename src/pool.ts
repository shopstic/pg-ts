type Factory<T> = {
  create: () => Promise<T>;
  destroy: (resource: T) => Promise<void>;
};

type PoolOptions = {
  max: number;
  min: number;
  ttl: number;
  acquireTimeout: number;
  onError: (error: Error) => void;
};

type ResourceWrapper<T> = {
  resource: T;
  lastUsed: number;
  timer: number;
};

type PendingCreation<T> = Promise<ResourceWrapper<T>> | null;

export class GenericPool<T> {
  private factory: Factory<T>;
  private options: PoolOptions;
  private pool: ResourceWrapper<T>[] = [];
  // deno-lint-ignore no-explicit-any
  private pendingRequests: Array<{ resolve: (v: any) => void; reject: (e: Error) => void; timer: number }> = [];
  private isDraining = false;
  private resourceToWrapper = new Map<T, ResourceWrapper<T>>();
  private pendingCreations: PendingCreation<T>[] = [];

  constructor(factory: Factory<T>, options: PoolOptions) {
    this.factory = factory;
    this.options = options;
  }

  private async createResource(): Promise<ResourceWrapper<T>> {
    let creation = (async () => {
      const resource = await this.factory.create();
      const wrapper: ResourceWrapper<T> = {
        resource,
        lastUsed: Date.now(),
        timer: 0,
      };
      this.resourceToWrapper.set(resource, wrapper);
      return wrapper;
    })();

    this.pendingCreations.push(creation);

    const wrapper = await creation;

    // Remove this creation promise from pendingCreations
    const index = this.pendingCreations.indexOf(creation);
    if (index !== -1) {
      this.pendingCreations.splice(index, 1);
    }

    return wrapper;
  }

  private async destroyResource(wrapper: ResourceWrapper<T>) {
    try {
      clearTimeout(wrapper.timer);
      await this.factory.destroy(wrapper.resource);
    } catch (error) {
      this.options.onError(error);
    }
  }

  private fulfillRequest(wrapper: ResourceWrapper<T>) {
    const request = this.pendingRequests.shift();
    if (request) {
      clearTimeout(request.timer);
      this.resourceToWrapper.set(wrapper.resource, wrapper); // <-- New line
      request.resolve(wrapper.resource);
    } else {
      this.pool.push(wrapper);
    }
  }
  public async acquire(acquireTimeoutOverride?: number): Promise<T> {
    if (this.isDraining) {
      throw new Error("Pool is draining. Cannot acquire new resources.");
    }

    const timeout = acquireTimeoutOverride ?? this.options.acquireTimeout;
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error("Resource acquisition timeout"));
      }, timeout) as unknown as number;

      this.pendingRequests.push({ resolve, reject, timer });
      this.processQueue().catch(this.options.onError);
    });
  }

  private async processQueue() {
    if (this.isDraining || this.pendingRequests.length === 0) return;

    while (this.pool.length > 0) {
      const wrapper = this.pool.shift()!;
      if (Date.now() - wrapper.lastUsed < this.options.ttl) {
        this.fulfillRequest(wrapper);
      } else {
        await this.destroyResource(wrapper);
      }
    }

    if (this.pool.length + this.pendingRequests.length <= this.options.max) {
      const wrapper = await this.createResource();
      this.fulfillRequest(wrapper);
    }
  }

  public release(resource: T) {
    const wrapper = this.resourceToWrapper.get(resource);
    if (wrapper) {
      wrapper.lastUsed = Date.now();
      this.resourceToWrapper.delete(resource);
      this.pool.push(wrapper);
      this.processQueue().catch(this.options.onError);
    } else {
      throw new Error("Resource not found in the pool");
    }
  }

  public async drain(): Promise<void> {
    if (this.isDraining) {
      throw new Error("Already draining");
    }
    this.isDraining = true;

    // Cancel any pending resource acquisitions
    for (const { reject, timer } of this.pendingRequests) {
      clearTimeout(timer);
      reject(new Error("Pool is draining. Cannot acquire new resources."));
    }
    this.pendingRequests.length = 0;

    // Await and destroy all pending resource creations
    await Promise.all(this.pendingCreations);

    // Destroy existing resources
    for (const wrapper of this.pool) {
      await this.factory.destroy(wrapper.resource);
    }
    this.pool.length = 0;
  }

  public clear() {
    this.pool.forEach((wrapper) => {
      clearTimeout(wrapper.timer);
    });
    this.pool = [];
  }
}
