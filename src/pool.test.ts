import { assertEquals, assertRejects, deferred } from "./deps.ts";
import { GenericPool } from "./pool.ts";

Deno.test({
  name: "Should acquire and release a resource",
  async fn() {
    const factory = {
      create: () => Promise.resolve("resource"),
      destroy: () => Promise.resolve(),
    };

    const pool = new GenericPool(factory, {
      max: 2,
      min: 0,
      ttl: 1000,
      acquireTimeout: 500,
      onError: (error) => console.error(error),
    });

    const resource = await pool.acquire();
    assertEquals(resource, "resource");

    pool.release(resource);
    await pool.drain();
  },
});

Deno.test({
  name: "Should timeout if resource cannot be acquired in time",
  async fn() {
    const factory = {
      create: async () => {
        await new Promise((resolve) => setTimeout(resolve, 500));
        return "resource";
      },
      destroy: () => Promise.resolve(),
    };

    const pool = new GenericPool(factory, {
      max: 1,
      min: 0,
      ttl: 1000,
      acquireTimeout: 100,
      onError: (error) => console.error(error),
    });

    await assertRejects(
      async () => {
        await pool.acquire();
      },
      Error,
      "Resource acquisition timeout",
    );

    await pool.drain();
  },
});

Deno.test("Should destroy resources after TTL", async () => {
  const destroyedDeferred = deferred<string>();

  const factory = {
    create: () => Promise.resolve("resource"),
    destroy: async (resource: string) => {
      console.log("HERE! destroy", resource);
      destroyedDeferred.resolve(resource);
      await destroyedDeferred;
    },
  };

  const pool = new GenericPool(factory, {
    max: 1,
    min: 0,
    ttl: 100,
    acquireTimeout: 500,
    onError: (error) => {
      throw error;
    },
  });

  const resource = await pool.acquire();
  pool.release(resource);

  console.log("HERE one", destroyedDeferred);
  try {
    const destroyedResource = await destroyedDeferred;
    assertEquals(destroyedResource, "resource");
    await pool.drain();
  } catch (e) {
    console.log(e);
    throw e;
  }
  console.log("HERE two");
});

// Deno.test("Should throw when trying to acquire resource while draining", async () => {
//   const factory = {
//     create: async () => "resource",
//     destroy: async (resource: string) => {},
//   };

//   const pool = new GenericPool(factory, {
//     max: 1,
//     min: 0,
//     ttl: 1000,
//     acquireTimeout: 500,
//     onError: (error) => console.error(error),
//   });

//   await pool.drain();

//   assertRejects(
//     async () => {
//       await pool.acquire();
//     },
//     Error,
//     "Pool is draining. Cannot acquire new resources.",
//   );
// });

// Deno.test("Should drain all resources", async () => {
//   const destroyCounter: { count: number } = { count: 0 };
//   const factory = {
//     create: async () => "resource",
//     destroy: async (resource: string) => {
//       destroyCounter.count++;
//     },
//   };

//   const pool = new GenericPool(factory, {
//     max: 3,
//     min: 0,
//     ttl: 1000,
//     acquireTimeout: 500,
//     onError: (error) => console.error(error),
//   });

//   const resources = await Promise.all([pool.acquire(), pool.acquire(), pool.acquire()]);
//   resources.forEach((r) => pool.release(r));

//   await pool.drain();

//   assertEquals(destroyCounter.count, 3);
// });

// Deno.test("Should clear all resources without destroying them", async () => {
//   const destroyCounter: { count: number } = { count: 0 };
//   const factory = {
//     create: async () => "resource",
//     destroy: async (resource: string) => {
//       destroyCounter.count++;
//     },
//   };

//   const pool = new GenericPool(factory, {
//     max: 3,
//     min: 0,
//     ttl: 1000,
//     acquireTimeout: 500,
//     onError: (error) => console.error(error),
//   });

//   const resources = await Promise.all([pool.acquire(), pool.acquire(), pool.acquire()]);
//   resources.forEach((r) => pool.release(r));

//   pool.clear();

//   assertEquals(destroyCounter.count, 0);
// });

// function assertRejects(arg0: () => Promise<void>, Error: ErrorConstructor, arg2: string) {
//   throw new Error("Function not implemented.");
// }
