import { KvFs } from "./mod.ts";
import { assertEquals } from "https://deno.land/std@0.195.0/assert/assert_equals.ts";
import { assertRejects } from "https://deno.land/std@0.195.0/assert/assert_rejects.ts";
import { assert } from "https://deno.land/std@0.201.0/assert/assert.ts";
import { setup } from "https://deno.land/std@0.201.0/log/mod.ts";
import { ConsoleHandler } from "https://deno.land/std@0.201.0/log/handlers.ts";

setup({
  handlers: {
    console: new ConsoleHandler("DEBUG"),
  },
  loggers: {
    kvfs: { level: "DEBUG", handlers: ["console"] },
  },
});

const db = await Deno.openKv();

// a 2MB UInt8Array object where bytes are just consecutive numbers repeating
const data = new Uint8Array(2 * 1024 * 1024).map((_, i) => i % 256);

Deno.test("create and delete file", async () => {
  // init
  const fs = new KvFs(db);
  await fs.delete(["fs", "file1"]);

  // assert that regular set throws
  await assertRejects(() => db.set(["fs", "file1"], data));

  // initial get - should return null
  const get1Result = await fs.get(["fs", "file1"]);
  assertEquals(get1Result.key, ["fs", "file1"]);
  assert(get1Result.versionstamp == null);
  assert(get1Result.value == null);

  // set
  const setResult = await fs.set(["fs", "file1"], data);
  assertEquals(setResult.ok, true);
  assert(setResult.versionstamp != null);

  // get - should return the same data
  const get2Result = await fs.get(["fs", "file1"]);
  assertEquals(get2Result.key, ["fs", "file1"]);
  assertEquals(get2Result.versionstamp, setResult.versionstamp);
  const dataChunk1 = Array.from(data.slice(0, 128 * 1024));
  const dataChunk2 = Array.from(get2Result.value!.slice(0, 128 * 1024));
  assertEquals(dataChunk1, dataChunk2);

  // delete
  await fs.delete(["fs", "file1"]);
});

Deno.test("fail on missing chunk", async () => {
  // init
  const fs = new KvFs(db);
  await fs.delete(["fs", "file2"]);

  // set
  await fs.set(["fs", "file2"], data);

  // delete a chunk
  await db.delete(["fs", "file2", 3]);

  // get - should throw
  await assertRejects(() => fs.get(["fs", "file2"]));

  // delete
  await fs.delete(["fs", "file2"]);
});

Deno.test("fail on digest mismatch", async () => {
  // init
  const fs = new KvFs(db);
  await fs.delete(["fs", "file3"]);

  // set
  await fs.set(["fs", "file3"], data);

  // modify a chunk
  await db.set(["fs", "file3", 4], new Uint8Array([0, 0, 0, 0, 0, 0, 0]));

  // get - should throw
  await assertRejects(() => fs.get(["fs", "file3"]));

  // delete
  await fs.delete(["fs", "file3"]);
});
