# KVFS

[![deno doc](https://doc.deno.land/badge.svg)](https://deno.land/x/kvfs/mod.ts)

Library for saving bigger files in Deno KV

## Example

```ts
import { KvFs } from "https://deno.land/x/kvfs/mod.ts";

const fs = new KvFs(db);

const data = new UInt8Array([1, 2, 3]);

await fs.set(["fs", "file1"], data);

const entry = await fs.get(["fs", "file1"]);

assertEquals(entry.value, data);
```

See more examples in [tests](./mod.test.ts).
