import { concat, equals } from "https://deno.land/std@0.202.0/bytes/mod.ts";
import { getLogger } from "https://deno.land/std@0.201.0/log/mod.ts";

const logger = () => getLogger("kvfs");

const chunkSize = 64 * 1024;

/**
 * Metadata of a file stored in the database.
 */
export interface FileMetadata {
  digest: Uint8Array;
  chunkCount: number;
}

/**
 * A {@link Deno.Kv} wrapper that allows storing files bigger than 64kB.
 *
 * Each file is split into chunks and stored as separate sub-entries in the store.
 *
 * The main file entry contains {@link FileMetadata}.
 */
export class KvFs {
  /**
   * The underlying Kv database.
   */
  db: Deno.Kv;

  /**
   * Create KvFs wrapper.
   */
  constructor(db: Deno.Kv) {
    this.db = db;
  }

  /**
   * Store a file in the database.
   */
  async set(
    key: Deno.KvKey,
    fileData: Uint8Array,
    options: { expireIn?: number } = {},
  ): Promise<Deno.KvCommitResult> {
    // calculate digest of the value
    const digest = await crypto.subtle.digest("SHA-256", fileData);

    // split data into 64kB chunks and save them in sub-entries
    for (let i = 0; i * chunkSize < fileData.length; i++) {
      const chunkData = fileData.slice(i * chunkSize, (i + 1) * chunkSize);
      await this.db.set([...key, i], chunkData satisfies Uint8Array, options);
    }

    // save the main file entry
    const fileMetadata: FileMetadata = {
      digest: new Uint8Array(digest),
      chunkCount: Math.ceil(fileData.length / chunkSize),
    };

    logger().debug(
      `File saved ${key.join("/")} saved (${fileMetadata.chunkCount} chunks)`,
    );
    return await this.db.set(key, fileMetadata satisfies FileMetadata, options);
  }

  /**
   * Load a file from the database.
   */
  async get(
    key: Deno.KvKey,
  ): Promise<Deno.KvEntryMaybe<Uint8Array>> {
    // load the main file entry
    const fileEntry = await this.db.get<FileMetadata>(key);
    if (fileEntry.versionstamp == null) {
      return { key, value: null, versionstamp: null };
    }

    // join the chunks into a single Uint8Array
    let fileData = new Uint8Array(0);
    for (let i = 0; i < fileEntry.value.chunkCount; i++) {
      const chunkEntry = await this.db.get<Uint8Array>([...key, i]);
      if (chunkEntry.versionstamp == null) {
        throw new Error(
          `Loading file ${key.join("/")} failed: Chunk ${
            chunkEntry.key.join("/")
          } is missing`,
        );
      }
      fileData = concat(fileData, chunkEntry.value);
    }

    // check the digest
    const digest = await crypto.subtle.digest("SHA-256", fileData);
    if (!equals(new Uint8Array(digest), fileEntry.value.digest)) {
      throw new Error(
        `Loading file ${key.join("/")} failed: Digest mismatch`,
      );
    }

    return { key, value: fileData, versionstamp: fileEntry.versionstamp };
  }

  /**
   * Delete a file from the database.
   *
   * It just deletes the key and all the sub-keys without looking what was stored there.
   */
  async delete(key: Deno.KvKey): Promise<void> {
    let i = 0;
    for await (const entry of this.db.list({ prefix: key })) {
      await this.db.delete(entry.key);
      i++;
    }
    logger().debug(`File ${key.join("/")} deleted (${i} chunks)`);
    await this.db.delete(key);
  }
}
