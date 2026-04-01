/**
 * LazyFilesystem — wraps a backing jswasi Filesystem (typically FSA)
 * and intercepts file operations to trigger on-demand chunk hydration.
 *
 * Directory structure and metadata are answered from the manifest.
 * File content is fetched from chunk archives when first accessed,
 * then written to the backing store so subsequent accesses are fast.
 */

import * as constants from "../constants.js";
import type {
    Filesystem,
    Descriptor,
    Filestat,
    LookupFlags,
    OpenFlags,
    Rights,
    Fdflags,
} from "./filesystem.js";

// ---------------------------------------------------------------------------
// Manifest types (duplicated minimally to avoid cross-package import)
// ---------------------------------------------------------------------------

export interface LazyManifest {
    version: 1;
    baseUrl: string;
    prefetch: string[];
    files: Record<string, [chunkId: string, size: number]>;
}

export interface LazyChunkFetcher {
    /** Ensure all files from the given chunk are extracted to the backing store. */
    hydrate(chunkId: string): Promise<void>;
    /** Whether the chunk has already been fully extracted. */
    isHydrated(chunkId: string): boolean;
}

// ---------------------------------------------------------------------------
// LazyFilesystem
// ---------------------------------------------------------------------------

export class LazyFilesystem implements Filesystem {
    private backing: Filesystem;
    private manifest: LazyManifest;
    private fetcher: LazyChunkFetcher;
    private dirs: Set<string>;

    constructor(
        backing: Filesystem,
        manifest: LazyManifest,
        fetcher: LazyChunkFetcher,
    ) {
        this.backing = backing;
        this.manifest = manifest;
        this.fetcher = fetcher;
        this.dirs = this.buildDirSet();
    }

    fsname(): string {
        return 'lazy';
    }

    async initialize(opts: Object): Promise<number> {
        return this.backing.initialize(opts);
    }

    // -------------------------------------------------------------------
    // open — the critical interception point
    // -------------------------------------------------------------------

    async open(
        path: string,
        dirflags: LookupFlags,
        oflags: OpenFlags,
        fs_rights_base: Rights,
        fs_rights_inheriting: Rights,
        fdflags: Fdflags,
        workerId: number,
    ): Promise<{ err: number; index: number; desc: Descriptor }> {
        // Try the backing store first
        const result = await this.backing.open(
            path, dirflags, oflags, fs_rights_base, fs_rights_inheriting, fdflags, workerId,
        );

        if (result.err !== constants.WASI_ENOENT) {
            return result; // found (or a different error) — pass through
        }

        // Not found — check manifest
        const norm = normalizePath(path);
        const entry = this.manifest.files[norm];

        if (!entry) {
            // Also check if it's a directory in the manifest
            if (this.dirs.has(norm)) {
                // Create the directory in the backing store and retry
                await this.ensureDir(path);
                return this.backing.open(
                    path, dirflags, oflags, fs_rights_base, fs_rights_inheriting, fdflags, workerId,
                );
            }
            return result; // genuinely not found
        }

        // Hydrate the chunk containing this file
        const [chunkId] = entry;
        await this.fetcher.hydrate(chunkId);

        // Retry open on the backing store
        return this.backing.open(
            path, dirflags, oflags, fs_rights_base, fs_rights_inheriting, fdflags, workerId,
        );
    }

    // -------------------------------------------------------------------
    // getFilestat — return synthetic stats for lazy files
    // -------------------------------------------------------------------

    async getFilestat(path: string): Promise<{ err: number; filestat: Filestat }> {
        // Try backing store first
        const result = await this.backing.getFilestat(path);
        if (result.err !== constants.WASI_ENOENT) return result;

        const norm = normalizePath(path);

        // Check if it's a file in the manifest
        const entry = this.manifest.files[norm];
        if (entry) {
            const [, size] = entry;
            return {
                err: constants.WASI_ESUCCESS,
                filestat: {
                    dev: 0n,
                    ino: 0n,
                    filetype: constants.WASI_FILETYPE_REGULAR_FILE,
                    nlink: 1n,
                    size: BigInt(size),
                    atim: 0n,
                    mtim: 0n,
                    ctim: 0n,
                },
            };
        }

        // Check if it's a directory in the manifest
        if (this.dirs.has(norm)) {
            return {
                err: constants.WASI_ESUCCESS,
                filestat: {
                    dev: 0n,
                    ino: 0n,
                    filetype: constants.WASI_FILETYPE_DIRECTORY,
                    nlink: 1n,
                    size: 0n,
                    atim: 0n,
                    mtim: 0n,
                    ctim: 0n,
                },
            };
        }

        return result; // not found anywhere
    }

    // -------------------------------------------------------------------
    // Pass-through methods
    // -------------------------------------------------------------------

    async mkdirat(desc: Descriptor, path: string): Promise<number> {
        return this.backing.mkdirat(desc, path);
    }

    async unlinkat(desc: Descriptor, path: string, is_dir: boolean): Promise<number> {
        return this.backing.unlinkat(desc, path, is_dir);
    }

    async renameat(
        oldDesc: Descriptor, oldPath: string,
        newDesc: Descriptor, newPath: string,
    ): Promise<number> {
        return this.backing.renameat(oldDesc, oldPath, newDesc, newPath);
    }

    async symlinkat(target: string, desc: Descriptor, linkpath: string): Promise<number> {
        return this.backing.symlinkat(target, desc, linkpath);
    }

    async mknodat(desc: Descriptor, path: string, dev: number, args: Object): Promise<number> {
        return this.backing.mknodat(desc, path, dev, args);
    }

    // -------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------

    /** Build the set of all directories implied by manifest file paths. */
    private buildDirSet(): Set<string> {
        const dirs = new Set<string>();
        dirs.add(''); // root
        for (const filePath of Object.keys(this.manifest.files)) {
            let dir = filePath;
            while (true) {
                const slash = dir.lastIndexOf('/');
                if (slash === -1) break;
                dir = dir.substring(0, slash);
                if (dirs.has(dir)) break;
                dirs.add(dir);
            }
        }
        return dirs;
    }

    /** Ensure a directory path exists in the backing filesystem. */
    private async ensureDir(path: string): Promise<void> {
        // Walk the path segments and create each directory.
        // We can't assume recursive mkdir support on the Filesystem interface,
        // so we create each segment individually, ignoring EEXIST.
        const segments = path.replace(/^\//, '').split('/');
        let current = '/';
        for (const seg of segments) {
            if (!seg) continue;
            current = current === '/' ? `/${seg}` : `${current}/${seg}`;

            // Open the directory to check if it exists; create if not.
            // We use the backing's open to avoid infinite recursion.
            const check = await this.backing.getFilestat(current);
            if (check.err === constants.WASI_ENOENT) {
                // Need a descriptor for mkdirat — open the parent
                const parentResult = await this.backing.open(
                    current.substring(0, current.lastIndexOf('/')) || '/',
                    0, 0, 0n, 0n, 0, 0,
                );
                if (parentResult.err === constants.WASI_ESUCCESS && parentResult.desc) {
                    await this.backing.mkdirat(parentResult.desc, seg);
                    await parentResult.desc.close();
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function normalizePath(path: string): string {
    return path.replace(/^\//, '');
}
