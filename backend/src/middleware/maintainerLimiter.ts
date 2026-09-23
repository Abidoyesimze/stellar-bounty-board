import { Request, Response, NextFunction, RequestHandler } from "express";
import fs from "fs";
import path from "path";
import lockfile from "proper-lockfile";
import { logger } from "../logger";
import { MiddlewareDependencyError } from "./errors";

const LIMIT = Number(process.env.MAINTAINER_BOUNTY_RATE_LIMIT ?? 10);
const WINDOW_MS = Number(process.env.MAINTAINER_BOUNTY_RATE_WINDOW_MS ?? 3600_000);

interface RateLimitRecord {
  timestamps: number[];
}

function getStorePath(): string {
  if (process.env.MAINTAINER_RATE_LIMIT_STORE_PATH?.trim()) {
    return path.resolve(process.env.MAINTAINER_RATE_LIMIT_STORE_PATH.trim());
  }
  if (process.env.BOUNTY_STORE_PATH?.trim()) {
    return path.resolve(path.dirname(process.env.BOUNTY_STORE_PATH.trim()), "maintainer_rate_limits.json");
  }
  return path.resolve(process.cwd(), "data", "maintainer_rate_limits.json");
}

function ensureStore(storePath: string) {
  const dir = path.dirname(storePath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  if (!fs.existsSync(storePath)) {
    fs.writeFileSync(storePath, JSON.stringify({}));
  }
}

const STORE_UNAVAILABLE_MESSAGE = "Rate limit store is unavailable, please try again.";

function storeError(operation: string, cause: unknown): MiddlewareDependencyError {
  return new MiddlewareDependencyError({
    operation: `maintainer_rate_limit.${operation}`,
    dependency: "json-store",
    statusCode: 503,
    publicMessage: STORE_UNAVAILABLE_MESSAGE,
    cause,
  });
}

async function releaseQuietly(release: () => Promise<void>, storePath: string): Promise<void> {
  try {
    await release();
  } catch (err) {
    // The lock goes stale after `stale` ms, so a failed release only delays
    // the next writer; it must not fail a request whose work already finished.
    logger.warn({ err, storePath }, "maintainer_rate_limit_release_failed");
  }
}

export const maintainerLimiter: RequestHandler = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  if (process.env.NODE_ENV === "test") {
    next();
    return;
  }

  const maintainer = req.body?.maintainer;
  if (!maintainer || typeof maintainer !== "string") {
    next();
    return;
  }

  const storePath = getStorePath();
  try {
    ensureStore(storePath);
  } catch (err) {
    next(storeError("init_store", err));
    return;
  }

  let release: () => Promise<void>;
  try {
    release = await lockfile.lock(storePath, {
      retries: { retries: 5, minTimeout: 10, maxTimeout: 50 },
      stale: 5000,
    });
  } catch (err) {
    next(
      new MiddlewareDependencyError({
        operation: "maintainer_rate_limit.acquire_lock",
        dependency: "file-lock",
        statusCode: 503,
        publicMessage: "Service busy, please try again.",
        cause: err,
      }),
    );
    return;
  }

  let operation = "read_store";
  try {
    const raw = fs.readFileSync(storePath, "utf8");
    let store: Record<string, RateLimitRecord> = {};
    try {
      store = JSON.parse(raw);
    } catch {
      // Ignore parse error and start fresh
    }

    const now = Date.now();
    const windowStart = now - WINDOW_MS;

    const record = store[maintainer] || { timestamps: [] };

    // Clean up old timestamps
    record.timestamps = record.timestamps.filter((ts) => ts > windowStart);

    if (record.timestamps.length >= LIMIT) {
      const oldest = record.timestamps[0];
      const resetTime = oldest + WINDOW_MS;
      const retryAfter = Math.ceil((resetTime - now) / 1000);

      await releaseQuietly(release, storePath);

      res.setHeader("Retry-After", String(retryAfter));
      res.status(429).json({ error: "Too many requests. Please retry later." });
      return;
    }

    record.timestamps.push(now);
    store[maintainer] = record;
    operation = "write_store";
    fs.writeFileSync(storePath, JSON.stringify(store, null, 2));
  } catch (err) {
    await releaseQuietly(release, storePath);
    next(storeError(operation, err));
    return;
  }

  await releaseQuietly(release, storePath);
  next();
};
