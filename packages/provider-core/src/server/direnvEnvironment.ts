import type { ThreadId } from "@t3tools/contracts";

/** Variables to set (string) or unset (null), as `direnv export json` reports them. */
export type DirenvEnvironmentDiff = Readonly<Record<string, string | null>>;

/**
 * Apply a direnv diff over a provider's launch environment. Values the
 * provider instance set itself (anything differing from the server's own
 * environment, like `CODEX_HOME` or `CLAUDE_CONFIG_DIR`) are explicit user
 * configuration and win over the project's `.envrc`.
 */
export function applyDirenvEnvironment(
  base: NodeJS.ProcessEnv,
  diff: DirenvEnvironmentDiff | undefined,
  serverEnv: NodeJS.ProcessEnv = process.env,
): NodeJS.ProcessEnv {
  if (!diff) return base;
  const next: NodeJS.ProcessEnv = { ...base };
  for (const [name, value] of Object.entries(diff)) {
    if (base[name] !== serverEnv[name]) continue;
    if (value === null) delete next[name];
    else next[name] = value;
  }
  return next;
}

// ProviderSessionManagerV2 loads the diff before opening a session; adapters
// read it while building the provider's launch environment.
const diffsByThread = new Map<ThreadId, DirenvEnvironmentDiff>();

export function setThreadDirenvEnvironment(
  threadId: ThreadId,
  diff: DirenvEnvironmentDiff | undefined,
): void {
  if (diff && Object.keys(diff).length > 0) diffsByThread.set(threadId, diff);
  else diffsByThread.delete(threadId);
}

export function readThreadDirenvEnvironment(threadId: ThreadId): DirenvEnvironmentDiff | undefined {
  return diffsByThread.get(threadId);
}

export function clearAllThreadDirenvEnvironments(): void {
  diffsByThread.clear();
}

/** `base` with the thread's direnv environment applied, or `base` untouched. */
export function withThreadDirenvEnvironment(
  base: NodeJS.ProcessEnv,
  threadId: ThreadId,
): NodeJS.ProcessEnv {
  return applyDirenvEnvironment(base, diffsByThread.get(threadId));
}
