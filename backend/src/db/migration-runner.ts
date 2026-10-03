import * as fs from 'node:fs';
import * as path from 'node:path';
import * as crypto from 'node:crypto';
import type { Pool, PoolClient } from 'pg';
import { pool as defaultPool } from '../config/database';

export const DEFAULT_MIGRATIONS_DIR = path.resolve(__dirname, '../../../db/migrations');
export const ADVISORY_LOCK_ID = 84729104;

export interface BundledMigration {
  version: number;
  name: string;
  fileName: string;
  filePath: string;
  sql: string;
  checksum: string;
}

export interface AppliedMigration {
  version: number;
  name: string;
  checksum: string;
  applied_at: Date;
}

export interface MigrationOptions {
  migrationsDir?: string;
  quiet?: boolean;
}

export interface MigrationResult {
  appliedCount: number;
  appliedMigrations: { version: number; name: string }[];
  alreadyAppliedCount: number;
  baselineAdopted: boolean;
  latestVersion: number;
}

export interface MigrationStatus {
  latestBundled: number;
  latestApplied: number | null;
  isReady: boolean;
  appliedCount: number;
  pendingCount: number;
}

/**
 * Calculates SHA-256 hex digest for a string.
 */
export function computeChecksum(content: string): string {
  return crypto.createHash('sha256').update(content, 'utf-8').digest('hex');
}

/**
 * Reads and parses all valid .sql migration files from the migrations directory,
 * sorted numerically by version.
 */
export function loadBundledMigrations(migrationsDir: string = DEFAULT_MIGRATIONS_DIR): BundledMigration[] {
  if (!fs.existsSync(migrationsDir)) {
    throw new Error(`Migrations directory does not exist: ${migrationsDir}`);
  }

  const files = fs.readdirSync(migrationsDir);
  const migrationFiles = files.filter(f => /^\d+_[a-zA-Z0-9_\-]+\.sql$/.test(f));

  const bundled: BundledMigration[] = migrationFiles.map(fileName => {
    const match = fileName.match(/^(\d+)_(.+)\.sql$/);
    if (!match) {
      throw new Error(`Invalid migration filename format: ${fileName}`);
    }
    const version = parseInt(match[1], 10);
    const name = match[2];
    const filePath = path.join(migrationsDir, fileName);
    const sql = fs.readFileSync(filePath, 'utf-8');
    const checksum = computeChecksum(sql);

    return {
      version,
      name,
      fileName,
      filePath,
      sql,
      checksum,
    };
  });

  return bundled.sort((a, b) => a.version - b.version);
}

/**
 * Returns the highest version number among bundled migrations.
 */
export function getLatestBundledVersion(migrationsDir: string = DEFAULT_MIGRATIONS_DIR): number {
  const bundled = loadBundledMigrations(migrationsDir);
  if (bundled.length === 0) return 0;
  return Math.max(...bundled.map(m => m.version));
}

/**
 * Returns the highest version number currently recorded in schema_migrations.
 * Returns null if schema_migrations does not exist or has no records.
 */
export async function getLatestAppliedVersion(poolOrClient: Pool | PoolClient = defaultPool): Promise<number | null> {
  try {
    const res = await poolOrClient.query('SELECT MAX(version) AS max_v FROM schema_migrations');
    const val = res.rows[0]?.max_v;
    return val !== null && val !== undefined ? Number(val) : null;
  } catch (err: any) {
    // 42P01 is undefined_table in PostgreSQL
    if (err?.code === '42P01') {
      return null;
    }
    throw err;
  }
}

/**
 * Returns the migration status: latest bundled version, latest applied version, and readiness.
 */
export async function getMigrationStatus(
  poolOrClient: Pool | PoolClient = defaultPool,
  options: MigrationOptions = {}
): Promise<MigrationStatus> {
  const migrationsDir = options.migrationsDir || DEFAULT_MIGRATIONS_DIR;
  const latestBundled = getLatestBundledVersion(migrationsDir);
  const latestApplied = await getLatestAppliedVersion(poolOrClient);

  const isReady = latestApplied !== null && latestApplied === latestBundled;

  return {
    latestBundled,
    latestApplied,
    isReady,
    appliedCount: latestApplied ?? 0,
    pendingCount: latestApplied !== null ? Math.max(0, latestBundled - latestApplied) : latestBundled,
  };
}

/**
 * Ensures the schema_migrations table exists.
 */
async function ensureMigrationsTable(client: PoolClient | Pool): Promise<void> {
  await client.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version INTEGER PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      checksum VARCHAR(64) NOT NULL,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
}

/**
 * Checks whether an existing database already has the baseline schema (invoices table exists)
 * so it can adopt baseline version 1 without replaying.
 */
async function isExistingBaselineDatabase(client: PoolClient | Pool): Promise<boolean> {
  const res = await client.query(`
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'invoices'
    LIMIT 1;
  `);
  return res.rows.length > 0;
}

/**
 * Applies pending migrations sequentially under pg_advisory_lock,
 * each in its own transaction.
 */
export async function runMigrations(
  poolOrClient: Pool | PoolClient = defaultPool,
  options: MigrationOptions = {}
): Promise<MigrationResult> {
  const migrationsDir = options.migrationsDir || DEFAULT_MIGRATIONS_DIR;
  const quiet = options.quiet ?? false;
  const log = (msg: string) => { if (!quiet) console.log(msg); };

  const bundled = loadBundledMigrations(migrationsDir);
  if (bundled.length === 0) {
    log('⚠️  No migration files found.');
    return {
      appliedCount: 0,
      appliedMigrations: [],
      alreadyAppliedCount: 0,
      baselineAdopted: false,
      latestVersion: 0,
    };
  }

  // Obtain client if pool was passed
  let client: PoolClient;
  let shouldRelease = false;
  if ('connect' in poolOrClient && typeof (poolOrClient as Pool).connect === 'function') {
    client = await (poolOrClient as Pool).connect();
    shouldRelease = true;
  } else {
    client = poolOrClient as PoolClient;
  }

  let baselineAdopted = false;
  const appliedMigrations: { version: number; name: string }[] = [];

  try {
    // 1. Acquire advisory lock
    await client.query('SELECT pg_advisory_lock($1)', [ADVISORY_LOCK_ID]);

    // 2. Ensure schema_migrations table exists
    await ensureMigrationsTable(client);

    // 3. Fetch already applied migrations
    const appliedRes = await client.query<AppliedMigration>(
      'SELECT version, name, checksum, applied_at FROM schema_migrations ORDER BY version ASC'
    );
    const appliedMap = new Map<number, AppliedMigration>();
    for (const row of appliedRes.rows) {
      appliedMap.set(Number(row.version), row);
    }

    // 4. Validate checksums of applied migrations
    for (const b of bundled) {
      const applied = appliedMap.get(b.version);
      if (applied) {
        if (applied.checksum !== b.checksum) {
          throw new Error(
            `Migration checksum mismatch for version ${b.version} (${b.fileName}). ` +
            `Applied checksum: ${applied.checksum}, Current checksum: ${b.checksum}. ` +
            `Migrations must be forward-only and immutable.`
          );
        }
      }
    }

    // 5. Baseline adoption detection:
    // If version 1 is not recorded in schema_migrations, but invoices table exists,
    // adopt baseline (version 1) without replaying.
    const baselineMigration = bundled.find(m => m.version === 1);
    if (baselineMigration && !appliedMap.has(1)) {
      const hasBaselineTables = await isExistingBaselineDatabase(client);
      if (hasBaselineTables) {
        log('🔍 Detected existing pre-migration database. Adopting baseline version 1...');
        await client.query(
          'INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES ($1, $2, $3, NOW()) ON CONFLICT (version) DO NOTHING',
          [baselineMigration.version, baselineMigration.fileName, baselineMigration.checksum]
        );
        appliedMap.set(1, {
          version: 1,
          name: baselineMigration.fileName,
          checksum: baselineMigration.checksum,
          applied_at: new Date(),
        });
        baselineAdopted = true;
        log('✅ Baseline adopted successfully.');
      }
    }

    // 6. Identify pending migrations
    const pending = bundled.filter(m => !appliedMap.has(m.version));

    if (pending.length === 0) {
      log('✨ Database is already up to date. No pending migrations.');
      const latest = Math.max(...bundled.map(m => m.version));
      return {
        appliedCount: 0,
        appliedMigrations: [],
        alreadyAppliedCount: appliedMap.size,
        baselineAdopted,
        latestVersion: latest,
      };
    }

    log(`🚀 Applying ${pending.length} pending migration(s)...`);

    // 7. Apply each pending migration in its own transaction
    for (const migration of pending) {
      log(`  Applying [${migration.version.toString().padStart(4, '0')}] ${migration.fileName}...`);
      await client.query('BEGIN');
      try {
        await client.query(migration.sql);
        await client.query(
          'INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES ($1, $2, $3, NOW())',
          [migration.version, migration.fileName, migration.checksum]
        );
        await client.query('COMMIT');
        appliedMigrations.push({ version: migration.version, name: migration.fileName });
        appliedMap.set(migration.version, {
          version: migration.version,
          name: migration.fileName,
          checksum: migration.checksum,
          applied_at: new Date(),
        });
        log(`  ✔ Applied [${migration.version.toString().padStart(4, '0')}] ${migration.fileName}`);
      } catch (err: any) {
        await client.query('ROLLBACK');
        throw new Error(
          `Failed to apply migration ${migration.fileName} (version ${migration.version}): ${err.message}`
        );
      }
    }

    log(`✅ Successfully applied ${appliedMigrations.length} migration(s).`);

    const latest = Math.max(...bundled.map(m => m.version));
    return {
      appliedCount: appliedMigrations.length,
      appliedMigrations,
      alreadyAppliedCount: appliedMap.size - appliedMigrations.length,
      baselineAdopted,
      latestVersion: latest,
    };
  } finally {
    // 8. Always release advisory lock
    try {
      await client.query('SELECT pg_advisory_unlock($1)', [ADVISORY_LOCK_ID]);
    } catch (unlockErr) {
      console.error('Warning: Failed to release advisory lock:', unlockErr);
    }
    if (shouldRelease) {
      client.release();
    }
  }
}
