import assert from 'node:assert/strict';
import { describe, it, beforeEach } from 'node:test';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';
import express from 'express';
import type { AddressInfo } from 'node:net';

import {
  runMigrations,
  loadBundledMigrations,
  getLatestBundledVersion,
  getLatestAppliedVersion,
  getMigrationStatus,
  computeChecksum,
  ADVISORY_LOCK_ID,
  type BundledMigration,
} from '../src/db/migration-runner.ts';
import { readinessHandler } from '../src/health.ts';

// ── Mock In-Memory Database for Migration Runner Unit Testing ────────────────

interface MockRow {
  [key: string]: any;
}

class FakeMigrationClient {
  public locked = false;
  public lockCount = 0;
  public unlockCount = 0;
  public inTransaction = false;
  public tables: Set<string> = new Set();
  public schemaMigrations: Map<number, { version: number; name: string; checksum: string; applied_at: Date }> = new Map();
  public invoices: MockRow[] = [];
  public executedQueries: string[] = [];
  private lockWaiters: (() => void)[] = [];

  async query(sqlText: string, params?: any[]): Promise<{ rows: any[]; rowCount: number }> {
    const rawSql = sqlText.trim();
    this.executedQueries.push(rawSql);

    // Advisory lock handling - blocks if already locked until unlocked
    if (rawSql.includes('pg_advisory_lock')) {
      const lockId = params ? params[0] : ADVISORY_LOCK_ID;
      assert.equal(lockId, ADVISORY_LOCK_ID, 'Must acquire correct advisory lock ID');
      while (this.locked) {
        await new Promise<void>(resolve => this.lockWaiters.push(resolve));
      }
      this.locked = true;
      this.lockCount++;
      return { rows: [{ pg_advisory_lock: true }], rowCount: 1 };
    }

    if (rawSql.includes('pg_advisory_unlock')) {
      const lockId = params ? params[0] : ADVISORY_LOCK_ID;
      assert.equal(lockId, ADVISORY_LOCK_ID, 'Must unlock correct advisory lock ID');
      this.locked = false;
      this.unlockCount++;
      const next = this.lockWaiters.shift();
      if (next) {
        next();
      }
      return { rows: [{ pg_advisory_unlock: true }], rowCount: 1 };
    }

    // Transaction controls
    if (rawSql === 'BEGIN') {
      this.inTransaction = true;
      return { rows: [], rowCount: 0 };
    }
    if (rawSql === 'COMMIT') {
      this.inTransaction = false;
      return { rows: [], rowCount: 0 };
    }
    if (rawSql === 'ROLLBACK') {
      this.inTransaction = false;
      return { rows: [], rowCount: 0 };
    }

    // Schema migrations table creation
    if (rawSql.includes('CREATE TABLE IF NOT EXISTS schema_migrations')) {
      this.tables.add('schema_migrations');
      return { rows: [], rowCount: 0 };
    }

    // Checking if invoices table exists in information_schema
    if (rawSql.includes("table_name = 'invoices'")) {
      const exists = this.tables.has('invoices');
      return { rows: exists ? [{ '?column?': 1 }] : [], rowCount: exists ? 1 : 0 };
    }

    // Query schema_migrations
    if (rawSql.includes('SELECT version, name, checksum, applied_at FROM schema_migrations')) {
      if (!this.tables.has('schema_migrations')) {
        const err: any = new Error('relation "schema_migrations" does not exist');
        err.code = '42P01';
        throw err;
      }
      const rows = Array.from(this.schemaMigrations.values()).sort((a, b) => a.version - b.version);
      return { rows, rowCount: rows.length };
    }

    // SELECT MAX(version)
    if (rawSql.includes('SELECT MAX(version)')) {
      if (!this.tables.has('schema_migrations')) {
        const err: any = new Error('relation "schema_migrations" does not exist');
        err.code = '42P01';
        throw err;
      }
      if (this.schemaMigrations.size === 0) {
        return { rows: [{ max_v: null }], rowCount: 1 };
      }
      const maxV = Math.max(...Array.from(this.schemaMigrations.keys()));
      return { rows: [{ max_v: maxV }], rowCount: 1 };
    }

    // Insert into schema_migrations
    if (rawSql.includes('INSERT INTO schema_migrations')) {
      const version = params![0];
      const name = params![1];
      const checksum = params![2];
      this.schemaMigrations.set(version, {
        version,
        name,
        checksum,
        applied_at: new Date(),
      });
      return { rows: [], rowCount: 1 };
    }

    // Check connectivity: SELECT 1
    if (rawSql === 'SELECT 1') {
      return { rows: [{ '?column?': 1 }], rowCount: 1 };
    }

    // General DDL simulation (CREATE TABLE, DROP INDEX, etc.)
    if (rawSql.includes('CREATE TABLE IF NOT EXISTS invoices')) {
      this.tables.add('invoices');
    }
    if (rawSql.includes('CREATE TABLE IF NOT EXISTS transactions')) {
      this.tables.add('transactions');
    }
    if (rawSql.includes('CREATE TABLE IF NOT EXISTS payment_events')) {
      this.tables.add('payment_events');
    }

    return { rows: [], rowCount: 0 };
  }
}

class FakeMigrationPool {
  public client: FakeMigrationClient;
  public connectCount = 0;
  public releaseCount = 0;

  constructor(client: FakeMigrationClient) {
    this.client = client;
  }

  async connect() {
    this.connectCount++;
    return {
      query: (sql: string, params?: any[]) => this.client.query(sql, params),
      release: () => {
        this.releaseCount++;
      },
    };
  }

  async query(sql: string, params?: any[]) {
    return this.client.query(sql, params);
  }
}

// ── Tests ───────────────────────────────────────────────────────────────────

describe('Database Migration Runner (Issue #583)', () => {
  let tempMigrationsDir: string;

  beforeEach(() => {
    tempMigrationsDir = fs.mkdtempSync(path.join(os.tmpdir(), 'quittance-mig-'));
  });

  it('computes consistent sha256 checksums', () => {
    const sum1 = computeChecksum('SELECT 1;');
    const sum2 = computeChecksum('SELECT 1;');
    const sum3 = computeChecksum('SELECT 2;');
    assert.equal(sum1, sum2);
    assert.notEqual(sum1, sum3);
    assert.equal(sum1.length, 64);
  });

  it('loads bundled migrations sorted by numerical version', () => {
    fs.writeFileSync(path.join(tempMigrationsDir, '0002_updates.sql'), '-- migration 2');
    fs.writeFileSync(path.join(tempMigrationsDir, '0001_baseline.sql'), '-- migration 1');
    fs.writeFileSync(path.join(tempMigrationsDir, '0010_later.sql'), '-- migration 10');
    fs.writeFileSync(path.join(tempMigrationsDir, 'README.txt'), 'ignore me');

    const bundled = loadBundledMigrations(tempMigrationsDir);
    assert.equal(bundled.length, 3);
    assert.equal(bundled[0].version, 1);
    assert.equal(bundled[0].fileName, '0001_baseline.sql');
    assert.equal(bundled[1].version, 2);
    assert.equal(bundled[1].fileName, '0002_updates.sql');
    assert.equal(bundled[2].version, 10);
    assert.equal(bundled[2].fileName, '0010_later.sql');

    assert.equal(getLatestBundledVersion(tempMigrationsDir), 10);
  });

  it('runs twice on an empty database: first applies all, second run applies nothing', async () => {
    fs.writeFileSync(path.join(tempMigrationsDir, '0001_baseline.sql'), 'CREATE TABLE IF NOT EXISTS invoices (id UUID);');
    fs.writeFileSync(path.join(tempMigrationsDir, '0002_updates.sql'), 'ALTER TABLE invoices ADD COLUMN status TEXT;');

    const client = new FakeMigrationClient();
    const pool = new FakeMigrationPool(client);

    // First run
    const result1 = await runMigrations(pool as any, { migrationsDir: tempMigrationsDir, quiet: true });
    assert.equal(result1.appliedCount, 2);
    assert.equal(result1.alreadyAppliedCount, 0);
    assert.equal(result1.baselineAdopted, false);
    assert.equal(result1.latestVersion, 2);
    assert.equal(client.schemaMigrations.size, 2);
    assert.equal(client.lockCount, 1);
    assert.equal(client.unlockCount, 1);
    assert.equal(client.locked, false, 'Advisory lock must be released');

    // Second run
    const result2 = await runMigrations(pool as any, { migrationsDir: tempMigrationsDir, quiet: true });
    assert.equal(result2.appliedCount, 0);
    assert.equal(result2.alreadyAppliedCount, 2);
    assert.equal(result2.baselineAdopted, false);
    assert.equal(result2.latestVersion, 2);
    assert.equal(client.lockCount, 2);
    assert.equal(client.unlockCount, 2);
    assert.equal(client.locked, false);
  });

  it('editing an applied migration fails the runner with a checksum error', async () => {
    fs.writeFileSync(path.join(tempMigrationsDir, '0001_baseline.sql'), 'CREATE TABLE invoices (id UUID);');
    const client = new FakeMigrationClient();
    const pool = new FakeMigrationPool(client);

    // Apply 0001
    await runMigrations(pool as any, { migrationsDir: tempMigrationsDir, quiet: true });
    assert.equal(client.schemaMigrations.has(1), true);

    // Modify 0001 content
    fs.writeFileSync(path.join(tempMigrationsDir, '0001_baseline.sql'), 'CREATE TABLE invoices (id UUID, tampered BOOLEAN);');

    // Re-running runner must throw a checksum mismatch error
    await assert.rejects(
      () => runMigrations(pool as any, { migrationsDir: tempMigrationsDir, quiet: true }),
      /Migration checksum mismatch for version 1/
    );

    // Advisory lock must still be unlocked on error
    assert.equal(client.locked, false);
  });

  it('adopts baseline on existing database built from schema.sql without replaying 0001', async () => {
    fs.writeFileSync(path.join(tempMigrationsDir, '0001_baseline.sql'), 'CREATE TABLE invoices (id UUID PRIMARY KEY);');
    fs.writeFileSync(path.join(tempMigrationsDir, '0002_updates.sql'), 'ALTER TABLE invoices ADD COLUMN status TEXT;');

    const client = new FakeMigrationClient();
    // Simulate pre-existing database built from schema.sql: invoices table exists with rows
    client.tables.add('invoices');
    client.invoices.push({ id: '11111111-1111-4111-8111-111111111111', memo: 'INV-EXISTING-1' });

    const pool = new FakeMigrationPool(client);

    // Run migrations
    const result = await runMigrations(pool as any, { migrationsDir: tempMigrationsDir, quiet: true });

    // Baseline adopted, version 2 applied
    assert.equal(result.baselineAdopted, true);
    assert.equal(result.appliedCount, 1, 'Only migration 2 should be applied');
    assert.equal(result.appliedMigrations[0].version, 2);
    assert.equal(client.schemaMigrations.has(1), true);
    assert.equal(client.schemaMigrations.has(2), true);

    // Rows intact
    assert.equal(client.invoices.length, 1);
    assert.equal(client.invoices[0].memo, 'INV-EXISTING-1');

    // Did not replay the DDL for 0001
    const executed0001 = client.executedQueries.some(q => q === 'CREATE TABLE invoices (id UUID PRIMARY KEY);');
    assert.equal(executed0001, false, '0001 DDL must not be replayed when baseline is adopted');
  });

  it('concurrent runs acquire pg_advisory_lock and never apply the same version', async () => {
    fs.writeFileSync(path.join(tempMigrationsDir, '0001_baseline.sql'), 'CREATE TABLE test1 (id INT);');
    fs.writeFileSync(path.join(tempMigrationsDir, '0002_updates.sql'), 'CREATE TABLE test2 (id INT);');

    const client = new FakeMigrationClient();
    const pool = new FakeMigrationPool(client);

    // Execute two concurrent promises
    const [resA, resB] = await Promise.all([
      runMigrations(pool as any, { migrationsDir: tempMigrationsDir, quiet: true }),
      runMigrations(pool as any, { migrationsDir: tempMigrationsDir, quiet: true }),
    ]);

    // One applied the migrations, the other applied 0
    const totalApplied = resA.appliedCount + resB.appliedCount;
    assert.equal(totalApplied, 2, 'Combined applied count across concurrent runs must be exactly 2');
    assert.ok(
      (resA.appliedCount === 2 && resB.appliedCount === 0) ||
      (resA.appliedCount === 0 && resB.appliedCount === 2)
    );
    assert.equal(client.schemaMigrations.size, 2);
    assert.equal(client.locked, false, 'Lock must be freed');
  });

  it('reports not-ready (503) when database is behind bundled migrations or disconnected in Postgres mode', async () => {
    fs.writeFileSync(path.join(tempMigrationsDir, '0001_baseline.sql'), '-- v1');
    fs.writeFileSync(path.join(tempMigrationsDir, '0002_updates.sql'), '-- v2');

    const client = new FakeMigrationClient();
    const pool = new FakeMigrationPool(client);

    const app = express();
    process.env.NODE_ENV = 'test';
    process.env.FRONTEND_URL = 'http://localhost:3000';
    process.env.STELLAR_NETWORK = 'TESTNET';
    process.env.STELLAR_HORIZON_URL = 'https://horizon-testnet.stellar.org';
    process.env.ALLOW_SIMULATE = 'false';

    // Mount readiness handler in Postgres mode with our pool and test migrationsDir
    app.get('/api/ready', readinessHandler('postgres', { pool, migrationsDir: tempMigrationsDir }));

    const server = app.listen(0, '127.0.0.1');
    await new Promise<void>((resolve) => server.once('listening', resolve));
    const port = (server.address() as AddressInfo).port;

    try {
      // 1. Database is completely unmigrated -> 503
      const res1 = await fetch(`http://127.0.0.1:${port}/api/ready`);
      assert.equal(res1.status, 503);
      const body1 = (await res1.json()) as any;
      assert.equal(body1.status, 'not_ready');
      assert.equal(body1.ready, false);
      assert.equal(body1.checks.databaseMigration, false);

      // 2. Database has only version 1 applied (behind bundled version 2) -> 503
      client.tables.add('schema_migrations');
      client.schemaMigrations.set(1, { version: 1, name: '0001_baseline.sql', checksum: 'abc', applied_at: new Date() });

      const res2 = await fetch(`http://127.0.0.1:${port}/api/ready`);
      assert.equal(res2.status, 503);
      const body2 = (await res2.json()) as any;
      assert.equal(body2.status, 'not_ready');
      assert.equal(body2.ready, false);
      assert.equal(body2.checks.databaseMigration, false);
      assert.ok(body2.reasons.some((r: string) => r.includes('Database migration pending')));

      // 3. Database is migrated to version 2 -> 200 Ready
      client.schemaMigrations.set(2, { version: 2, name: '0002_updates.sql', checksum: 'def', applied_at: new Date() });

      const res3 = await fetch(`http://127.0.0.1:${port}/api/ready`);
      assert.equal(res3.status, 200);
      const body3 = (await res3.json()) as any;
      assert.equal(body3.status, 'ready');
      assert.equal(body3.ready, true);
      assert.equal(body3.checks.databaseMigration, true);
      assert.equal(body3.checks.storageReady, true);

      // 4. Database disconnected / connection error -> 503
      const failingPool = {
        query: () => Promise.reject(new Error('Connection refused: 5432')),
      };
      const appFail = express();
      appFail.get('/api/ready', readinessHandler('postgres', { pool: failingPool, migrationsDir: tempMigrationsDir }));
      const serverFail = appFail.listen(0, '127.0.0.1');
      await new Promise<void>((r) => serverFail.once('listening', r));
      const failPort = (serverFail.address() as AddressInfo).port;
      try {
        const resFail = await fetch(`http://127.0.0.1:${failPort}/api/ready`);
        assert.equal(resFail.status, 503);
        const bodyFail = (await resFail.json()) as any;
        assert.equal(bodyFail.status, 'not_ready');
        assert.equal(bodyFail.checks.databaseConnected, false);
      } finally {
        await new Promise<void>((r) => serverFail.close(() => r()));
      }
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });

  it('runs against actual shipped repo migrations in db/migrations/ cleanly', async () => {
    const shippedDir = path.resolve(__dirname, '../../db/migrations');
    assert.equal(fs.existsSync(shippedDir), true);

    const bundled = loadBundledMigrations(shippedDir);
    assert.ok(bundled.length >= 2, 'Should have at least 0001 and 0002');
    assert.equal(bundled[0].version, 1);
    assert.equal(bundled[0].fileName, '0001_baseline.sql');
    assert.equal(bundled[1].version, 2);
    assert.equal(bundled[1].fileName, '0002_updates.sql');

    const client = new FakeMigrationClient();
    const pool = new FakeMigrationPool(client);

    const res = await runMigrations(pool as any, { migrationsDir: shippedDir, quiet: true });
    assert.equal(res.appliedCount, bundled.length);
    assert.equal(res.latestVersion, bundled[bundled.length - 1].version);
  });
});
