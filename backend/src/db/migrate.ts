import { pool } from '../config/database';
import { runMigrations } from './migration-runner';

/**
 * Migration CLI entry point (npm run db:migrate).
 * Runs versioned forward-only migrations under pg_advisory_lock.
 */
async function migrate() {
  console.log('🚀 Starting database migrations...\n');

  try {
    const result = await runMigrations(pool);
    if (result.appliedCount > 0) {
      console.log(`\n✅ Applied ${result.appliedCount} migration(s):`);
      for (const m of result.appliedMigrations) {
        console.log(`  - [v${m.version}] ${m.name}`);
      }
    } else {
      console.log('✨ No new migrations to apply.');
    }
    if (result.baselineAdopted) {
      console.log('📋 Existing database baseline adopted.');
    }
    console.log(`📌 Current database version: ${result.latestVersion}\n`);
  } catch (error) {
    console.error('❌ Migration failed:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

// Run migration if called directly
if (require.main === module || process.argv[1]?.endsWith('migrate.ts')) {
  migrate()
    .then(() => {
      console.log('✨ All done!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('Migration error:', error);
      process.exit(1);
    });
}
