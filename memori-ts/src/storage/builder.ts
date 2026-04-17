import { Config } from '../core/config.js';
import { StorageAdapter, BaseDriver } from './base.js';

export class Builder {
  private displayBanner = true;

  constructor(
    private readonly config: Config,
    private readonly adapter: StorageAdapter,
    private readonly driver: BaseDriver
  ) {}

  public disableBanner(): this {
    this.displayBanner = false;
    return this;
  }

  public async execute(): Promise<void> {
    let currentVersion = 0;

    // 1. Determine current schema version
    try {
      const res = await this.driver.schema.version.read();
      if (res !== null) {
        currentVersion = res;
      }
    } catch {
      if (this.driver.requiresRollbackOnError) {
        await this.adapter.rollback();
      }
      currentVersion = 0; // The schema table doesn't exist yet
    }

    if (this.displayBanner) {
      console.log(`[Memori] Currently at schema revision #${currentVersion}.`);
    }

    const migrations = this.driver.migrations;
    const availableVersions = Object.keys(migrations)
      .map(Number)
      .sort((a, b) => a - b);
    const maxVersion =
      availableVersions.length > 0 ? availableVersions[availableVersions.length - 1] : 0;

    if (currentVersion === maxVersion) {
      if (this.displayBanner) console.log(`[Memori] Data structures are up-to-date.`);
      return;
    }

    // 2. Run pending migrations sequentially
    let num = currentVersion;
    for (;;) {
      num += 1;
      const batch = migrations[num];
      if (!batch) break;

      if (this.displayBanner) console.log(`[Memori] Building revision #${num}...`);

      for (const migration of batch) {
        if (this.displayBanner) console.log(`  -> ${migration.description}`);

        const ops = migration.operations || (migration.operation ? [migration.operation] : []);

        // FIX: Start a transaction before running the migration statements!
        await this.adapter.begin();
        for (const operation of ops) {
          await this.adapter.execute(operation);
        }
        await this.adapter.commit();
      }
    }

    // 3. Update the schema version tracking table
    await this.adapter.begin();
    await this.driver.schema.version.delete();
    await this.driver.schema.version.create(num - 1);
    await this.adapter.commit();

    if (this.displayBanner) console.log(`[Memori] Build executed successfully!`);
  }
}
