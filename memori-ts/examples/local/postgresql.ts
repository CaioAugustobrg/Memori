import 'dotenv/config';
import pg from 'pg';
import { OpenAI } from 'openai';
import { Memori } from '../../src/index.js';

const pool = new pg.Pool({
  user: 'memori',
  host: 'localhost',
  database: 'memori_test',
  password: 'memori',
  port: 5432,
});

async function runPostgresTest() {
  const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const mem = new Memori({ conn: pool }).llm.register(client);
  mem.attribution("pg-user", "pg-test");

  await mem.config.storage!.build();

  await client.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [{ role: "user", content: "The secret password for the vault is 'RAINBOW-DASH'." }],
  });

  await mem.engine.waitForAugmentation();

  const response = await client.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [{ role: "user", content: "What is the secret password for the vault?" }],
  });
  console.log(`🤖 AI: ${response.choices[0].message.content}`);

  await mem.config.storage!.close();
}

runPostgresTest().catch(console.error);