import 'dotenv/config';
import mysql from 'mysql2/promise';
import { OpenAI } from 'openai';
import { Memori } from '../../src/index.js';

async function runMysqlTest() {
  const conn = await mysql.createConnection({
    host: 'localhost',
    user: 'memori',
    database: 'memori_test',
    password: 'memori',
    port: 3307 // Mapped in your Docker Compose
  });

  const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const mem = new Memori({ conn }).llm.register(client);
  mem.attribution("mysql-user", "mysql-test");

  await mem.config.storage!.build();

  await client.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [{ role: "user", content: "I am allergic to peanuts." }],
  });

  await mem.engine.waitForAugmentation();

  const response = await client.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [{ role: "user", content: "Is there anything I shouldn't eat?" }],
  });
  console.log(`🤖 AI: ${response.choices[0].message.content}`);

  await mem.config.storage!.close();
}

runMysqlTest().catch(console.error);