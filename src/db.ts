import pg from "pg";
import { config } from "./config.js";

const { Pool } = pg;

export const db = new Pool({
  connectionString: config.DATABASE_URL,
  max: 10,
  ssl: {
    rejectUnauthorized: false
  },
  application_name: "email-verification-engine"
});

export async function closeDb() {
  await db.end();
}
