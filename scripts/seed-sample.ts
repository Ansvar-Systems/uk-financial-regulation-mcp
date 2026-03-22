/**
 * Seed the FCA Handbook database with sample provisions for testing.
 *
 * Inserts well-known provisions from PRIN, SYSC, and COBS sourcebooks
 * so MCP tools can be tested without running the full Playwright crawl.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force   # drop and recreate
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["FCA_DB_PATH"] ?? "data/fca.db";
const force = process.argv.includes("--force");

// ── Bootstrap database ───────────────────────────────────────────────────────

const dir = dirname(DB_PATH);
if (!existsSync(dir)) {
  mkdirSync(dir, { recursive: true });
}

if (force && existsSync(DB_PATH)) {
  unlinkSync(DB_PATH);
  console.log(`Deleted existing database at ${DB_PATH}`);
}

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);

console.log(`Database initialised at ${DB_PATH}`);

// ── Sourcebooks ──────────────────────────────────────────────────────────────

interface SourcebookRow {
  id: string;
  name: string;
  description: string;
}

const sourcebooks: SourcebookRow[] = [
  {
    id: "PRIN",
    name: "Principles for Businesses",
    description:
      "The eleven fundamental obligations that apply to every FCA-authorised firm.",
  },
  {
    id: "SYSC",
    name: "Senior Management Arrangements, Systems and Controls",
    description:
      "Requirements for governance, risk management, compliance, and internal controls.",
  },
  {
    id: "COBS",
    name: "Conduct of Business Sourcebook",
    description:
      "Rules and guidance on how firms should conduct business with customers, covering disclosures, suitability, and best execution.",
  },
  {
    id: "MAR",
    name: "Market Conduct Sourcebook",
    description:
      "Rules on market abuse, trading behaviour, and market manipulation prevention.",
  },
  {
    id: "SUP",
    name: "Supervision Manual",
    description:
      "The FCA's approach to supervision, reporting requirements, and waivers.",
  },
  {
    id: "MCOB",
    name: "Mortgages and Home Finance: Conduct of Business",
    description:
      "Rules for mortgage lending, home purchase plans, and regulated sale and rent back agreements.",
  },
  {
    id: "FEES",
    name: "Fees Manual",
    description:
      "Periodic fee requirements, application fees, and the Financial Ombudsman Service levy.",
  },
  {
    id: "GEN",
    name: "General Provisions",
    description:
      "Interpreting provisions, references to writing and electronic communication, and waiver of rules.",
  },
];

const insertSourcebook = db.prepare(
  "INSERT OR IGNORE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
);

for (const sb of sourcebooks) {
  insertSourcebook.run(sb.id, sb.name, sb.description);
}

console.log(`Inserted ${sourcebooks.length} sourcebooks`);

// ── Sample provisions ────────────────────────────────────────────────────────

interface ProvisionRow {
  sourcebook_id: string;
  reference: string;
  title: string;
  text: string;
  type: string;
  status: string;
  effective_date: string;
  chapter: string;
  section: string;
}

const provisions: ProvisionRow[] = [
  // ── PRIN — Principles for Businesses ───────────────────────────────────
  {
    sourcebook_id: "PRIN",
    reference: "PRIN 2.1.1R",
    title: "Principle 1 — Integrity",
    text: "A firm must conduct its business with integrity.",
    type: "R",
    status: "in_force",
    effective_date: "2001-12-01",
    chapter: "2",
    section: "2.1",
  },
  {
    sourcebook_id: "PRIN",
    reference: "PRIN 2.1.2R",
    title: "Principle 2 — Skill, care and diligence",
    text: "A firm must conduct its business with due skill, care and diligence.",
    type: "R",
    status: "in_force",
    effective_date: "2001-12-01",
    chapter: "2",
    section: "2.1",
  },
  {
    sourcebook_id: "PRIN",
    reference: "PRIN 2.1.3R",
    title: "Principle 3 — Management and control",
    text: "A firm must take reasonable care to organise and control its affairs responsibly and effectively, with adequate risk management systems.",
    type: "R",
    status: "in_force",
    effective_date: "2001-12-01",
    chapter: "2",
    section: "2.1",
  },
  {
    sourcebook_id: "PRIN",
    reference: "PRIN 2.1.4R",
    title: "Principle 4 — Financial prudence",
    text: "A firm must maintain adequate financial resources.",
    type: "R",
    status: "in_force",
    effective_date: "2001-12-01",
    chapter: "2",
    section: "2.1",
  },
  {
    sourcebook_id: "PRIN",
    reference: "PRIN 2.1.6R",
    title: "Principle 6 — Customers' interests",
    text: "A firm must pay due regard to the interests of its customers and treat them fairly.",
    type: "R",
    status: "in_force",
    effective_date: "2001-12-01",
    chapter: "2",
    section: "2.1",
  },
  {
    sourcebook_id: "PRIN",
    reference: "PRIN 2.1.7R",
    title: "Principle 7 — Communications with clients",
    text: "A firm must pay due regard to the information needs of its clients, and communicate information to them in a way which is clear, fair and not misleading.",
    type: "R",
    status: "in_force",
    effective_date: "2001-12-01",
    chapter: "2",
    section: "2.1",
  },
  {
    sourcebook_id: "PRIN",
    reference: "PRIN 2.1.9R",
    title: "Principle 9 — Customers: relationships of trust",
    text: "A firm must take reasonable care to ensure the suitability of its advice and discretionary decisions for any customer who is entitled to rely upon its judgment.",
    type: "R",
    status: "in_force",
    effective_date: "2001-12-01",
    chapter: "2",
    section: "2.1",
  },
  {
    sourcebook_id: "PRIN",
    reference: "PRIN 2.1.11R",
    title: "Principle 11 — Relations with regulators",
    text: "A firm must deal with its regulators in an open and cooperative way, and must disclose to the appropriate regulator appropriately anything relating to the firm of which that regulator would reasonably expect notice.",
    type: "R",
    status: "in_force",
    effective_date: "2001-12-01",
    chapter: "2",
    section: "2.1",
  },
  {
    sourcebook_id: "PRIN",
    reference: "PRIN 2A.1.1R",
    title: "Consumer Duty — Cross-cutting obligation: act in good faith",
    text: "A firm must act in good faith towards retail customers.",
    type: "R",
    status: "in_force",
    effective_date: "2023-07-31",
    chapter: "2A",
    section: "2A.1",
  },

  // ── SYSC — Senior Management Arrangements ──────────────────────────────
  {
    sourcebook_id: "SYSC",
    reference: "SYSC 3.1.1R",
    title: "Systems and controls",
    text: "A firm must take reasonable care to establish and maintain such systems and controls as are appropriate to its business.",
    type: "R",
    status: "in_force",
    effective_date: "2001-12-01",
    chapter: "3",
    section: "3.1",
  },
  {
    sourcebook_id: "SYSC",
    reference: "SYSC 3.2.6R",
    title: "Risk assessment",
    text: "A firm must take reasonable care to establish and maintain effective systems and controls for compliance with applicable requirements and standards under the regulatory system and for countering the risk that the firm might be used to further financial crime.",
    type: "R",
    status: "in_force",
    effective_date: "2001-12-01",
    chapter: "3",
    section: "3.2",
  },
  {
    sourcebook_id: "SYSC",
    reference: "SYSC 4.1.1R",
    title: "General organisational requirements",
    text: "A firm must have robust governance arrangements, which include a clear organisational structure with well-defined, transparent and consistent lines of responsibility, effective processes to identify, manage, monitor and report the risks it is or might be exposed to, and internal control mechanisms, including sound administrative and accounting procedures and effective control and safeguard arrangements for information processing systems.",
    type: "R",
    status: "in_force",
    effective_date: "2007-11-01",
    chapter: "4",
    section: "4.1",
  },
  {
    sourcebook_id: "SYSC",
    reference: "SYSC 6.1.1R",
    title: "Compliance function",
    text: "A firm must establish, implement and maintain adequate policies and procedures sufficient to ensure compliance of the firm including its managers, employees and appointed representatives (or where applicable, tied agents) with its obligations under the regulatory system and for countering the risk that the firm might be used to further financial crime. A firm must also establish, implement and maintain adequate policies and procedures for the detection of any risk of failure by the firm to comply with its obligations under the regulatory system as well as associated risks, and put in place adequate measures and procedures designed to minimise such risks and to enable the appropriate regulator to exercise its powers effectively under the regulatory system.",
    type: "R",
    status: "in_force",
    effective_date: "2007-11-01",
    chapter: "6",
    section: "6.1",
  },
  {
    sourcebook_id: "SYSC",
    reference: "SYSC 6.1.2R",
    title: "Compliance function responsibilities",
    text: "A firm must, taking into account the nature, scale and complexity of its business, and the nature and range of financial services and activities undertaken in the course of that business, establish, implement and maintain a permanent and effective compliance function which operates independently and which has the responsibility for monitoring and, on a regular basis, assessing the adequacy and effectiveness of the measures and procedures put in place in accordance with SYSC 6.1.1R, and the actions taken to address any deficiencies in the firm's compliance with its obligations, and advising and assisting the relevant persons responsible for carrying out regulated activities to comply with the firm's obligations under the regulatory system.",
    type: "R",
    status: "in_force",
    effective_date: "2007-11-01",
    chapter: "6",
    section: "6.1",
  },
  {
    sourcebook_id: "SYSC",
    reference: "SYSC 6.3.1R",
    title: "Financial crime",
    text: "A firm must ensure the policies and procedures established under SYSC 6.1.1R include adequate policies and procedures for countering the risk that the firm might be used to further financial crime.",
    type: "R",
    status: "in_force",
    effective_date: "2007-11-01",
    chapter: "6",
    section: "6.3",
  },

  // ── COBS — Conduct of Business ─────────────────────────────────────────
  {
    sourcebook_id: "COBS",
    reference: "COBS 2.1.1R",
    title: "Acting honestly, fairly and professionally",
    text: "A firm must act honestly, fairly and professionally in accordance with the best interests of its client (the client's best interests rule).",
    type: "R",
    status: "in_force",
    effective_date: "2007-11-01",
    chapter: "2",
    section: "2.1",
  },
  {
    sourcebook_id: "COBS",
    reference: "COBS 4.2.1R",
    title: "Fair, clear and not misleading communications",
    text: "A firm must ensure that a communication or a financial promotion is fair, clear and not misleading.",
    type: "R",
    status: "in_force",
    effective_date: "2007-11-01",
    chapter: "4",
    section: "4.2",
  },
  {
    sourcebook_id: "COBS",
    reference: "COBS 9.2.1R",
    title: "Suitability — assessing suitability",
    text: "A firm must take reasonable steps to ensure that a personal recommendation, or a decision to trade, is suitable for its client.",
    type: "R",
    status: "in_force",
    effective_date: "2007-11-01",
    chapter: "9",
    section: "9.2",
  },
  {
    sourcebook_id: "COBS",
    reference: "COBS 11.2.1R",
    title: "Best execution — obligation",
    text: "A firm must take all sufficient steps to obtain, when executing orders, the best possible result for its clients taking into account the execution factors.",
    type: "R",
    status: "in_force",
    effective_date: "2007-11-01",
    chapter: "11",
    section: "11.2",
  },
  {
    sourcebook_id: "COBS",
    reference: "COBS 14.3.2R",
    title: "Key Features Document",
    text: "A firm that sells a packaged product or offers to sell a packaged product to a retail client must provide the client with a key features document, unless an exception applies.",
    type: "R",
    status: "in_force",
    effective_date: "2007-11-01",
    chapter: "14",
    section: "14.3",
  },
];

const insertProvision = db.prepare(`
  INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertAll = db.transaction(() => {
  for (const p of provisions) {
    insertProvision.run(
      p.sourcebook_id,
      p.reference,
      p.title,
      p.text,
      p.type,
      p.status,
      p.effective_date,
      p.chapter,
      p.section,
    );
  }
});

insertAll();

console.log(`Inserted ${provisions.length} sample provisions`);

// ── Sample enforcement actions ───────────────────────────────────────────────

interface EnforcementRow {
  firm_name: string;
  reference_number: string;
  action_type: string;
  amount: number;
  date: string;
  summary: string;
  sourcebook_references: string;
}

const enforcements: EnforcementRow[] = [
  {
    firm_name: "Barclays Bank Plc",
    reference_number: "122702",
    action_type: "fine",
    amount: 72_069_400,
    date: "2015-11-26",
    summary:
      "Fined for failing to control business practices in its foreign exchange trading operations. The firm failed to adequately manage conflicts of interest between itself and its clients as well as the risk that traders could engage in trading which amounted to market abuse.",
    sourcebook_references: "PRIN 2.1.1R, SYSC 6.1.1R",
  },
  {
    firm_name: "Credit Suisse International",
    reference_number: "150006",
    action_type: "fine",
    amount: 147_190_276,
    date: "2023-10-24",
    summary:
      "Fined for serious failures in its management of the Archegos Capital Management risk. The firm failed to properly manage its exposure to a single counterparty and did not have appropriate risk management systems and controls.",
    sourcebook_references: "PRIN 2.1.3R, SYSC 4.1.1R",
  },
  {
    firm_name: "Citigroup Global Markets Limited",
    reference_number: "124384",
    action_type: "fine",
    amount: 225_575_000,
    date: "2024-05-22",
    summary:
      "Fined for failures in its trading controls. Traders were able to execute a series of trades that were significantly in excess of their approved risk limits due to deficiencies in the firm's systems and controls.",
    sourcebook_references: "SYSC 4.1.1R, SYSC 6.1.1R",
  },
];

const insertEnforcement = db.prepare(`
  INSERT INTO enforcement_actions (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

const insertEnforcementsAll = db.transaction(() => {
  for (const e of enforcements) {
    insertEnforcement.run(
      e.firm_name,
      e.reference_number,
      e.action_type,
      e.amount,
      e.date,
      e.summary,
      e.sourcebook_references,
    );
  }
});

insertEnforcementsAll();

console.log(`Inserted ${enforcements.length} sample enforcement actions`);

// ── Summary ──────────────────────────────────────────────────────────────────

const provisionCount = (
  db.prepare("SELECT count(*) as cnt FROM provisions").get() as {
    cnt: number;
  }
).cnt;
const sourcebookCount = (
  db.prepare("SELECT count(*) as cnt FROM sourcebooks").get() as {
    cnt: number;
  }
).cnt;
const enforcementCount = (
  db.prepare("SELECT count(*) as cnt FROM enforcement_actions").get() as {
    cnt: number;
  }
).cnt;
const ftsCount = (
  db.prepare("SELECT count(*) as cnt FROM provisions_fts").get() as {
    cnt: number;
  }
).cnt;

console.log(`\nDatabase summary:`);
console.log(`  Sourcebooks:          ${sourcebookCount}`);
console.log(`  Provisions:           ${provisionCount}`);
console.log(`  Enforcement actions:  ${enforcementCount}`);
console.log(`  FTS entries:          ${ftsCount}`);
console.log(`\nDone. Database ready at ${DB_PATH}`);

db.close();
