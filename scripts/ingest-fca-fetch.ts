/**
 * FCA Handbook Ingestion — fetch-based (no browser required).
 *
 * Uses the FCA Handbook's public REST API at api-handbook.fca.org.uk
 * to crawl sourcebook structure and provisions without Playwright.
 *
 * Two-phase approach:
 *   Phase 1: Walk the handbook tree via GetAllHandbook and fetch provisions
 *            per section via GetAllHandBookProvisionsSortedOrderByChapter.
 *   Phase 2: Bulk sweep via GetAllProvisionsForAdvanceSearch with pagination
 *            to catch any provisions the tree walk missed.
 *
 * Usage:
 *   npx tsx scripts/ingest-fca-fetch.ts                        # full crawl
 *   npx tsx scripts/ingest-fca-fetch.ts --list-sourcebooks      # print sourcebooks
 *   npx tsx scripts/ingest-fca-fetch.ts --sourcebook SYSC       # single sourcebook
 *   npx tsx scripts/ingest-fca-fetch.ts --resume                # resume from checkpoint
 *   npx tsx scripts/ingest-fca-fetch.ts --search-only           # phase 2 only (bulk sweep)
 *   npx tsx scripts/ingest-fca-fetch.ts --tree-only             # phase 1 only (tree walk)
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

// ── Configuration ────────────────────────────────────────────────────────────

const API_BASE = "https://api-handbook.fca.org.uk";
const DB_PATH = process.env["FCA_DB_PATH"] ?? "data/fca.db";
const PROGRESS_PATH =
  process.env["FCA_PROGRESS_PATH"] ?? "data/ingest-fetch-progress.json";
const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 3;
const SEARCH_PAGE_SIZE = 200;

// ── CLI argument parsing ─────────────────────────────────────────────────────

interface CliArgs {
  listSourcebooks: boolean;
  sourcebook: string | null;
  resume: boolean;
  searchOnly: boolean;
  treeOnly: boolean;
}

function parseArgs(): CliArgs {
  const args = process.argv.slice(2);
  const result: CliArgs = {
    listSourcebooks: false,
    sourcebook: null,
    resume: false,
    searchOnly: false,
    treeOnly: false,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--list-sourcebooks") {
      result.listSourcebooks = true;
    } else if (arg === "--sourcebook" && i + 1 < args.length) {
      result.sourcebook = args[++i]!.toUpperCase();
    } else if (arg === "--resume") {
      result.resume = true;
    } else if (arg === "--search-only") {
      result.searchOnly = true;
    } else if (arg === "--tree-only") {
      result.treeOnly = true;
    } else {
      console.error("Unknown argument: " + String(arg));
      console.error(
        "Usage: npx tsx scripts/ingest-fca-fetch.ts [--list-sourcebooks] [--sourcebook SYSC] [--resume] [--search-only] [--tree-only]",
      );
      process.exit(1);
    }
  }

  return result;
}

// ── Progress tracking ────────────────────────────────────────────────────────

interface Progress {
  /** Sections completed in tree walk (phase 1) */
  completed_sections: string[];
  /** Sourcebooks fully completed in tree walk */
  completed_sourcebooks: string[];
  /** Search offset for bulk sweep (phase 2) */
  search_offset: number;
  /** Whether phase 2 is done */
  search_complete: boolean;
  last_updated: string;
}

function loadProgress(): Progress {
  if (existsSync(PROGRESS_PATH)) {
    try {
      return JSON.parse(readFileSync(PROGRESS_PATH, "utf-8")) as Progress;
    } catch {
      // Corrupted file, start fresh
    }
  }
  return {
    completed_sections: [],
    completed_sourcebooks: [],
    search_offset: 0,
    search_complete: false,
    last_updated: new Date().toISOString(),
  };
}

function saveProgress(progress: Progress): void {
  const dir = dirname(PROGRESS_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  progress.last_updated = new Date().toISOString();
  writeFileSync(PROGRESS_PATH, JSON.stringify(progress, null, 2));
}

// ── HTTP helpers ─────────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function apiFetch<T>(
  path: string,
  params?: Record<string, string>,
  retries: number = MAX_RETRIES,
): Promise<T | null> {
  const url = new URL(path, API_BASE);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      url.searchParams.set(k, v);
    }
  }

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const resp = await fetch(url.toString(), {
        headers: {
          Accept: "application/json",
          "User-Agent": "Ansvar-FCA-Ingestion/1.0",
        },
        signal: AbortSignal.timeout(30_000),
      });

      if (!resp.ok) {
        const text = await resp.text().catch(() => "");
        console.warn(
          `  HTTP ${resp.status} for ${url.pathname} (attempt ${attempt}/${retries}): ${text.slice(0, 200)}`,
        );
        if (attempt < retries) {
          await sleep(RATE_LIMIT_MS * attempt * 2);
          continue;
        }
        return null;
      }

      const json = (await resp.json()) as {
        Error: string | null;
        Result: T;
        Success: boolean;
      };

      if (!json.Success || json.Error) {
        console.warn(
          `  API error for ${url.pathname} (attempt ${attempt}/${retries}): ${json.Error}`,
        );
        if (attempt < retries) {
          await sleep(RATE_LIMIT_MS * attempt);
          continue;
        }
        return null;
      }

      return json.Result;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(
        `  Fetch failed for ${url.pathname} (attempt ${attempt}/${retries}): ${msg}`,
      );
      if (attempt < retries) {
        await sleep(RATE_LIMIT_MS * attempt * 2);
      }
    }
  }
  return null;
}

// ── HTML to plain text ───────────────────────────────────────────────────────

function htmlToText(html: string): string {
  return html
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n\n")
    .replace(/<\/li>/gi, "\n")
    .replace(/<\/tr>/gi, "\n")
    .replace(/<\/div>/gi, "\n")
    .replace(/<[^>]+>/g, "")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]+/g, " ")
    .trim();
}

// ── API response types ───────────────────────────────────────────────────────

interface TreeNode {
  id: number;
  name: string;
  type: string; // "blocks", "sourcebook", "chapter", "section"
  contains: string;
  lastmodifieddate: string;
  entityId: string;
  parentEntityId: string | null;
  parts: TreeNode[];
  isDeleted: boolean | null;
}

interface HandbookTree {
  headers: TreeNode[];
}

interface ChapterProvision {
  entityId: string;
  timeline: string;
  isDeleted: boolean;
  sectionId: number;
  contentText: string;
  contentType: string;
  provisionId: number;
  chapterName: string;
  sectionName: string;
  sectionHeader: string | null;
  sectionFooter: string | null;
  glossaryTerm: string | null;
  subSectionId: number | null;
  provisionName: string;
  provisionType: string;
  provisionTypeId: number;
  subSectionName: string | null;
  handbookProvisionStageId: string | null;
  isApprovedFromStage: boolean | null;
}

interface ChapterResult {
  chapterId: number | null;
  sectionId: number | null;
  chapterName: string | null;
  sectionName: string | null;
  provisions: ChapterProvision[];
}

interface SearchProvision {
  provisionId: number;
  provisionName: string;
  provisionLink: string;
  timeline: string;
  content: string;
  provisionType: string;
}

interface SearchResult {
  provsionDetailsForAdvanceSearch: SearchProvision[];
  totalCount: number;
}

// ── Provision type mapping ───────────────────────────────────────────────────

function mapProvisionType(apiType: string): string {
  const t = apiType.toLowerCase();
  if (t.includes("rule")) return "R";
  if (t.includes("guidance")) return "G";
  if (t.includes("evidential")) return "E";
  if (t.includes("direction")) return "D";
  if (t.includes("principle")) return "P";
  if (t.includes("code")) return "C";
  if (t.includes("uk")) return "UK";
  if (t.includes("eu")) return "EU";
  return apiType.slice(0, 10);
}

// ── Reference normalization ──────────────────────────────────────────────────

/**
 * The tree walk API returns provisionName without type suffix (e.g., "PRIN 2.1.1"),
 * while the FCA convention and the seed data use "PRIN 2.1.1R".
 * The search API wraps names in <em> tags.
 *
 * This function normalises the reference: strips HTML, appends a type suffix
 * if the name ends with a digit and the type maps to a single letter.
 */
const TYPE_SUFFIX_MAP: Record<string, string> = {
  R: "R",
  G: "G",
  E: "E",
  D: "D",
  P: "P",
  C: "C",
};

function normaliseReference(rawName: string, typeCode: string): string {
  let ref = rawName.replace(/<[^>]+>/g, "").trim();
  // FCA provision references look like "SYSC 4.1.1R" where R/G/E/D is the type.
  // The API often returns just "SYSC 4.1.1" without the suffix.
  // Some references end with letters that are NOT type suffixes — e.g., "1.1.1A"
  // means a sub-provision inserted between 1.1.1 and 1.1.2.
  //
  // Only skip appending if the reference ends with a standalone type letter
  // following a digit (e.g., "4.1.1R") which signals a type suffix.
  // Pattern: digit + single R/G/E/D at end = already has suffix.
  if (/\d[RGED]$/.test(ref)) return ref;
  // Append the type suffix if we have a single-letter mapping and ref ends with digit
  const suffix = TYPE_SUFFIX_MAP[typeCode];
  if (suffix && /\d$/.test(ref)) {
    ref = ref + suffix;
  }
  return ref;
}

// ── Sourcebook ID extraction ─────────────────────────────────────────────────

/** Extract sourcebook ID from a provision name like "SYSC 4.1.1R" or "COBS 2.2.3G". */
function extractSourcebookId(provisionName: string): string {
  // Strip HTML tags that might wrap the name (from search API)
  const clean = provisionName.replace(/<[^>]+>/g, "").trim();
  // Match the uppercase prefix before the first space or digit
  const m = clean.match(/^([A-Z][A-Z\-]+)/);
  return m ? m[1] : clean.split(/\s/)[0] ?? clean;
}

/** Extract chapter and section from provision name like "SYSC 4.1.2" -> chapter="4", section="4.1" */
function extractChapterSection(provisionName: string): {
  chapter: string;
  section: string;
} {
  const clean = provisionName.replace(/<[^>]+>/g, "").trim();
  // Match patterns like "SYSC 4.1.2R" or "PRIN 2.1.1" or "SYSC Sch 1.2"
  const m = clean.match(
    /[A-Z\-]+\s+(?:Sch\s+)?(\d+[A-Z]?)(?:\.(\d+))?(?:\.[\d\-]+)?/,
  );
  if (m) {
    const chapter = m[1] ?? "";
    const section = m[2] ? `${m[1]}.${m[2]}` : chapter;
    return { chapter, section };
  }
  // Schedule/Annex patterns like "PRIN Sch 5.3"
  const schM = clean.match(/[A-Z\-]+\s+(Sch|Annex|App)\s+(\d+)/i);
  if (schM) {
    return {
      chapter: `${schM[1]} ${schM[2]}`,
      section: `${schM[1]} ${schM[2]}`,
    };
  }
  return { chapter: "", section: "" };
}

// ── Database operations ──────────────────────────────────────────────────────

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);
  return db;
}

function upsertSourcebook(
  db: Database.Database,
  id: string,
  name: string,
  description: string,
): void {
  db.prepare(
    "INSERT OR REPLACE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
  ).run(id, name, description);
}

function upsertProvision(
  db: Database.Database,
  sourcebookId: string,
  reference: string,
  title: string | null,
  text: string,
  type: string,
  status: string,
  effectiveDate: string | null,
  chapter: string,
  section: string,
): boolean {
  const result = db
    .prepare(
      `INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(sourcebook_id, reference) DO UPDATE SET
         title = excluded.title,
         text = excluded.text,
         type = excluded.type,
         status = excluded.status,
         effective_date = excluded.effective_date,
         chapter = excluded.chapter,
         section = excluded.section
       `,
    )
    .run(
      sourcebookId,
      reference,
      title,
      text,
      type,
      status,
      effectiveDate,
      chapter,
      section,
    );
  return result.changes > 0;
}

function ensureUniqueConstraint(db: Database.Database): void {
  // Add unique constraint if missing (the original schema uses AUTOINCREMENT + INSERT OR IGNORE)
  // We need (sourcebook_id, reference) to be unique for upsert
  try {
    db.exec(
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_provisions_sb_ref ON provisions(sourcebook_id, reference)",
    );
  } catch {
    // Index might already exist under a different name or the column combo might
    // have duplicates from a previous partial run. That's OK — we'll fall back to
    // INSERT OR IGNORE behavior.
  }
}

// ── Phase 1: Tree walk ───────────────────────────────────────────────────────

async function phase1TreeWalk(
  db: Database.Database,
  progress: Progress,
  cli: CliArgs,
): Promise<number> {
  console.log("\n=== Phase 1: Tree walk ===");
  console.log("Fetching handbook structure...");

  const tree = await apiFetch<HandbookTree>("/Handbook/GetAllHandbook");
  if (!tree) {
    console.error("Failed to fetch handbook tree. Aborting phase 1.");
    return 0;
  }

  // Flatten tree: collect all sourcebooks with their chapters and sections
  interface SourcebookEntry {
    id: string;
    name: string;
    blockName: string;
    entityId: string;
    chapters: Array<{
      numericId: number;
      name: string;
      entityId: string;
      sections: Array<{
        numericId: number;
        name: string;
        entityId: string;
      }>;
    }>;
  }

  const sourcebooks: SourcebookEntry[] = [];
  for (const block of tree.headers) {
    for (const sb of block.parts) {
      if (sb.type !== "sourcebook") continue;
      const sbId = sb.contains.toUpperCase();
      sourcebooks.push({
        id: sbId,
        name: sb.name.replace(/^[A-Z\-]+\s+/, ""), // Strip prefix like "PRIN "
        blockName: block.name,
        entityId: sb.entityId,
        chapters: sb.parts
          .filter((ch) => ch.type === "chapter")
          .map((ch) => ({
            numericId: ch.id,
            name: ch.name,
            entityId: ch.entityId,
            sections: ch.parts
              .filter((sec) => sec.type === "section")
              .map((sec) => ({
                numericId: sec.id,
                name: sec.name,
                entityId: sec.entityId,
              })),
          })),
      });
    }
  }

  console.log(
    `Found ${sourcebooks.length} sourcebooks, ` +
      `${sourcebooks.reduce((n, sb) => n + sb.chapters.length, 0)} chapters, ` +
      `${sourcebooks.reduce((n, sb) => n + sb.chapters.reduce((m, ch) => m + ch.sections.length, 0), 0)} sections`,
  );

  if (cli.listSourcebooks) {
    console.log("\nFCA Handbook Sourcebooks:");
    console.log("-".repeat(80));
    let currentBlock = "";
    for (const sb of sourcebooks) {
      if (sb.blockName !== currentBlock) {
        currentBlock = sb.blockName;
        console.log(`\n  ${currentBlock}`);
      }
      const chCount = sb.chapters.length;
      const secCount = sb.chapters.reduce(
        (n, ch) => n + ch.sections.length,
        0,
      );
      console.log(
        `    ${sb.id.padEnd(14)} ${sb.name.padEnd(50)} ${chCount} ch / ${secCount} sec`,
      );
    }
    console.log(`\nTotal: ${sourcebooks.length} sourcebooks`);
    return 0;
  }

  // Filter to requested sourcebook
  const targets = cli.sourcebook
    ? sourcebooks.filter((sb) => sb.id === cli.sourcebook)
    : sourcebooks;

  if (cli.sourcebook && targets.length === 0) {
    console.error(`Sourcebook "${cli.sourcebook}" not found.`);
    console.error(
      "Available: " + sourcebooks.map((sb) => sb.id).join(", "),
    );
    process.exit(1);
  }

  let totalInserted = 0;

  for (const sb of targets) {
    if (
      cli.resume &&
      progress.completed_sourcebooks.includes(sb.id)
    ) {
      console.log(`Skipping ${sb.id} (completed)`);
      continue;
    }

    const fullName = `${sb.id} ${sb.name}`;
    console.log(
      `\nIngesting ${sb.id} (${sb.chapters.length} chapters, ${sb.chapters.reduce((n, ch) => n + ch.sections.length, 0)} sections)`,
    );
    upsertSourcebook(db, sb.id, fullName, sb.blockName);

    let sbInserted = 0;

    for (const ch of sb.chapters) {
      // If chapter has sections, fetch per-section
      if (ch.sections.length > 0) {
        for (const sec of ch.sections) {
          const sectionKey = `${sb.id}/${ch.entityId}/${sec.entityId}`;

          if (
            cli.resume &&
            progress.completed_sections.includes(sectionKey)
          ) {
            continue;
          }

          await sleep(RATE_LIMIT_MS);

          const result = await apiFetch<ChapterResult>(
            `/Handbook/GetAllHandBookProvisionsSortedOrderByChapter/${ch.entityId}`,
            { sectionId: sec.entityId },
          );

          if (!result || !result.provisions) {
            console.warn(
              `  Failed to fetch ${sectionKey}`,
            );
            // Still mark as completed to avoid retrying broken sections forever
            progress.completed_sections.push(sectionKey);
            saveProgress(progress);
            continue;
          }

          let secInserted = 0;
          const tx = db.transaction(() => {
            for (const p of result.provisions) {
              if (!p.provisionName || !p.contentText) continue;

              const type = mapProvisionType(p.provisionType);
              const refClean = normaliseReference(p.provisionName, type);
              const text = p.contentText.trim();
              if (text.length < 5) continue;

              const status = p.isDeleted ? "deleted" : "in_force";
              const effectiveDate = parseDate(p.timeline);
              const { chapter, section } = extractChapterSection(refClean);

              const inserted = upsertProvision(
                db,
                sb.id,
                refClean,
                null,
                text,
                type,
                status,
                effectiveDate,
                chapter,
                section,
              );
              if (inserted) secInserted++;
            }
          });
          tx();

          if (secInserted > 0 || result.provisions.length > 0) {
            console.log(
              `  ${sec.name}: ${result.provisions.length} provisions, ${secInserted} new/updated`,
            );
          }

          sbInserted += secInserted;
          progress.completed_sections.push(sectionKey);
          saveProgress(progress);
        }
      } else {
        // Chapter with no sections — fetch directly
        const chapterKey = `${sb.id}/${ch.entityId}/_`;

        if (
          cli.resume &&
          progress.completed_sections.includes(chapterKey)
        ) {
          continue;
        }

        await sleep(RATE_LIMIT_MS);

        const result = await apiFetch<ChapterResult>(
          `/Handbook/GetAllHandBookProvisionsSortedOrderByChapter/${ch.entityId}`,
        );

        if (result?.provisions) {
          let chInserted = 0;
          const tx = db.transaction(() => {
            for (const p of result.provisions) {
              if (!p.provisionName || !p.contentText) continue;

              const type = mapProvisionType(p.provisionType);
              const refClean = normaliseReference(p.provisionName, type);
              const text = p.contentText.trim();
              if (text.length < 5) continue;

              const status = p.isDeleted ? "deleted" : "in_force";
              const effectiveDate = parseDate(p.timeline);
              const { chapter, section } = extractChapterSection(refClean);

              const inserted = upsertProvision(
                db,
                sb.id,
                refClean,
                null,
                text,
                type,
                status,
                effectiveDate,
                chapter,
                section,
              );
              if (inserted) chInserted++;
            }
          });
          tx();

          if (chInserted > 0) {
            console.log(
              `  ${ch.name}: ${result.provisions.length} provisions, ${chInserted} new/updated`,
            );
          }
          sbInserted += chInserted;
        }

        progress.completed_sections.push(chapterKey);
        saveProgress(progress);
      }
    }

    console.log(`  ${sb.id} total: ${sbInserted} provisions ingested`);
    totalInserted += sbInserted;

    progress.completed_sourcebooks.push(sb.id);
    saveProgress(progress);
  }

  return totalInserted;
}

// ── Phase 2: Bulk search sweep ───────────────────────────────────────────────

async function phase2SearchSweep(
  db: Database.Database,
  progress: Progress,
  cli: CliArgs,
): Promise<number> {
  console.log("\n=== Phase 2: Bulk search sweep ===");

  if (cli.resume && progress.search_complete) {
    console.log("Search sweep already complete (from checkpoint).");
    return 0;
  }

  let offset = cli.resume ? progress.search_offset : 0;
  let totalInserted = 0;
  let totalSeen = 0;

  // First request to get total count
  const firstPage = await apiFetch<SearchResult>(
    "/Handbook/GetAllProvisionsForAdvanceSearch",
    {
      SearchTerm: "*",
      SkipCount: "0",
      MaxResultCount: "1",
    },
  );

  if (!firstPage) {
    console.error("Failed to fetch search results. Aborting phase 2.");
    return 0;
  }

  const totalCount = firstPage.totalCount;
  console.log(
    `Total provisions in API: ${totalCount} (starting from offset ${offset})`,
  );

  // Track sourcebooks discovered during search to auto-register them
  const knownSourcebooks = new Set<string>();
  const sbRows = db
    .prepare("SELECT id FROM sourcebooks")
    .all() as Array<{ id: string }>;
  for (const row of sbRows) {
    knownSourcebooks.add(row.id);
  }

  while (offset < totalCount) {
    await sleep(RATE_LIMIT_MS);

    const page = await apiFetch<SearchResult>(
      "/Handbook/GetAllProvisionsForAdvanceSearch",
      {
        SearchTerm: "*",
        SkipCount: offset.toString(),
        MaxResultCount: SEARCH_PAGE_SIZE.toString(),
      },
    );

    if (!page || !page.provsionDetailsForAdvanceSearch) {
      console.warn(`  Failed at offset ${offset}. Retrying next iteration.`);
      // Don't advance offset — retry on next loop
      await sleep(RATE_LIMIT_MS * 3);
      // But do advance to avoid infinite loop on persistent failures
      offset += SEARCH_PAGE_SIZE;
      progress.search_offset = offset;
      saveProgress(progress);
      continue;
    }

    const items = page.provsionDetailsForAdvanceSearch;
    if (items.length === 0) break;

    let pageInserted = 0;

    const tx = db.transaction(() => {
      for (const item of items) {
        const type = mapProvisionType(item.provisionType);
        const refClean = normaliseReference(item.provisionName, type);
        if (!refClean) continue;

        const sbId = extractSourcebookId(refClean);
        if (!sbId) continue;

        // Auto-register sourcebook if not seen
        if (!knownSourcebooks.has(sbId)) {
          upsertSourcebook(db, sbId, sbId, "");
          knownSourcebooks.add(sbId);
        }

        // Extract text from HTML content
        const text = htmlToText(item.content);
        if (text.length < 5) continue;

        const effectiveDate = parseDate(item.timeline);
        const { chapter, section } = extractChapterSection(refClean);

        const inserted = upsertProvision(
          db,
          sbId,
          refClean,
          null,
          text,
          type,
          "in_force",
          effectiveDate,
          chapter,
          section,
        );
        if (inserted) pageInserted++;
      }
    });
    tx();

    totalSeen += items.length;
    totalInserted += pageInserted;
    offset += items.length;

    const pct = ((offset / totalCount) * 100).toFixed(1);
    console.log(
      `  [${pct}%] offset=${offset}/${totalCount} — page: ${items.length} items, ${pageInserted} new/updated`,
    );

    progress.search_offset = offset;
    saveProgress(progress);
  }

  progress.search_complete = true;
  saveProgress(progress);

  console.log(
    `Search sweep complete: ${totalSeen} provisions seen, ${totalInserted} new/updated`,
  );
  return totalInserted;
}

// ── Date parsing ─────────────────────────────────────────────────────────────

/** Parse DD/MM/YYYY to ISO YYYY-MM-DD. Returns null on failure. */
function parseDate(dateStr: string | null | undefined): string | null {
  if (!dateStr) return null;
  // DD/MM/YYYY format from API
  const m = dateStr.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (m) {
    return `${m[3]}-${m[2]}-${m[1]}`;
  }
  // Already ISO?
  if (/^\d{4}-\d{2}-\d{2}/.test(dateStr)) {
    return dateStr.slice(0, 10);
  }
  return null;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const cli = parseArgs();
  const db = initDb();
  ensureUniqueConstraint(db);

  const progress = cli.resume ? loadProgress() : loadProgress();

  // Quick connectivity check
  console.log("Testing FCA API connectivity...");
  const testResult = await apiFetch<{ id: string }>(
    "/TitleNote/GetTitleNote",
  );
  if (!testResult) {
    console.error(
      "Cannot reach FCA Handbook API at " + API_BASE + ". Check connectivity.",
    );
    process.exit(1);
  }
  console.log("API reachable.");

  let phase1Count = 0;
  let phase2Count = 0;

  // Phase 1: Tree walk
  if (!cli.searchOnly) {
    phase1Count = await phase1TreeWalk(db, progress, cli);
    if (cli.listSourcebooks) {
      db.close();
      return;
    }
  }

  // Phase 2: Bulk search sweep (catches anything the tree walk missed)
  if (!cli.treeOnly && !cli.listSourcebooks) {
    phase2Count = await phase2SearchSweep(db, progress, cli);
  }

  // Summary
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

  console.log("\n" + "=".repeat(60));
  console.log("Ingestion complete.");
  console.log(`  Phase 1 (tree walk):    ${phase1Count} provisions`);
  console.log(`  Phase 2 (search sweep): ${phase2Count} provisions`);
  console.log(`  Total provisions in DB: ${provisionCount}`);
  console.log(`  Total sourcebooks in DB: ${sourcebookCount}`);
  console.log(`  Database: ${DB_PATH}`);

  db.close();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
