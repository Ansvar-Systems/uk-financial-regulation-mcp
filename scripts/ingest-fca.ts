/**
 * FCA Handbook Ingestion — Playwright-based crawler.
 *
 * Navigates the FCA Handbook (handbook.fca.org.uk) using a headless browser,
 * extracts sourcebook structure and provisions from rendered Angular pages,
 * and stores them in the SQLite database.
 *
 * The FCA Handbook is an Angular SPA with WASM-based request signing that
 * prevents direct API access. Playwright renders the pages so we can extract
 * from the DOM after Angular hydration.
 *
 * Usage:
 *   npx tsx scripts/ingest-fca.ts                       # full crawl
 *   npx tsx scripts/ingest-fca.ts --list-sourcebooks     # discover and print sourcebooks
 *   npx tsx scripts/ingest-fca.ts --sourcebook SYSC      # ingest single sourcebook
 *   npx tsx scripts/ingest-fca.ts --resume               # resume from last checkpoint
 *   npx tsx scripts/ingest-fca.ts --sourcebook SYSC --chapter 3  # single chapter
 */

import { chromium } from "playwright";
import type { Browser, Page } from "playwright";
import Database from "better-sqlite3";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

// ── Configuration ────────────────────────────────────────────────────────────

const BASE_URL = "https://www.handbook.fca.org.uk";
const DB_PATH = process.env["FCA_DB_PATH"] ?? "data/fca.db";
const PROGRESS_PATH =
  process.env["FCA_PROGRESS_PATH"] ?? "data/ingest-progress.json";
const RATE_LIMIT_MS = 1500; // 1.5s between page loads
const PAGE_TIMEOUT_MS = 30_000;
const MAX_RETRIES = 3;

// ── CLI argument parsing ─────────────────────────────────────────────────────

interface CliArgs {
  listSourcebooks: boolean;
  sourcebook: string | null;
  chapter: string | null;
  resume: boolean;
}

function parseArgs(): CliArgs {
  const args = process.argv.slice(2);
  const result: CliArgs = {
    listSourcebooks: false,
    sourcebook: null,
    chapter: null,
    resume: false,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--list-sourcebooks") {
      result.listSourcebooks = true;
    } else if (arg === "--sourcebook" && i + 1 < args.length) {
      result.sourcebook = args[++i]!.toUpperCase();
    } else if (arg === "--chapter" && i + 1 < args.length) {
      result.chapter = args[++i]!;
    } else if (arg === "--resume") {
      result.resume = true;
    } else {
      console.error("Unknown argument: " + String(arg));
      console.error(
        "Usage: npx tsx scripts/ingest-fca.ts [--list-sourcebooks] [--sourcebook SYSC] [--chapter 3] [--resume]",
      );
      process.exit(1);
    }
  }

  return result;
}

// ── Progress tracking ────────────────────────────────────────────────────────

interface Progress {
  completed_sourcebooks: string[];
  completed_chapters: Record<string, string[]>; // sourcebook -> chapter[]
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
    completed_sourcebooks: [],
    completed_chapters: {},
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

// ── Rate limiting ────────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function navigateWithRetry(
  page: Page,
  url: string,
  retries: number = MAX_RETRIES,
): Promise<boolean> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      await page.goto(url, {
        waitUntil: "networkidle",
        timeout: PAGE_TIMEOUT_MS,
      });
      // Wait for Angular to render content
      await page.waitForTimeout(2000);
      return true;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(
        "  Attempt " + attempt + "/" + retries + " failed for " + url + ": " + msg,
      );
      if (attempt < retries) {
        await sleep(RATE_LIMIT_MS * attempt);
      }
    }
  }
  return false;
}

// ── Sourcebook discovery ─────────────────────────────────────────────────────

interface SourcebookInfo {
  id: string;
  name: string;
  url: string;
  description: string;
}

async function discoverSourcebooks(page: Page): Promise<SourcebookInfo[]> {
  console.log("Discovering sourcebooks from handbook index...");

  const ok = await navigateWithRetry(page, BASE_URL + "/handbook");
  if (!ok) {
    throw new Error("Failed to load handbook index page");
  }

  // The handbook page lists sourcebooks as links. Extract them from the DOM.
  const sourcebooks = await page.evaluate(() => {
    const results: Array<{
      id: string;
      name: string;
      url: string;
      description: string;
    }> = [];

    // Look for links that match sourcebook patterns.
    const links = document.querySelectorAll("a[href]");

    for (const link of links) {
      const href = link.getAttribute("href") ?? "";
      const text = (link.textContent ?? "").trim();

      // Match handbook sourcebook links: /handbook/XXXX or just XXXX
      const match = href.match(/\/handbook\/([A-Z]{2,10})(?:\/|$)/);
      if (match?.[1] && text.length > 0) {
        const id = match[1];
        if (!results.some((r) => r.id === id)) {
          results.push({
            id,
            name: text,
            url: href.startsWith("http") ? href : location.origin + href,
            description: "",
          });
        }
      }
    }

    // Also try the navigation sidebar if it exists
    const navItems = document.querySelectorAll(
      '[class*="nav"] a, [class*="toc"] a, [class*="menu"] a, [class*="sidebar"] a',
    );
    for (const item of navItems) {
      const href = item.getAttribute("href") ?? "";
      const text = (item.textContent ?? "").trim();
      const match = href.match(/\/handbook\/([A-Z]{2,10})(?:\/|$)/);
      if (match?.[1] && text.length > 0) {
        const id = match[1];
        if (!results.some((r) => r.id === id)) {
          results.push({
            id,
            name: text,
            url: href.startsWith("http") ? href : location.origin + href,
            description: "",
          });
        }
      }
    }

    return results;
  });

  // If dynamic extraction found nothing, use known sourcebooks as fallback
  if (sourcebooks.length === 0) {
    console.log(
      "  Dynamic extraction found no sourcebooks. Using known FCA sourcebook list.",
    );
    return getKnownSourcebooks();
  }

  console.log("  Found " + sourcebooks.length + " sourcebooks");
  return sourcebooks;
}

/**
 * Fallback list of known FCA Handbook sourcebooks.
 * Used when the Angular SPA does not render navigable links in a way
 * that the DOM extractor can parse.
 */
function getKnownSourcebooks(): SourcebookInfo[] {
  const sbs: Array<{ id: string; name: string; desc: string }> = [
    { id: "PRIN", name: "Principles for Businesses", desc: "The fundamental obligations that apply to every FCA-authorised firm." },
    { id: "SYSC", name: "Senior Management Arrangements, Systems and Controls", desc: "Requirements for governance, risk management, compliance, and internal controls." },
    { id: "COBS", name: "Conduct of Business Sourcebook", desc: "Rules on how firms conduct business with customers." },
    { id: "MCOB", name: "Mortgages and Home Finance: Conduct of Business", desc: "Rules for mortgage lending and home finance." },
    { id: "ICOBS", name: "Insurance: Conduct of Business Sourcebook", desc: "Rules for insurance distribution activities." },
    { id: "BCOBS", name: "Banking: Conduct of Business Sourcebook", desc: "Rules for retail banking conduct." },
    { id: "MAR", name: "Market Conduct Sourcebook", desc: "Rules on market abuse and trading behaviour." },
    { id: "SUP", name: "Supervision Manual", desc: "The FCA's approach to supervision and reporting." },
    { id: "DEPP", name: "Decision Procedure and Penalties Manual", desc: "Procedures for enforcement decisions and penalty setting." },
    { id: "EG", name: "Enforcement Guide", desc: "The FCA's approach to enforcement." },
    { id: "AUTH", name: "Authorisation Manual", desc: "Application and authorisation procedures." },
    { id: "FIT", name: "Fit and Proper Test for Employees and Senior Personnel", desc: "Criteria for assessing fitness and propriety." },
    { id: "GEN", name: "General Provisions", desc: "Interpretation, communication, and waiver of rules." },
    { id: "FEES", name: "Fees Manual", desc: "Periodic fee requirements and application fees." },
    { id: "COLL", name: "Collective Investment Schemes", desc: "Rules for authorised fund managers and depositaries." },
    { id: "FUND", name: "Investment Funds Sourcebook", desc: "Rules for alternative investment fund managers." },
    { id: "BIPRU", name: "Prudential Sourcebook for Banks, Building Societies and Investment Firms", desc: "Prudential requirements (partly superseded by CRR)." },
    { id: "IFPRU", name: "Prudential Sourcebook for Investment Firms", desc: "Prudential requirements for MiFID investment firms." },
    { id: "MIFIDPRU", name: "Prudential Sourcebook for MiFID Investment Firms", desc: "Post-Brexit prudential regime for investment firms." },
    { id: "GENPRU", name: "General Prudential Sourcebook", desc: "General prudential standards." },
    { id: "CASS", name: "Client Assets Sourcebook", desc: "Rules on holding and safeguarding client money and assets." },
    { id: "COCON", name: "Code of Conduct Sourcebook", desc: "Individual conduct rules under the SM&CR." },
    { id: "CONC", name: "Consumer Credit Sourcebook", desc: "Rules for consumer credit activities." },
    { id: "DISP", name: "Dispute Resolution: Complaints", desc: "Complaint handling rules and ombudsman referrals." },
    { id: "DTR", name: "Disclosure Guidance and Transparency Rules", desc: "Disclosure and transparency requirements for issuers." },
    { id: "LR", name: "Listing Rules", desc: "Rules for companies listed on UK regulated markets." },
    { id: "PR", name: "Prospectus Regulation Rules", desc: "Rules on prospectus requirements." },
    { id: "PERG", name: "Perimeter Guidance Manual", desc: "Guidance on the regulatory perimeter." },
    { id: "TC", name: "Training and Competence Sourcebook", desc: "Competence requirements for employees giving advice." },
    { id: "APER", name: "Statements of Principle and Code of Practice for Approved Persons", desc: "Conduct standards for approved persons (being replaced by COCON)." },
    { id: "COMP", name: "Compensation Sourcebook", desc: "FSCS compensation scheme rules." },
    { id: "INSPRU", name: "Prudential Sourcebook for Insurers", desc: "Prudential requirements for insurers (pre-Solvency II)." },
    { id: "IPRU-INV", name: "Interim Prudential Sourcebook for Investment Businesses", desc: "Legacy prudential rules for investment businesses." },
  ];

  return sbs.map((s) => ({
    id: s.id,
    name: s.name,
    url: BASE_URL + "/handbook/" + s.id,
    description: s.desc,
  }));
}

// ── Chapter discovery ────────────────────────────────────────────────────────

interface ChapterInfo {
  id: string;
  title: string;
  url: string;
}

async function discoverChapters(
  page: Page,
  sourcebook: SourcebookInfo,
): Promise<ChapterInfo[]> {
  console.log("  Discovering chapters for " + sourcebook.id + "...");

  const ok = await navigateWithRetry(page, sourcebook.url);
  if (!ok) {
    console.warn("  Failed to load sourcebook page for " + sourcebook.id);
    return [];
  }

  const sbId = sourcebook.id;
  const chapters = await page.evaluate((evalSbId: string) => {
    const results: Array<{ id: string; title: string; url: string }> = [];
    const links = document.querySelectorAll("a[href]");

    for (const link of links) {
      const href = link.getAttribute("href") ?? "";
      const text = (link.textContent ?? "").trim();

      // Match chapter links: /handbook/SYSC/3 or /handbook/SYSC/3/...
      const chapterPattern = new RegExp(
        "\\/handbook\\/" + evalSbId + "\\/(\\d+[A-Z]?)(?:\\/|$|#)",
      );
      const match = href.match(chapterPattern);
      if (match?.[1] && text.length > 0) {
        const id = match[1];
        if (!results.some((r) => r.id === id)) {
          results.push({
            id,
            title: text.replace(/\s+/g, " ").substring(0, 200),
            url: href.startsWith("http")
              ? href
              : location.origin + href,
          });
        }
      }
    }

    return results;
  }, sbId);

  if (chapters.length === 0) {
    // Try alternative: look for numbered sections in the page content
    const altChapters = await page.evaluate((evalSbId: string) => {
      const results: Array<{ id: string; title: string; url: string }> = [];
      const allText = document.body.innerText;
      const chapterRegex = new RegExp(evalSbId + "\\s+(\\d+[A-Z]?)\\b", "g");
      const seen = new Set<string>();
      let m: RegExpExecArray | null;
      while ((m = chapterRegex.exec(allText)) !== null) {
        if (m[1] && !seen.has(m[1])) {
          seen.add(m[1]);
          results.push({
            id: m[1],
            title: "Chapter " + m[1],
            url: location.origin + "/handbook/" + evalSbId + "/" + m[1],
          });
        }
      }
      return results;
    }, sbId);

    if (altChapters.length > 0) {
      console.log(
        "  Found " + altChapters.length + " chapters via text extraction",
      );
      return altChapters;
    }
  }

  console.log("  Found " + chapters.length + " chapters");
  return chapters;
}

// ── Provision extraction ─────────────────────────────────────────────────────

interface ExtractedProvision {
  reference: string;
  title: string;
  text: string;
  type: string; // R, G, E, D
  chapter: string;
  section: string;
}

async function extractProvisions(
  page: Page,
  sourcebook: SourcebookInfo,
  chapter: ChapterInfo,
): Promise<ExtractedProvision[]> {
  // Try loading the chapter page directly
  const chapterUrl = BASE_URL + "/handbook/" + sourcebook.id + "/" + chapter.id;
  const ok = await navigateWithRetry(page, chapterUrl);
  if (!ok) {
    console.warn(
      "  Failed to load chapter " + sourcebook.id + " " + chapter.id,
    );
    return [];
  }

  const sbId = sourcebook.id;
  const chId = chapter.id;

  const provisions = await page.evaluate(
    (params: { sbId: string; chId: string }) => {
      const results: Array<{
        reference: string;
        title: string;
        text: string;
        type: string;
        chapter: string;
        section: string;
      }> = [];

      // Strategy 1: Look for provision containers with structured content
      // FCA handbook typically renders provisions in distinct blocks
      const provisionElements = document.querySelectorAll(
        '[class*="provision"], [class*="rule"], [class*="guidance"], [class*="para"], [data-reference]',
      );

      for (const el of provisionElements) {
        const ref =
          el.getAttribute("data-reference") ??
          el.querySelector('[class*="ref"], [class*="number"]')
            ?.textContent?.trim() ??
          "";
        const text = (el.textContent ?? "").trim();

        if (ref && text.length > 10) {
          const typeSuffix = ref.match(/([RGED])$/)?.[1] ?? "R";
          const sectionMatch = ref.match(
            new RegExp(params.sbId + "\\s+(\\d+[A-Z]?\\.\\d+)"),
          );
          results.push({
            reference: ref.replace(/\s+/g, " "),
            title: "",
            text: text.substring(0, 10000),
            type: typeSuffix,
            chapter: params.chId,
            section: sectionMatch ? sectionMatch[1]! : params.chId,
          });
        }
      }

      // Strategy 2: Look for numbered paragraphs matching the FCA pattern
      if (results.length === 0) {
        const refPattern = new RegExp(
          "(" + params.sbId + "\\s+\\d+[A-Z]?\\.\\d+\\.\\d+[RGED]?)",
          "g",
        );
        const bodyText = document.body.innerHTML;

        // Split content around provision references
        const segments = bodyText.split(refPattern);
        for (let i = 1; i < segments.length; i += 2) {
          const ref = (segments[i] ?? "").trim();
          const rawContent = segments[i + 1] ?? "";

          // Strip HTML tags to get text
          const tempDiv = document.createElement("div");
          tempDiv.innerHTML = rawContent;
          let text = (tempDiv.textContent ?? "").trim();

          // Trim to next reference or reasonable length
          const nextRefPattern = new RegExp(
            params.sbId + "\\s+\\d+[A-Z]?\\.\\d+\\.\\d+[RGED]?",
          );
          const nextRefIdx = text.search(nextRefPattern);
          if (nextRefIdx > 0) {
            text = text.substring(0, nextRefIdx).trim();
          }
          text = text.substring(0, 10000);

          if (ref && text.length > 5) {
            const typeSuffix = ref.match(/([RGED])$/)?.[1] ?? "R";
            const sectionMatch = ref.match(/(\d+[A-Z]?\.\d+)/);
            results.push({
              reference: ref.replace(/\s+/g, " "),
              title: "",
              text,
              type: typeSuffix,
              chapter: params.chId,
              section: sectionMatch ? sectionMatch[1]! : params.chId,
            });
          }
        }
      }

      // Strategy 3: Extract all visible text blocks with reference-like headers
      if (results.length === 0) {
        const headings = document.querySelectorAll(
          "h1, h2, h3, h4, h5, h6, [role='heading'], strong, b",
        );
        for (const h of headings) {
          const hText = (h.textContent ?? "").trim();
          const refMatch = hText.match(
            new RegExp(
              "(" +
                params.sbId +
                "\\s+\\d+[A-Z]?\\.\\d+(?:\\.\\d+)?[RGED]?)",
            ),
          );
          if (refMatch?.[1]) {
            // Get the next sibling text content as the provision text
            let provText = "";
            let sibling = h.nextElementSibling;
            while (sibling) {
              const sibText = (sibling.textContent ?? "").trim();
              // Stop if we hit another reference
              if (
                sibText.match(
                  new RegExp(
                    params.sbId +
                      "\\s+\\d+[A-Z]?\\.\\d+(?:\\.\\d+)?[RGED]?",
                  ),
                )
              ) {
                break;
              }
              provText += " " + sibText;
              sibling = sibling.nextElementSibling;
            }
            provText = provText.trim().substring(0, 10000);

            if (provText.length > 5) {
              const ref = refMatch[1];
              const typeSuffix = ref.match(/([RGED])$/)?.[1] ?? "R";
              const sectionMatch = ref.match(/(\d+[A-Z]?\.\d+)/);
              results.push({
                reference: ref.replace(/\s+/g, " "),
                title: "",
                text: provText,
                type: typeSuffix,
                chapter: params.chId,
                section: sectionMatch ? sectionMatch[1]! : params.chId,
              });
            }
          }
        }
      }

      return results;
    },
    { sbId, chId },
  );

  // Deduplicate by reference
  const seen = new Set<string>();
  const unique: ExtractedProvision[] = [];
  for (const p of provisions) {
    if (!seen.has(p.reference)) {
      seen.add(p.reference);
      unique.push(p);
    }
  }

  return unique;
}

// ── Section page handling ────────────────────────────────────────────────────

async function discoverSections(
  page: Page,
  sourcebook: SourcebookInfo,
  chapter: ChapterInfo,
): Promise<string[]> {
  const sbId = sourcebook.id;
  const chId = chapter.id;

  const sectionUrls = await page.evaluate(
    (params: { sbId: string; chId: string }) => {
      const results: string[] = [];
      const links = document.querySelectorAll("a[href]");

      for (const link of links) {
        const href = link.getAttribute("href") ?? "";
        const sectionPattern = new RegExp(
          "\\/handbook\\/" +
            params.sbId +
            "\\/" +
            params.chId +
            "\\/(\\d+)(?:\\/|$|#)",
        );
        const match = href.match(sectionPattern);
        if (match) {
          const url = href.startsWith("http")
            ? href
            : location.origin + href;
          if (!results.includes(url)) {
            results.push(url);
          }
        }
      }

      return results;
    },
    { sbId, chId },
  );

  return sectionUrls;
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
  sb: SourcebookInfo,
): void {
  db.prepare(
    "INSERT OR REPLACE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
  ).run(sb.id, sb.name, sb.description);
}

function insertProvisions(
  db: Database.Database,
  sourcebookId: string,
  provisions: ExtractedProvision[],
): number {
  const insert = db.prepare(
    "INSERT OR IGNORE INTO provisions " +
      "(sourcebook_id, reference, title, text, type, status, effective_date, chapter, section) " +
      "VALUES (?, ?, ?, ?, ?, 'in_force', NULL, ?, ?)",
  );

  let count = 0;
  const tx = db.transaction(() => {
    for (const p of provisions) {
      const result = insert.run(
        sourcebookId,
        p.reference,
        p.title || null,
        p.text,
        p.type,
        p.chapter,
        p.section,
      );
      if (result.changes > 0) count++;
    }
  });

  tx();
  return count;
}

// ── Main ingestion flow ──────────────────────────────────────────────────────

async function main(): Promise<void> {
  const cli = parseArgs();
  const db = initDb();

  console.log("Launching Playwright browser...");
  const browser: Browser = await chromium.launch({
    headless: true,
    args: ["--disable-blink-features=AutomationControlled"],
  });

  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    viewport: { width: 1920, height: 1080 },
  });

  const page = await context.newPage();

  try {
    // ── Step 1: Discover sourcebooks ──────────────────────────────────────
    const sourcebooks = await discoverSourcebooks(page);

    if (cli.listSourcebooks) {
      console.log("\nFCA Handbook Sourcebooks:");
      console.log("-".repeat(70));
      for (const sb of sourcebooks) {
        console.log("  " + sb.id.padEnd(12) + " " + sb.name);
      }
      console.log("\nTotal: " + sourcebooks.length + " sourcebooks");
      return;
    }

    // Filter to requested sourcebook if specified
    const targetSourcebooks = cli.sourcebook
      ? sourcebooks.filter((sb) => sb.id === cli.sourcebook)
      : sourcebooks;

    if (cli.sourcebook && targetSourcebooks.length === 0) {
      console.error('Sourcebook "' + cli.sourcebook + '" not found.');
      console.error(
        "Available: " + sourcebooks.map((sb) => sb.id).join(", "),
      );
      process.exit(1);
    }

    // ── Step 2: Load progress ────────────────────────────────────────────
    const progress = cli.resume ? loadProgress() : loadProgress();

    let totalProvisions = 0;

    // ── Step 3: Ingest each sourcebook ───────────────────────────────────
    for (const sb of targetSourcebooks) {
      if (
        cli.resume &&
        progress.completed_sourcebooks.includes(sb.id)
      ) {
        console.log("Skipping " + sb.id + " (already completed)");
        continue;
      }

      console.log("\nIngesting sourcebook: " + sb.id + " -- " + sb.name);
      upsertSourcebook(db, sb);

      await sleep(RATE_LIMIT_MS);

      // Discover chapters
      const chapters = await discoverChapters(page, sb);

      if (chapters.length === 0) {
        console.log(
          "  No chapters found for " + sb.id + ". Trying direct extraction...",
        );
        // Try extracting from the sourcebook page directly
        const directProvisions = await extractProvisions(page, sb, {
          id: "1",
          title: sb.name,
          url: sb.url,
        });
        if (directProvisions.length > 0) {
          const inserted = insertProvisions(db, sb.id, directProvisions);
          console.log(
            "  Direct extraction: " +
              directProvisions.length +
              " found, " +
              inserted +
              " new",
          );
          totalProvisions += inserted;
        }
        continue;
      }

      // Filter to requested chapter if specified
      const targetChapters = cli.chapter
        ? chapters.filter((ch) => ch.id === cli.chapter)
        : chapters;

      for (const ch of targetChapters) {
        const completedChapters =
          progress.completed_chapters[sb.id] ?? [];
        if (cli.resume && completedChapters.includes(ch.id)) {
          console.log(
            "  Skipping " +
              sb.id +
              " chapter " +
              ch.id +
              " (already completed)",
          );
          continue;
        }

        await sleep(RATE_LIMIT_MS);

        const provisions = await extractProvisions(page, sb, ch);

        if (provisions.length > 0) {
          const inserted = insertProvisions(db, sb.id, provisions);
          console.log(
            "  Ingesting " +
              sb.id +
              " chapter " +
              ch.id +
              "... " +
              provisions.length +
              " provisions found, " +
              inserted +
              " new",
          );
          totalProvisions += inserted;
        } else {
          console.log(
            "  Ingesting " +
              sb.id +
              " chapter " +
              ch.id +
              "... 0 provisions (page may not have rendered)",
          );

          // Try section-level pages
          const sectionUrls = await discoverSections(page, sb, ch);
          for (const sectionUrl of sectionUrls) {
            await sleep(RATE_LIMIT_MS);
            const sectionOk = await navigateWithRetry(page, sectionUrl);
            if (sectionOk) {
              const sectionProvisions = await extractProvisions(
                page,
                sb,
                ch,
              );
              if (sectionProvisions.length > 0) {
                const sInserted = insertProvisions(
                  db,
                  sb.id,
                  sectionProvisions,
                );
                console.log(
                  "    Section " +
                    sectionUrl +
                    ": " +
                    sectionProvisions.length +
                    " provisions, " +
                    sInserted +
                    " new",
                );
                totalProvisions += sInserted;
              }
            }
          }
        }

        // Save progress after each chapter
        if (!progress.completed_chapters[sb.id]) {
          progress.completed_chapters[sb.id] = [];
        }
        progress.completed_chapters[sb.id]!.push(ch.id);
        saveProgress(progress);
      }

      // Mark sourcebook as complete
      if (!cli.chapter) {
        progress.completed_sourcebooks.push(sb.id);
        saveProgress(progress);
      }
    }

    // ── Summary ──────────────────────────────────────────────────────────
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

    console.log("\n" + "-".repeat(50));
    console.log("Ingestion complete.");
    console.log("  New provisions inserted: " + totalProvisions);
    console.log("  Total provisions in DB:  " + provisionCount);
    console.log("  Total sourcebooks in DB: " + sourcebookCount);
  } finally {
    await browser.close();
    db.close();
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
