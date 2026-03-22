/**
 * FCA Handbook Ingestion Script
 *
 * TODO: Implement crawler for the FCA Handbook.
 *
 * The FCA Handbook is available at:
 *   https://www.handbook.fca.org.uk/
 *
 * Planned ingestion strategy (pending research):
 *
 * 1. Sourcebooks — crawl the handbook index to discover all active sourcebooks
 *    (SYSC, COBS, MAR, PRIN, FEES, etc.) and insert into `sourcebooks` table.
 *
 * 2. Provisions — for each sourcebook, crawl chapters and sections. Extract:
 *    - reference (e.g., "SYSC 3.2.1R")
 *    - title
 *    - full text
 *    - type suffix: R (rule), G (guidance), E (evidential provision), D (direction)
 *    - status: in_force | deleted | not_yet_in_force
 *    - effective_date
 *
 *    The FCA publishes machine-readable versions via:
 *    - FCA Handbook XML/JSON API (if available — check https://www.handbook.fca.org.uk/instrument)
 *    - HTML scraping of https://www.handbook.fca.org.uk/handbook/{SOURCEBOOK}
 *
 * 3. Enforcement actions — crawl FCA Final Notices at:
 *    https://www.fca.org.uk/news/final-notices
 *    Extract firm name, reference number, action type, amount, date, summary.
 *
 * 4. FTS population — FTS5 triggers handle index updates automatically on INSERT.
 *
 * 5. Incremental updates — track last_modified per sourcebook, skip unchanged.
 *
 * Notes:
 * - Respect FCA robots.txt and rate limits (add ~500ms delay between requests).
 * - The FCA Handbook changes frequently; plan for weekly automated re-ingestion.
 * - PRA Rulebook (https://www.prarulebook.co.uk/) is a separate corpus — consider
 *   a separate sourcebook namespace prefix (e.g., "PRA-") or a second DB table.
 *
 * Run once research is complete:
 *   npx tsx scripts/ingest.ts
 */

console.log("FCA Handbook ingestion not yet implemented. See TODO comments above.");
process.exit(0);
