#!/usr/bin/env node

/**
 * UK Financial Regulation MCP — stdio entry point.
 *
 * Provides MCP tools for querying the FCA Handbook: provisions, sourcebooks,
 * enforcement actions, and currency checks.
 *
 * Tool prefix: gb_fin_
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import {
  listSourcebooks,
  searchProvisions,
  getProvision,
  searchEnforcement,
  checkProvisionCurrency,
} from "./db.js";
import { buildCitation } from "./utils/citation.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let pkgVersion = "0.1.0";
try {
  const pkg = JSON.parse(
    readFileSync(join(__dirname, "..", "package.json"), "utf8"),
  ) as { version: string };
  pkgVersion = pkg.version;
} catch {
  // fallback to default
}

const SERVER_NAME = "uk-financial-regulation-mcp";

// ─── Tool definitions ────────────────────────────────────────────────────────

const TOOLS = [
  {
    name: "gb_fin_search_regulations",
    description:
      "Full-text search across FCA Handbook provisions. Returns matching rules, guidance, evidential provisions, and directions.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Search query (e.g., 'conflicts of interest', 'suitability assessment')",
        },
        sourcebook: {
          type: "string",
          description: "Filter by sourcebook ID (e.g., SYSC, COBS, MAR). Optional.",
        },
        status: {
          type: "string",
          enum: ["in_force", "deleted", "not_yet_in_force"],
          description: "Filter by provision status. Defaults to all statuses.",
        },
        limit: {
          type: "number",
          description: "Maximum number of results to return. Defaults to 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "gb_fin_get_regulation",
    description:
      "Get a specific FCA Handbook provision by sourcebook and reference. Accepts references like 'SYSC 3.2.1R' or 'COBS 9.2.1G'.",
    inputSchema: {
      type: "object" as const,
      properties: {
        sourcebook: {
          type: "string",
          description: "Sourcebook identifier (e.g., SYSC, COBS, MAR, PRIN)",
        },
        reference: {
          type: "string",
          description: "Full provision reference (e.g., 'SYSC 3.2.1R', 'COBS 9.2.1G', 'SYSC 3.2.1')",
        },
      },
      required: ["sourcebook", "reference"],
    },
  },
  {
    name: "gb_fin_list_sourcebooks",
    description:
      "List all FCA Handbook sourcebooks with their names and descriptions.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
  {
    name: "gb_fin_search_enforcement",
    description:
      "Search FCA enforcement actions — Final Notices, fines, bans, and restrictions. Returns matching enforcement decisions.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Search query (e.g., firm name, type of breach, 'market abuse')",
        },
        action_type: {
          type: "string",
          enum: ["fine", "ban", "restriction", "warning"],
          description: "Filter by action type. Optional.",
        },
        limit: {
          type: "number",
          description: "Maximum number of results to return. Defaults to 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "gb_fin_check_currency",
    description:
      "Check whether a specific FCA Handbook provision reference is currently in force. Returns status and effective date.",
    inputSchema: {
      type: "object" as const,
      properties: {
        reference: {
          type: "string",
          description: "Full provision reference to check (e.g., 'SYSC 3.2.1R')",
        },
      },
      required: ["reference"],
    },
  },
  {
    name: "gb_fin_about",
    description: "Return metadata about this MCP server: version, data source, tool list.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
  {
    name: "gb_fin_list_sources",
    description:
      "List all primary data sources used by this MCP server, including source URLs, issuing organisations, and update information.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
];

// ─── Zod schemas for argument validation ────────────────────────────────────

const SearchRegulationsArgs = z.object({
  query: z.string().min(1),
  sourcebook: z.string().optional(),
  status: z.enum(["in_force", "deleted", "not_yet_in_force"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const GetRegulationArgs = z.object({
  sourcebook: z.string().min(1),
  reference: z.string().min(1),
});

const SearchEnforcementArgs = z.object({
  query: z.string().min(1),
  action_type: z.enum(["fine", "ban", "restriction", "warning"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const CheckCurrencyArgs = z.object({
  reference: z.string().min(1),
});

// ─── Helper ──────────────────────────────────────────────────────────────────

function textContent(data: unknown) {
  return {
    content: [
      { type: "text" as const, text: JSON.stringify(data, null, 2) },
    ],
  };
}

function errorContent(message: string) {
  return {
    content: [{ type: "text" as const, text: message }],
    isError: true as const,
  };
}

// ─── Server setup ────────────────────────────────────────────────────────────

const server = new Server(
  { name: SERVER_NAME, version: pkgVersion },
  { capabilities: { tools: {} } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: TOOLS,
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args = {} } = request.params;

  try {
    switch (name) {
      case "gb_fin_search_regulations": {
        const parsed = SearchRegulationsArgs.parse(args);
        const results = searchProvisions({
          query: parsed.query,
          sourcebook: parsed.sourcebook,
          status: parsed.status,
          limit: parsed.limit,
        });
        return textContent({ results, count: results.length });
      }

      case "gb_fin_get_regulation": {
        const parsed = GetRegulationArgs.parse(args);
        const provision = getProvision(parsed.sourcebook, parsed.reference);
        if (!provision) {
          return errorContent(
            `Provision not found: ${parsed.sourcebook} ${parsed.reference}`,
          );
        }
        const _citation = buildCitation(
          `${parsed.sourcebook} ${parsed.reference}`,
          (provision as Record<string, unknown>).title as string || `${parsed.sourcebook} ${parsed.reference}`,
          "gb_fin_get_regulation",
          { sourcebook: parsed.sourcebook, reference: parsed.reference },
        );
        return textContent({ ...provision as Record<string, unknown>, _citation });
      }

      case "gb_fin_list_sourcebooks": {
        const sourcebooks = listSourcebooks();
        return textContent({ sourcebooks, count: sourcebooks.length });
      }

      case "gb_fin_search_enforcement": {
        const parsed = SearchEnforcementArgs.parse(args);
        const results = searchEnforcement({
          query: parsed.query,
          action_type: parsed.action_type,
          limit: parsed.limit,
        });
        return textContent({ results, count: results.length });
      }

      case "gb_fin_check_currency": {
        const parsed = CheckCurrencyArgs.parse(args);
        const currency = checkProvisionCurrency(parsed.reference);
        return textContent(currency);
      }

      case "gb_fin_about": {
        return textContent({
          name: SERVER_NAME,
          version: pkgVersion,
          description:
            "UK Financial Conduct Authority (FCA) Handbook MCP server. Provides access to FCA rules, guidance, evidential provisions, and enforcement actions.",
          data_source: "FCA Handbook (https://www.handbook.fca.org.uk/)",
          tools: TOOLS.map((t) => ({ name: t.name, description: t.description })),
        });
      }

      case "gb_fin_list_sources": {
        return textContent({
          sources: [
            {
              name: "FCA Handbook",
              url: "https://www.handbook.fca.org.uk/",
              organisation: "Financial Conduct Authority",
              description:
                "FCA rules, guidance, evidential provisions (PRIN, SYSC, COBS, MCOB, etc.)",
            },
            {
              name: "FCA Register",
              url: "https://register.fca.org.uk/",
              organisation: "Financial Conduct Authority",
              description: "Authorised firms and individuals",
            },
            {
              name: "FCA Final Notices",
              url: "https://www.fca.org.uk/news/final-notices",
              organisation: "Financial Conduct Authority",
              description: "Enforcement actions, fines, bans",
            },
            {
              name: "PRA Rulebook",
              url: "https://www.prarulebook.co.uk/",
              organisation: "Prudential Regulation Authority",
              description: "Prudential rules for banks and insurers",
            },
            {
              name: "UK Listing Rules",
              url: "https://www.handbook.fca.org.uk/handbook/UKLR",
              organisation: "Financial Conduct Authority",
              description: "Listing standards for public companies",
            },
            {
              name: "Financial Services and Markets Act 2000",
              url: "https://www.legislation.gov.uk/ukpga/2000/8/contents",
              organisation: "UK Parliament",
              description: "Primary UK financial services statute",
            },
          ],
          count: 6,
        });
      }

      default:
        return errorContent(`Unknown tool: ${name}`);
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return errorContent(`Error executing ${name}: ${message}`);
  }
});

// ─── Main ────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  process.stderr.write(`${SERVER_NAME} v${pkgVersion} running on stdio\n`);
}

main().catch((err) => {
  process.stderr.write(`Fatal error: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
});
