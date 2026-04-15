import fs from "node:fs/promises";
import path from "node:path";

export type ModelRow = {
  model_name: string;
  training_version: string;
  family: string;
  prior_beta_sd: number | null;
  prior_int_mean: number | null;
  train_time_min: number | null;
  mae: number | null;
  rmse: number | null;
  mape: number | null;
  coverage: number | null;
  waic: number | null;
  loo_ic: number | null;
};

const CSV_FILES = [
  "brms_grid_search_v1.csv",
  "brms_grid_search_v2.csv",
  "brms_grid_search_v3.csv",
];

function parseCsvLine(line: string): string[] {
  const out: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    if (char === '"') {
      inQuotes = !inQuotes;
      continue;
    }
    if (char === "," && !inQuotes) {
      out.push(current);
      current = "";
      continue;
    }
    current += char;
  }
  out.push(current);
  return out;
}

function toNum(value?: string): number | null {
  if (!value || value === "NA") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function inferVersion(row: Record<string, string>): string {
  if (row.training_version) return row.training_version;
  const model = row.model_name ?? "";
  const match = model.match(/_v(\d+)$/);
  return match ? `v${match[1]}` : "unknown";
}

function parseCsvContent(content: string): ModelRow[] {
  const lines = content.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];

  const headers = parseCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const values = parseCsvLine(line);
    const row = Object.fromEntries(headers.map((h, i) => [h, values[i] ?? ""]));
    return {
      model_name: row.model_name,
      training_version: inferVersion(row),
      family: row.family,
      prior_beta_sd: toNum(row.prior_beta_sd),
      prior_int_mean: toNum(row.prior_int_mean),
      train_time_min: toNum(row.train_time_min),
      mae: toNum(row.mae),
      rmse: toNum(row.rmse),
      mape: toNum(row.mape),
      coverage: toNum(row.coverage),
      waic: toNum(row.waic),
      loo_ic: toNum(row.loo_ic),
    };
  });
}

export async function getModelResults(): Promise<ModelRow[]> {
  const metricsDir = path.join(process.cwd(), "..", "..", "results", "metrics");
  const allRows = await Promise.all(
    CSV_FILES.map(async (fileName) => {
      const content = await fs.readFile(path.join(metricsDir, fileName), "utf8");
      return parseCsvContent(content);
    }),
  );
  return allRows.flat();
}
