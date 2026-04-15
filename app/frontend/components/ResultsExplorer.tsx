"use client";

import { useMemo, useState } from "react";
import type { ModelRow } from "../lib/model-results";

type MetricKey = "rmse" | "mae" | "coverage" | "loo_ic" | "train_time_min";

const MODEL_NAME_MAP: Record<string, string> = {
  "brms_beta0.5_int3.5_negbinomial_iter1000": "Beta(0.5, 3.5) NB",
  "brms_beta1.0_int3.0_negbinomial_iter1000": "Beta(1.0, 3.0) NB",
  "brms_beta1.0_int3.5_negbinomial_iter1000": "Beta(1.0, 3.5) NB",
  "brms_beta1.0_int3.5_poisson_iter1000": "Beta(1.0, 3.5) POIS",
};

const METRIC_LABELS: Record<MetricKey, string> = {
  rmse: "RMSE (lower is better)",
  mae: "MAE (lower is better)",
  coverage: "Coverage (higher is better)",
  loo_ic: "LOO-IC (lower is better)",
  train_time_min: "Train Time (min, lower is better)",
};

function formatValue(value: number | null): string {
  if (value === null) return "NA";
  if (Math.abs(value) >= 1000) return value.toExponential(2);
  return value.toFixed(2);
}

function displayModelName(modelName: string): string {
  const baseName = modelName.replace(/_v\d+$/, "");
  return MODEL_NAME_MAP[baseName] ?? modelName;
}

export function ResultsExplorer({ rows }: { rows: ModelRow[] }) {
  const [metric, setMetric] = useState<MetricKey>("rmse");
  const [family, setFamily] = useState("all");
  const [version, setVersion] = useState("all");

  const families = useMemo(
    () => ["all", ...new Set(rows.map((r) => r.family).filter(Boolean))],
    [rows],
  );
  const versions = useMemo(
    () => ["all", ...new Set(rows.map((r) => r.training_version).filter(Boolean))],
    [rows],
  );

  const filtered = useMemo(() => {
    return rows.filter((row) => {
      if (family !== "all" && row.family !== family) return false;
      if (version !== "all" && row.training_version !== version) return false;
      return row[metric] !== null;
    });
  }, [rows, family, version, metric]);

  const higherIsBetter = metric === "coverage";

  const sorted = useMemo(() => {
    const cloned = [...filtered];
    cloned.sort((a, b) => {
      const av = a[metric] ?? (higherIsBetter ? Number.NEGATIVE_INFINITY : Number.POSITIVE_INFINITY);
      const bv = b[metric] ?? (higherIsBetter ? Number.NEGATIVE_INFINITY : Number.POSITIVE_INFINITY);
      return higherIsBetter ? bv - av : av - bv;
    });
    return cloned;
  }, [filtered, metric, higherIsBetter]);

  const best = sorted[0];
  const minVal = Math.min(...sorted.map((r) => r[metric] as number));
  const maxVal = Math.max(...sorted.map((r) => r[metric] as number));
  const denom = Math.max(maxVal - minVal, 1e-9);

  return (
    <div className="card">
      <h2>Interactive Model Comparison</h2>
      <p className="muted">
        Explore BRMS grid-search outputs exported from notebook runs (v1-v3).
        Filter subsets and switch your target metric to see how rankings shift.
      </p>

      <div className="controls">
        <div className="control">
          <label htmlFor="metric">Metric</label>
          <select
            id="metric"
            value={metric}
            onChange={(e) => setMetric(e.target.value as MetricKey)}
          >
            {(Object.keys(METRIC_LABELS) as MetricKey[]).map((key) => (
              <option key={key} value={key}>
                {METRIC_LABELS[key]}
              </option>
            ))}
          </select>
        </div>

        <div className="control">
          <label htmlFor="family">Family</label>
          <select id="family" value={family} onChange={(e) => setFamily(e.target.value)}>
            {families.map((f) => (
              <option key={f} value={f}>
                {f}
              </option>
            ))}
          </select>
        </div>

        <div className="control">
          <label htmlFor="version">Version</label>
          <select id="version" value={version} onChange={(e) => setVersion(e.target.value)}>
            {versions.map((v) => (
              <option key={v} value={v}>
                {v}
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className="kpi-grid" style={{ marginTop: "1rem" }}>
        <div className="kpi">
          <p>Models in view</p>
          <h3>{sorted.length}</h3>
        </div>
        <div className="kpi">
          <p>Best model</p>
          <h3>{best ? displayModelName(best.model_name) : "None"}</h3>
        </div>
        <div className="kpi">
          <p>Best metric value</p>
          <h3 className="good">{best ? formatValue(best[metric]) : "NA"}</h3>
        </div>
      </div>

      <div className="grid-2" style={{ marginTop: "1rem" }}>
        <section className="card" style={{ margin: 0 }}>
          <h3 style={{ marginTop: 0 }}>Ranked Bar View</h3>
          <ul className="bar-list">
            {sorted.map((row) => {
              const value = row[metric] as number;
              const normalized = higherIsBetter
                ? (value - minVal) / denom
                : (maxVal - value) / denom;
              const width = 20 + normalized * 80;
              return (
                <li key={`${row.model_name}-${row.training_version}`} className="bar-row">
                  <div className="bar-meta">
                    <span title={row.model_name}>{displayModelName(row.model_name)}</span>
                    <strong>{formatValue(value)}</strong>
                  </div>
                  <div className="bar-track">
                    <div className="bar-fill" style={{ width: `${width}%` }} />
                  </div>
                </li>
              );
            })}
          </ul>
        </section>

        <section className="card" style={{ margin: 0, overflowX: "auto" }}>
          <h3 style={{ marginTop: 0 }}>Detailed Table</h3>
          <table className="data-table">
            <thead>
              <tr>
                <th>Model</th>
                <th>Version</th>
                <th>Family</th>
                <th>Metric</th>
                <th>Coverage</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((row) => (
                <tr key={`${row.model_name}-${row.training_version}`}>
                  <td title={row.model_name}>{displayModelName(row.model_name)}</td>
                  <td>
                    <span className="pill">{row.training_version}</span>
                  </td>
                  <td>{row.family}</td>
                  <td>{formatValue(row[metric])}</td>
                  <td>{formatValue(row.coverage)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      </div>
    </div>
  );
}
