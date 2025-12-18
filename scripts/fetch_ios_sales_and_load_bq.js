import { BigQuery } from "@google-cloud/bigquery";
import fs from "node:fs";
import path from "node:path";
import https from "node:https";
import zlib from "node:zlib";
import jwt from "jsonwebtoken";

// ===== env =====
const projectId = process.env.BQ_PROJECT;
const dataset = process.env.BQ_DATASET;
const tableDaily = process.env.BQ_TABLE_DAILY;

// BigQueryのロケーション（USのdatasetを使うなら "US" にするのが安全）
const location = process.env.BQ_LOCATION || "US";

const ASC_ISSUER_ID = process.env.ASC_ISSUER_ID;
const ASC_KEY_ID = process.env.ASC_KEY_ID;
const ASC_PRIVATE_KEY = process.env.ASC_PRIVATE_KEY;
const ASC_VENDOR_NUMBER = process.env.ASC_VENDOR_NUMBER;

if (!projectId || !dataset || !tableDaily) {
  throw new Error("Missing env: BQ_PROJECT / BQ_DATASET / BQ_TABLE_DAILY");
}
if (!ASC_ISSUER_ID || !ASC_KEY_ID || !ASC_PRIVATE_KEY || !ASC_VENDOR_NUMBER) {
  throw new Error("Missing env: ASC_ISSUER_ID / ASC_KEY_ID / ASC_PRIVATE_KEY / ASC_VENDOR_NUMBER");
}

// ===== utils =====
function getArg(name, def = null) {
  const idx = process.argv.indexOf(`--${name}`);
  if (idx === -1) return def;
  return process.argv[idx + 1] ?? def;
}

// SalesレポートはPacific基準で「昨日」を取るのが安定しがち
function ymdInLosAngeles(daysAgo = 1) {
  const now = new Date();
  const la = new Date(now.toLocaleString("en-US", { timeZone: "America/Los_Angeles" }));
  la.setDate(la.getDate() - daysAgo);
  const y = la.getFullYear();
  const m = String(la.getMonth() + 1).padStart(2, "0");
  const d = String(la.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function httpGet(url, headers) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { headers }, (res) => {
      const chunks = [];
      res.on("data", (d) => chunks.push(d));
      res.on("end", () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
    });
    req.on("error", reject);
  });
}

function makeJwt() {
  const privateKey = ASC_PRIVATE_KEY.replace(/\\n/g, "\n");
  const now = Math.floor(Date.now() / 1000);
  return jwt.sign(
    { iss: ASC_ISSUER_ID, aud: "appstoreconnect-v1", exp: now + 20 * 60 },
    privateKey,
    { algorithm: "ES256", header: { kid: ASC_KEY_ID, typ: "JWT" } }
  );
}

function loadApps() {
  const appsPath = path.resolve("config/apps.json");
  const apps = JSON.parse(fs.readFileSync(appsPath, "utf-8"));
  if (!Array.isArray(apps)) throw new Error("apps.json must be an array");

  const bundleToName = new Map(); // bundleId -> appName
  const skuToBundle = new Map();  // sku -> bundleId

  for (const a of apps) {
    if (!a.app_name) throw new Error(`Invalid app entry (missing app_name): ${JSON.stringify(a)}`);

    if (a.ios_bundle_id) bundleToName.set(a.ios_bundle_id, a.app_name);

    // ★ここが肝：SKUでbundleIdへ
    if (a.ios_sku && a.ios_bundle_id) {
      skuToBundle.set(String(a.ios_sku).trim(), String(a.ios_bundle_id).trim());
    }
  }

  return { bundleToName, skuToBundle };
}

function parseTsv(buf) {
  const text = buf.toString("utf8");
  const lines = text.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];

  const header = lines[0].split("\t").map((h) => h.trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split("\t");
    const o = {};
    header.forEach((h, idx) => (o[h] = (cols[idx] ?? "").trim()));
    rows.push(o);
  }
  return rows;
}

function pickFirst(obj, keys) {
  for (const k of keys) {
    if (obj[k] !== undefined && obj[k] !== null && String(obj[k]).trim() !== "") return obj[k];
  }
  return "";
}

function toInt(x) {
  const s = String(x ?? "0").replace(/,/g, "").trim();
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

async function fetchSalesReportTsv(token, reportDate) {
  const params = new URLSearchParams({
    "filter[frequency]": "DAILY",
    "filter[reportType]": "SALES",
    "filter[reportSubType]": "SUMMARY",
    "filter[vendorNumber]": ASC_VENDOR_NUMBER,
    "filter[reportDate]": reportDate
  });

  const url = `https://api.appstoreconnect.apple.com/v1/salesReports?${params.toString()}`;
  const r = await httpGet(url, {
    Authorization: `Bearer ${token}`,
    Accept: "application/a-gzip, application/octet-stream"
  });

  if (r.status !== 200) {
    throw new Error(`salesReports failed: ${r.status} ${r.body.toString("utf8")}`);
  }

  // gzip展開（非gzipの時もあるのでtry）
  try {
    return zlib.gunzipSync(r.body);
  } catch {
    return r.body;
  }
}

async function mergeToBigQuery(bigquery, tableFqdn, date, rows, source) {
  // rows: [{store, app_id, app_name, country_group, downloads}]
  const query = `
    MERGE ${tableFqdn} T
    USING (
      SELECT
        DATE(@date) AS date,
        r.store,
        r.app_id,
        r.app_name,
        r.country_group,
        r.downloads,
        @source AS source,
        CURRENT_TIMESTAMP() AS ingested_at
      FROM UNNEST(@rows) AS r
    ) S
    ON
      T.date = S.date
      AND T.store = S.store
      AND T.app_id = S.app_id
      AND T.country_group = S.country_group
    WHEN MATCHED THEN
      UPDATE SET
        T.app_name = S.app_name,
        T.downloads = S.downloads,
        T.source = S.source,
        T.ingested_at = S.ingested_at
    WHEN NOT MATCHED THEN
      INSERT (date, store, app_id, app_name, country_group, downloads, source, ingested_at)
      VALUES (S.date, S.store, S.app_id, S.app_name, S.country_group, S.downloads, S.source, S.ingested_at)
  `;

  const [job] = await bigquery.createQueryJob({
    query,
    location,
    params: { date, source, rows }
  });
  await job.getQueryResults();
}

async function main() {
  const mode = getArg("mode", "latest"); // latest | backfill
  const days = Number(getArg("days", "1"));
  const explicitDate = getArg("date", null);

  const { bundleToName, skuToBundle } = loadApps();
  const token = makeJwt();

  let credentials;
  try {
    credentials = process.env.GCP_SA_KEY ? JSON.parse(process.env.GCP_SA_KEY) : undefined;
  } catch {
    throw new Error("GCP_SA_KEY is not valid JSON (GitHub Secretsの貼り付け内容を確認してください)");
  }
  const bigquery = new BigQuery({ projectId, credentials });

  const tableFqdn = `\`${projectId}.${dataset}.${tableDaily}\``;

  const targets = [];
  if (explicitDate) targets.push(explicitDate);
  else if (mode === "backfill") for (let i = 1; i <= Math.max(1, days); i++) targets.push(ymdInLosAngeles(i));
  else targets.push(ymdInLosAngeles(1));

  targets.sort();

  let processed = 0;

  for (const reportDate of targets) {
    console.log("Fetching iOS sales report:", reportDate);

    const tsvBuf = await fetchSalesReportTsv(token, reportDate);
    const rows = parseTsv(tsvBuf);

    const agg = new Map();

    for (const r of rows) {
      const sku = String(pickFirst(r, ["SKU"])).trim();
      const country = String(pickFirst(r, ["Country Code", "Country", "国家コード"])).trim().toUpperCase();
      const units = toInt(pickFirst(r, ["App Units", "Units", "ユニット"]));

      if (!sku) continue;

      const bundleId = skuToBundle.get(sku);
      if (!bundleId) continue;

      if (!bundleToName.has(bundleId)) continue;

      if (!agg.has(bundleId)) agg.set(bundleId, { JP: 0, OVERSEAS: 0 });
      if (country === "JP") agg.get(bundleId).JP += units;
      else agg.get(bundleId).OVERSEAS += units;
    }

    const outRows = [];
    for (const [bundleId, v] of agg.entries()) {
      const appName = bundleToName.get(bundleId);
      outRows.push(
        { store: "ios", app_id: bundleId, app_name: appName, country_group: "JP", downloads: v.JP },
        { store: "ios", app_id: bundleId, app_name: appName, country_group: "OVERSEAS", downloads: v.OVERSEAS }
      );
    }

    if (outRows.length === 0) {
      console.log("No matching iOS rows. Skipped:", reportDate);
      continue;
    }

    await mergeToBigQuery(bigquery, tableFqdn, reportDate, outRows, "appstore_salesreports");
    processed += 1;

    console.log("MERGE done:", { date: reportDate, rows: outRows.length });
  }

  console.log("All done:", { processed, mode, targets: targets.length });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
