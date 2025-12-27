import { Storage } from "@google-cloud/storage";
import { BigQuery } from "@google-cloud/bigquery";
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
import { parse } from "csv-parse";

const projectId = process.env.BQ_PROJECT;
const dataset = process.env.BQ_DATASET;
const tableDaily = process.env.BQ_TABLE_DAILY;

// BigQueryのロケーション（USのdatasetを使うなら "US" にするのが安全）
const location = process.env.BQ_LOCATION || "US";

// GCS
const gcsBucket = process.env.GCS_BUCKET;
const gcsPrefix = process.env.GCS_PREFIX;

// Auth（あればSAキー、なければADC）
let credentials;
if (process.env.GCP_SA_KEY) {
  try {
    credentials = JSON.parse(process.env.GCP_SA_KEY);
  } catch {
    throw new Error("GCP_SA_KEY is not valid JSON (Secretsの貼り付け内容を確認してください)");
  }
}

if (!projectId || !dataset || !tableDaily) {
  throw new Error("Missing env: BQ_PROJECT / BQ_DATASET / BQ_TABLE_DAILY");
}
if (!gcsBucket || !gcsPrefix) {
  throw new Error("Missing env: GCS_BUCKET / GCS_PREFIX");
}

function getArg(name, def = null) {
  const idx = process.argv.indexOf(`--${name}`);
  if (idx === -1) return def;
  return process.argv[idx + 1] ?? def;
}

function todayJstYYYYMMDD() {
  const now = new Date();
  const jst = new Date(now.getTime() + 9 * 60 * 60 * 1000);
  return jst.toISOString().slice(0, 10);
}

function extractDateFromFilename(filename) {
  const m1 = filename.match(/(20\d{2})[-_]?(\d{2})[-_]?(\d{2})/);
  if (m1) return `${m1[1]}-${m1[2]}-${m1[3]}`;
  return null; // ★今日にしない
}

function loadApps() {
  const appsPath = path.resolve("config/apps.json");
  const apps = JSON.parse(fs.readFileSync(appsPath, "utf-8"));
  if (!Array.isArray(apps)) throw new Error("apps.json must be an array");

  // android_app_id をキーに引けるようにする
  const map = new Map();
  for (const a of apps) {
    if (!a.app_name) throw new Error(`Invalid app entry (missing app_name): ${JSON.stringify(a)}`);
    if (a.android_app_id) map.set(String(a.android_app_id).trim(), a.app_name);
  }
  return map; // packageName -> appName
}

function pickColumnIndex(headers, candidates) {
  const lower = headers.map((h) => String(h).trim().toLowerCase());
  for (const c of candidates) {
    const i = lower.indexOf(c.toLowerCase());
    if (i !== -1) return i;
  }
  return -1;
}

async function listObjects(bucket, prefix) {
  const [files] = await bucket.getFiles({ prefix });
  return files
    .map((f) => f.name)
    .filter((name) => name.endsWith(".csv") || name.endsWith(".csv.gz") || name.endsWith(".gz"));
}

async function parseInstallsFromGcsFile(bucket, objectName, appsMap) {
  const file = bucket.file(objectName);

  // ファイル名から日付を推定
  const date = extractDateFromFilename(objectName) ?? todayJstYYYYMMDD();

  // 集計：packageName -> {JP, OVERSEAS} or {ALL}
  const agg = new Map();

  const stream = file.createReadStream();

  // .csv.gz / .gz 対応
  const input = objectName.endsWith(".gz") ? stream.pipe(zlib.createGunzip()) : stream;

  let headers = null;

  const parser = parse({
    columns: false,
    relax_quotes: true,
    relax_column_count: true,
    bom: true,
    skip_empty_lines: true,
    trim: true,
  });

  const done = new Promise((resolve, reject) => {
    parser.on("readable", () => {
      let record;
      while ((record = parser.read()) !== null) {
        if (!headers) {
          headers = record.map((x) => String(x));
          continue;
        }
        if (!headers || headers.length === 0) continue;

        const pkgIdx = pickColumnIndex(headers, ["package_name", "package name", "package"]);
        const countryIdx = pickColumnIndex(headers, ["country", "country code"]); // 無いことがある(overview)
        const installsIdx = pickColumnIndex(headers, [
          "daily_device_installs",
          "daily device installs",
          "installs",
          "daily_installs",
          "daily installs",
          "device_installs",
          "device installs",
        ]);

        if (pkgIdx === -1 || installsIdx === -1) {
          throw new Error(
            `CSV columns not found in ${objectName}. Found headers: ${headers.slice(0, 20).join(", ")}`
          );
        }

        const packageName = String(record[pkgIdx] ?? "").trim();
        if (!packageName) continue;

        // apps.json にあるアプリだけ対象
        if (!appsMap.has(packageName)) continue;

        const installsRaw = String(record[installsIdx] ?? "0").trim();
        const installs = Number(installsRaw.replace(/,/g, "")) || 0;

        // country列が無い = overview 形式 → ALL に入れる
        if (countryIdx === -1) {
          if (!agg.has(packageName)) agg.set(packageName, { ALL: 0 });
          agg.get(packageName).ALL += installs;
        } else {
          const country = String(record[countryIdx] ?? "").trim().toUpperCase();
          if (!agg.has(packageName)) agg.set(packageName, { JP: 0, OVERSEAS: 0 });
          if (country === "JP") agg.get(packageName).JP += installs;
          else agg.get(packageName).OVERSEAS += installs;
        }
      }
    });

    parser.on("error", reject);
    parser.on("end", resolve);
  });

  input.pipe(parser);
  await done;

  return { date, agg };
}

async function mergeToBigQuery(bigquery, tableFqdn, date, rows, source) {
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
    params: { date, source, rows },
  });
  await job.getQueryResults();
}

async function main() {
  const mode = getArg("mode", "latest"); // latest | backfill
  const days = Number(getArg("days", "0")) || 0; // 0なら無制限
  const appsMap = loadApps();

  const storage = credentials ? new Storage({ projectId, credentials }) : new Storage();
  const bucket = storage.bucket(gcsBucket);

  const allObjects = await listObjects(bucket, gcsPrefix);
  if (allObjects.length === 0) {
    console.log("No objects found under prefix:", `gs://${gcsBucket}/${gcsPrefix}`);
    return;
  }

  const before = allObjects.length;
  const overviewOnly = allObjects.filter((name) =>
    name.endsWith("_overview.csv") || name.endsWith("_overview.csv.gz")
  );

  console.log(`Filtered overview.csv: ${before} -> ${overviewOnly.length}`);

  if (overviewOnly.length === 0) {
    console.log("No overview.csv found under prefix:", `gs://${gcsBucket}/${gcsPrefix}`);
    return;
  }

  // overviewOnly を日付で絞る（backfill時のみ）
  let filtered = overviewOnly;

  if (mode === "backfill" && days > 0) {
    const now = new Date();
    const todayJst = new Date(now.getTime() + 9 * 60 * 60 * 1000);
    const todayStr = todayJst.toISOString().slice(0, 10); // YYYY-MM-DD

    const today = new Date(todayStr + "T00:00:00Z");
    const from = new Date(today);
    from.setUTCDate(from.getUTCDate() - (days - 1));

    filtered = overviewOnly.filter((name) => {
      const d = extractDateFromFilename(name);
      if (!d) return false; // ★日付が無いものは除外
      const dt = new Date(d + "T00:00:00Z");
      return dt >= from && dt <= today;
    });

    console.log(`Filtered by days=${days}: ${overviewOnly.length} -> ${filtered.length}`);

    if (filtered.length === 0) {
      console.log("No overview files in the specified date range. Nothing to do.");
      return;
    }
  }

  // ★ targets は filtered を元に作る（ここが重要）
  filtered.sort();
  const targets =
    mode === "backfill" ? filtered : [filtered[filtered.length - 1]];

  console.log(`Mode=${mode} targets=${targets.length}`);

  const bigquery = credentials ? new BigQuery({ projectId, credentials }) : new BigQuery({ projectId });
  const tableFqdn = `\`${projectId}.${dataset}.${tableDaily}\``;

  let processed = 0;

  for (const obj of targets) {
    console.log("Processing:", obj);

    const { date, agg } = await parseInstallsFromGcsFile(bucket, obj, appsMap);

    const rows = [];
    for (const [packageName, v] of agg.entries()) {
      const appName = appsMap.get(packageName);

      if ("ALL" in v) {
        rows.push({
          store: "android",
          app_id: packageName,
          app_name: appName,
          country_group: "ALL",
          downloads: v.ALL,
        });
      } else {
        rows.push(
          {
            store: "android",
            app_id: packageName,
            app_name: appName,
            country_group: "JP",
            downloads: v.JP,
          },
          {
            store: "android",
            app_id: packageName,
            app_name: appName,
            country_group: "OVERSEAS",
            downloads: v.OVERSEAS,
          }
        );
      }
    }

    if (rows.length === 0) {
      console.log("No matching apps rows for this file. Skipped.");
      continue;
    }

    await mergeToBigQuery(bigquery, tableFqdn, date, rows, "google_play");
    processed += 1;

    console.log("MERGE done:", { date, rows: rows.length, file: obj });
  }

  console.log("All done:", { processed, mode });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
