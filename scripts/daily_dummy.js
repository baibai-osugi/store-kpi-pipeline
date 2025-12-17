import { BigQuery } from "@google-cloud/bigquery";

const PROJECT_ID = process.env.BQ_PROJECT ?? "store-kpi";
const DATASET = process.env.BQ_DATASET ?? "app_kpi";
const T_APPS = process.env.BQ_TABLE_APPS ?? "apps";
const T_DAILY = process.env.BQ_TABLE_DAILY ?? "downloads_daily";

const bq = new BigQuery({ projectId: PROJECT_ID });

function yyyyMmDdInJST(date = new Date()) {
  // JST(UTC+9)で日付を作る（前日分を取りたいのでJST基準が無難）
  const jst = new Date(date.getTime() + 9 * 60 * 60 * 1000);
  return jst.toISOString().slice(0, 10);
}

function yesterdayJST() {
  const now = new Date();
  now.setDate(now.getDate() - 1);
  return yyyyMmDdInJST(now);
}

function dummyValue(appId) {
  // ダミー値：毎日変わる＆アプリごとに少し違う値（安定）
  // 例: 1〜30程度
  let h = 0;
  for (let i = 0; i < appId.length; i++) h = (h * 31 + appId.charCodeAt(i)) >>> 0;
  const day = Math.floor(Date.now() / (24 * 60 * 60 * 1000));
  return (h + day) % 30 + 1;
}

async function fetchActiveApps() {
  const sql = `
    SELECT store, app_id, app_name, track_jp, track_overseas
    FROM \`${PROJECT_ID}.${DATASET}.${T_APPS}\`
    WHERE active = TRUE
  `;
  const [rows] = await bq.query({ query: sql });
  return rows;
}

async function deleteExistingKeys(dateStr, keys) {
  // keys: [{store, app_id, country_group}]
  const sql = `
    DELETE FROM \`${PROJECT_ID}.${DATASET}.${T_DAILY}\`
    WHERE date = DATE(@date)
      AND EXISTS (
        SELECT 1
        FROM UNNEST(@keys) k
        WHERE k.store = store
          AND k.app_id = app_id
          AND k.country_group = country_group
      )
  `;
  await bq.query({
    query: sql,
    params: { date: dateStr, keys }
  });
}

async function insertRows(rows) {
  const table = bq.dataset(DATASET).table(T_DAILY);
  // streaming insert
  await table.insert(rows);
}

async function main() {
  const dateStr = process.env.TARGET_DATE ?? yesterdayJST();
  const apps = await fetchActiveApps();

  const ingested_at = new Date().toISOString();
  const rows = [];
  const keys = [];

  for (const app of apps) {
    const { store, app_id, app_name, track_jp, track_overseas } = app;

    const base = dummyValue(app_id);
    const jp = track_jp ? base : 0;
    const overseas = track_overseas ? Math.max(0, Math.floor(base / 3)) : 0; // 日本のみなら0

    // JP行（必ず作る）
    rows.push({
      date: dateStr,
      store,
      app_id,
      app_name,
      country_group: "JP",
      downloads: jp,
      source: "dummy",
      ingested_at
    });
    keys.push({ store, app_id, country_group: "JP" });

    // OVERSEAS行（必ず作る：対象外は0）
    rows.push({
      date: dateStr,
      store,
      app_id,
      app_name,
      country_group: "OVERSEAS",
      downloads: overseas,
      source: "dummy",
      ingested_at
    });
    keys.push({ store, app_id, country_group: "OVERSEAS" });
  }

  // 再実行しても同日の重複が出ないように「同キーを削除→挿入」
  await deleteExistingKeys(dateStr, keys);
  await insertRows(rows);

  console.log(`OK: inserted ${rows.length} rows for ${dateStr}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
