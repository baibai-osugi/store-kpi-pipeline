import { BigQuery } from "@google-cloud/bigquery";

const projectId = process.env.BQ_PROJECT;
const dataset = process.env.BQ_DATASET;
const tableDaily = process.env.BQ_TABLE_DAILY;

if (!projectId || !dataset || !tableDaily) {
  throw new Error("Missing env: BQ_PROJECT / BQ_DATASET / BQ_TABLE_DAILY");
}

const bigquery = new BigQuery({ projectId });

// JST日付を作る（毎日1行入れたい想定）
function todayJstYYYYMMDD() {
  const now = new Date();
  // JSTに寄せる簡易版（UTC+9）
  const jst = new Date(now.getTime() + 9 * 60 * 60 * 1000);
  return jst.toISOString().slice(0, 10); // "YYYY-MM-DD"
}

const run = async () => {
  const date = todayJstYYYYMMDD();

  // ダミー値（必要なら調整）
  const appName = "dummy_app";
  const store = "iOS"; // or "Android"
  const downloads = Math.floor(Math.random() * 50) + 1;

  const tableFqdn = `\`${projectId}.${dataset}.${tableDaily}\``;
