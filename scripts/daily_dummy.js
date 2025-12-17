import { BigQuery } from "@google-cloud/bigquery";

const projectId = process.env.BQ_PROJECT;
const dataset = process.env.BQ_DATASET;
const tableDaily = process.env.BQ_TABLE_DAILY;

if (!projectId || !dataset || !tableDaily) {
  throw new Error("Missing env: BQ_PROJECT / BQ_DATASET / BQ_TABLE_DAILY");
}

const bigquery = new BigQuery({ projectId });

function todayJstYYYYMMDD() {
  const now = new Date();
  const jst = new Date(now.getTime() + 9 * 60 * 60 * 1000);
  return jst.toISOString().slice(0, 10); // YYYY-MM-DD
}

function randDownloads() {
  return Math.floor(Math.random() * 50) + 1;
}

async function main() {
  const apps = [
    {
      app_name: "ふつうの神経衰弱",
      ios_app_id: "849663603",
      android_app_id: "jp.baibai.fshinkei",
    },
    {
      app_name: "ふつうのビンゴ",
      ios_app_id: "1082797006",
      android_app_id: "jp.baibai.fbingo",
    }
  ];

  const date = todayJstYYYYMMDD();
  const source = "dummy";

  // apps を展開して「書き込み行」を作る（ios/android × JP/OVERSEAS）
  const rows = [];
  for (const app of apps) {
    if (app.ios_app_id) {
      rows.push(
        {
          store: "ios",
          app_id: app.ios_app_id,
          app_name: app.app_name,
          country_group: "JP",
          downloads: randDownloads(),
        },
        {
          store: "ios",
          app_id: app.ios_app_id,
          app_name: app.app_name,
          country_group: "OVERSEAS",
          downloads: 0,
        }
      );
    }
    if (app.android_app_id) {
      rows.push(
        {
          store: "android",
          app_id: app.android_app_id,
          app_name: app.app_name,
          country_group: "JP",
          downloads: randDownloads(),
        },
        {
          store: "android",
          app_id: app.android_app_id,
          app_name: app.app_name,
          country_group: "OVERSEAS",
          downloads: 0,
        }
      );
    }
  }

  const tableFqdn = `\`${projectId}.${dataset}.${tableDaily}\``;

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

  const options = {
    query,
    location: "asia-northeast1",
    params: {
      date,
      source,
      rows,
    },
  };

  const [job] = await bigquery.createQueryJob(options);
  console.log(`Started job ${job.id}`);
  await job.getQueryResults();

  console.log("MERGE done:", { date, apps: apps.length, rows: rows.length });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
