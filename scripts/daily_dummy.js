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

async function main() {
  const date = todayJstYYYYMMDD();

  const iosAppId = "849663603";
  const androidAppId = "jp.baibai.fshinkei";
  const appName = "ふつうの神経衰弱";
  const source = "dummy";

  const dlIosJp = Math.floor(Math.random() * 50) + 1;
  const dlIosOverseas = 0;
  const dlAndroidJp = Math.floor(Math.random() * 50) + 1;
  const dlAndroidOverseas = 0;

  const tableFqdn = `\`${projectId}.${dataset}.${tableDaily}\``;

  // ★MERGE：date+store+app_id+country_group をキーに上書き
  const query = `
    MERGE ${tableFqdn} T
    USING (
      SELECT
        DATE(@date) AS date,
        store,
        app_id,
        @app_name AS app_name,
        country_group,
        downloads,
        @source AS source,
        CURRENT_TIMESTAMP() AS ingested_at
      FROM UNNEST([
        STRUCT("ios" AS store, @ios_app_id AS app_id, "JP" AS country_group, @dl_ios_jp AS downloads),
        STRUCT("ios" AS store, @ios_app_id AS app_id, "OVERSEAS" AS country_group, @dl_ios_overseas AS downloads),
        STRUCT("android" AS store, @android_app_id AS app_id, "JP" AS country_group, @dl_android_jp AS downloads),
        STRUCT("android" AS store, @android_app_id AS app_id, "OVERSEAS" AS country_group, @dl_android_overseas AS downloads)
      ])
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
      ios_app_id: iosAppId,
      android_app_id: androidAppId,
      app_name: appName,
      source,
      dl_ios_jp: dlIosJp,
      dl_ios_overseas: dlIosOverseas,
      dl_android_jp: dlAndroidJp,
      dl_android_overseas: dlAndroidOverseas,
    },
  };

  const [job] = await bigquery.createQueryJob(options);
  console.log(`Started job ${job.id}`);
  await job.getQueryResults();

  console.log("MERGE done:", { date, dlIosJp, dlAndroidJp });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
