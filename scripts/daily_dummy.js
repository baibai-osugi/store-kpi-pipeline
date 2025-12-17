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
  return jst.toISOString().slice(0, 10);
}

async function main() {
  const date = todayJstYYYYMMDD();

  // ★必須：app_id（NOT NULL対策）
  const appId = "dummy_app_id";
  const appName = "dummy_app";

  const dlIos = Math.floor(Math.random() * 50) + 1;
  const dlAndroid = Math.floor(Math.random() * 50) + 1;

  const tableFqdn = `\`${projectId}.${dataset}.${tableDaily}\``;

  const query = `
    INSERT INTO ${tableFqdn} (date, app_id, app_name, store, downloads)
    VALUES
      (@date, @app_id, @app_name, "iOS", @dl_ios),
      (@date, @app_id, @app_name, "Android", @dl_android)
  `;

  const options = {
    query,
    location: "asia-northeast1",
    params: {
      date,
      app_id: appId,
      app_name: appName,
      dl_ios: dlIos,
      dl_android: dlAndroid,
    },
  };

  const [job] = await bigquery.createQueryJob(options);
  console.log(`Started job ${job.id}`);

  await job.getQueryResults();
  console.log("Insert done:", { date, appId, appName, dlIos, dlAndroid });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
