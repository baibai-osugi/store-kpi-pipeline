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

  const appName = "dummy_app";
  const store = "iOS";
  const downloads = Math.floor(Math.random() * 50) + 1;

  const tableFqdn = `\`${projectId}.${dataset}.${tableDaily}\``;

  const query = `
    INSERT INTO ${tableFqdn} (date, app_name, store, downloads)
    VALUES (@date, @app_name, @store, @downloads)
  `;

  const options = {
    query,
    // dataset のロケーションが US 以外ならここを合わせてください
    location: "US",
    params: {
      date,
      app_name: appName,
      store,
      downloads,
    },
  };

  const [job] = await bigquery.createQueryJob(options);
  console.log(`Started job ${job.id}`);

  await job.getQueryResults();
  console.log("Insert done:", { date, appName, store, downloads });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
