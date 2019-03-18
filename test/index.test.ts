import path from "path";
import fs from "fs-extra";
import { AutoMigrator, DumpQuery } from "../src";
import { Connection } from "jsforce";
import { getConnection } from "./util/getConnection";

let conn: Connection;

beforeAll(async () => {
  conn = await getConnection();
  conn.bulk.pollInterval = 50000;
  conn.bulk.pollTimeout = 300000;
});

/**
 *
 */
describe("SerializedUploader", () => {
  jest.setTimeout(300000);

  it("should upload empty data", async () => {
    const am = new AutoMigrator(conn);
    const { successes, failures } = await am.loadCSVData([]);
    expect(successes).toBeDefined();
    expect(successes.length).toBe(0);
    expect(failures).toBeDefined();
    expect(failures.length).toBe(0);
  });

  it("should upload data from csv", async () => {
    const accCnt = await conn.sobject("Account").count();
    const oppCnt = await conn.sobject("Opportunity").count();
    const am = new AutoMigrator(conn);
    const dataDir = path.join(__dirname, "fixtures", "csv");
    const filenames = await fs.readdir(dataDir);
    const inputs = await Promise.all(
      filenames.map(async filename => {
        const object = filename.split(".")[0];
        const csvData = await fs.readFile(path.join(dataDir, filename), "utf8");
        return { object, csvData };
      })
    );
    am.on("loadProgress", ({ totalCount, successCount, failureCount }) => {
      console.log(
        "total: ",
        totalCount,
        "successes: ",
        successCount,
        "failures: ",
        failureCount
      );
    });
    const { successes, failures } = await am.loadCSVData(inputs);
    expect(successes).toBeDefined();
    expect(successes.length).toBeGreaterThan(0);
    expect(failures).toBeDefined();
    failures.forEach(failure => console.log(failure));
    expect(failures.length).toBe(0);
    const newAccCnt = await conn.sobject("Account").count();
    expect(newAccCnt).toBeGreaterThan(accCnt);
    const newOppCnt = await conn.sobject("Opportunity").count();
    expect(newOppCnt).toBeGreaterThan(oppCnt);
  });

  it("should download data as csv", async () => {
    const am = new AutoMigrator(conn);
    const queries: DumpQuery[] = [
      { object: "Account", target: "query" },
      { object: "Contact", target: "related" },
      { object: "Task", target: "related" },
      { object: "User", target: "related" }
    ];
    am.on("dumpProgress", ({ fetchedCount, fetchedCountPerObject }) => {
      console.log("fetched: ", fetchedCount, fetchedCountPerObject);
    });
    const csvs = await am.dumpAsCSVData(queries);
    expect(csvs).toBeDefined();
    expect(csvs.length).toBe(queries.length);
    for (const csv of csvs) {
      expect(typeof csv).toBe("string");
      expect(csv.trim().split(/\n/).length).toBeGreaterThan(1);
    }
  });
});

afterAll(async () => {
  await Promise.all(
    ["Task", "Lead", "Case", "Opportunity", "Contact", "Account"].map(
      async sobject =>
        await conn
          .sobject(sobject)
          .find({}, "Id")
          .destroy()
    )
  );
});
