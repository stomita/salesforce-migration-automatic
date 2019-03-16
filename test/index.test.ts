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
    const { successes, failures } = await am.upload();
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
    for (const filename of filenames) {
      const sobject = filename.split(".")[0];
      const data = await fs.readFile(path.join(dataDir, filename), "utf8");
      await am.loadCSVData(sobject, data);
    }
    am.on("uploadProgress", ({ totalCount, successCount, failureCount }) => {
      console.log(
        "total: ",
        totalCount,
        "successes: ",
        successCount,
        "failures: ",
        failureCount
      );
    });
    const { successes, failures } = await am.upload();
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
    am.on("uploadProgress", ({ fetchedCount }) => {
      console.log("fetched: ", fetchedCount);
    });
    const csvs = await am.dumpAsCSVData(queries);
    expect(csvs).toBeDefined();
    expect(csvs.length).toBe(queries.length);
    for (const csv of csvs) {
      expect(typeof csv).toBe("string");
    }
  });
});

afterAll(async () => {
  for (const sobject of [
    "Task",
    "Lead",
    "Case",
    "Opportunity",
    "Contact",
    "Account"
  ]) {
    await conn
      .sobject(sobject)
      .find({}, "Id")
      .destroy();
  }
});
