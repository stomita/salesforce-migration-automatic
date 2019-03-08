import path from "path";
import fs from "fs-extra";
import { SerializedUploader } from "../src";
import { Connection } from "jsforce";
import { getConnection } from "./util/getConnection";

let conn: Connection;

/**
 *
 */
describe("SerializedUploader", () => {
  jest.setTimeout(100000);

  beforeAll(async () => {
    conn = await getConnection();
  });

  it("should upload empty data", async () => {
    const su = new SerializedUploader(conn);
    const { successes, failures } = await su.upload();
    expect(successes).toBeDefined();
    expect(successes.length).toBe(0);
    expect(failures).toBeDefined();
    expect(failures.length).toBe(0);
  });

  it("should upload data from csv", async () => {
    const accCnt = await conn.sobject("Account").count();
    const oppCnt = await conn.sobject("Opportunity").count();
    const su = new SerializedUploader(conn);
    const dataDir = path.join(__dirname, "fixtures", "csv");
    const filenames = await fs.readdir(dataDir);
    for (const filename of filenames) {
      const sobject = filename.split(".")[0];
      const data = await fs.readFile(path.join(dataDir, filename), "utf8");
      await su.loadCSVData(sobject, data);
    }
    su.on("uploadProgress", ({ totalCount, successCount, failureCount }) => {
      console.log(
        "total: ",
        totalCount,
        "successes: ",
        successCount,
        "failures: ",
        failureCount
      );
    });
    const { successes, failures } = await su.upload();
    expect(successes).toBeDefined();
    expect(successes.length).toBeGreaterThan(0);
    expect(failures).toBeDefined();
    expect(failures.length).toBe(0);
    const newAccCnt = await conn.sobject("Account").count();
    expect(newAccCnt).toBeGreaterThan(accCnt);
    const newOppCnt = await conn.sobject("Opportunity").count();
    expect(newOppCnt).toBeGreaterThan(oppCnt);
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
});
