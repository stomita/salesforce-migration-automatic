import path from "path";
import fs from "fs-extra";
import { SerializedUploader } from "../src";
import jsforce, { Connection } from "jsforce";

const username = process.env.SF_USERNAME;

/**
 *
 */
describe("SerializedUploader", () => {
  it("should upload empty data", async () => {
    const conn = new Connection({});
    const su = new SerializedUploader(conn);
    const { successes, failures } = await su.upload();
    expect(successes).toBeDefined();
    expect(successes.length).toBe(0);
    expect(failures).toBeDefined();
    expect(failures.length).toBe(0);
  });

  it("should upload data from csv", async () => {
    const conn: Connection = (jsforce as any).registry.getConnection(username);
    const su = new SerializedUploader(conn);
    const dataDir = path.join(__dirname, "fixtures", "csv");
    const filenames = await fs.readdir(dataDir);
    for (const filename of filenames) {
      console.log(filename);
      const sobject = filename.split(".")[0];
      const data = await fs.readFile(path.join(dataDir, filename), "utf8");
      console.log(sobject, data);
    }
    const { successes, failures } = await su.upload();
    expect(successes).toBeDefined();
    expect(successes.length).toBe(0);
    expect(failures).toBeDefined();
    expect(failures.length).toBe(0);
  });
});
