import { SerializedUploader } from "../src";
import { Connection } from "jsforce";

/**
 *
 */
describe("SerializedUploader", () => {
  it("should upload data", async () => {
    const conn = new Connection({});
    const su = new SerializedUploader(conn);
    const { successes, failures } = await su.upload();
    console.log(successes, failures);
  });
});
