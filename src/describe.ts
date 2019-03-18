import { Connection } from "jsforce";
import { DescribeSObjectResultMap } from "./types";

/**
 *
 * @param conn
 * @param objects
 */
export async function describeSObjects(conn: Connection, objects: string[]) {
  const descriptions = await Promise.all(
    objects.map(object => conn.describe(object))
  );
  return descriptions.reduce(
    (describedMap, described) => ({
      ...describedMap,
      [described.name]: described
    }),
    {} as DescribeSObjectResultMap
  );
}
