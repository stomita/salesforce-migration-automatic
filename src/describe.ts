import { Connection } from "jsforce";
import { DescribeSObjectResultMap } from "./types";
import { removeNamespace } from "./util";

/**
 *
 * @param conn
 * @param objects
 */
export async function describeSObjects(conn: Connection, objects: string[]) {
  const descriptions = await Promise.all(
    objects.map(async object =>
      conn.describe(object).catch(err => {
        const object2 = removeNamespace(object);
        if (object !== object2) {
          return conn.describe(removeNamespace(object));
        }
        throw err;
      })
    )
  );
  return descriptions.reduce(
    (describedMap, described) => ({
      ...describedMap,
      [described.name]: described
    }),
    {} as DescribeSObjectResultMap
  );
}
