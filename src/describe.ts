import { Connection, DescribeSObjectResult, Field } from 'jsforce';
import { getMapValue, removeNamespace } from './util';

/**
 *
 */
type DescribeOptions = {
  defaultNamespace?: string;
};

/**
 *
 */
function findSObjectDescription(
  objectName: string,
  descriptions: Map<string, DescribeSObjectResult>,
  options: DescribeOptions,
) {
  return getMapValue(descriptions, objectName, options.defaultNamespace);
}

/**
 *
 */
function findFieldDescription(
  objectName: string,
  fieldName: string,
  descriptions: Map<string, DescribeSObjectResult>,
  options: DescribeOptions,
) {
  const description = findSObjectDescription(objectName, descriptions, options);
  if (description) {
    const fields = new Map(
      description.fields.map((field) => [field.name, field]),
    );
    return getMapValue(fields, fieldName, options.defaultNamespace);
  }
}

/**
 *
 */
export interface Describer {
  findSObjectDescription(object: string): DescribeSObjectResult | undefined;
  findFieldDescription(object: string, fieldName: string): Field | undefined;
}

/**
 *
 * @param conn
 * @param objects
 */
export async function describeSObjects(
  conn: Connection,
  objects: string[],
  options: DescribeOptions,
): Promise<Describer> {
  const descriptions = new Map<string, DescribeSObjectResult>(
    (
      await Promise.all(
        objects.map(async (object) =>
          conn
            .describe(object)
            .catch((err) => {
              if (options.defaultNamespace) {
                const object2 = removeNamespace(
                  object,
                  options.defaultNamespace,
                );
                if (object !== object2) {
                  return conn.describe(object2);
                }
              }
              throw err;
            })
            .catch((err) => {
              if (err.name === 'NOT_FOUND') {
                throw new Error(`No object schema found: ${object}`);
              }
              throw err;
            }),
        ),
      )
    ).map((described) => [described.name, described]),
  );
  return {
    findSObjectDescription(objectName: string) {
      return findSObjectDescription(objectName, descriptions, options);
    },
    findFieldDescription(objectName: string, fieldName: string) {
      return findFieldDescription(objectName, fieldName, descriptions, options);
    },
  };
}
