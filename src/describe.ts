import { Connection, DescribeSObjectResult, Field } from 'jsforce';
import { removeNamespace, addNamespace } from './util';

/**
 *
 */
type DescribeOptions = {
  defaultNamespace?: string;
};

/**
 *
 */
function getTargetByIdentifierInNamespace<T>(
  map: Map<string, T>,
  identifier: string,
  namespace?: string,
) {
  let target = map.get(identifier);
  if (!target && namespace) {
    const identifierNoNamespace = removeNamespace(identifier, namespace);
    if (identifierNoNamespace !== identifier) {
      target = map.get(identifierNoNamespace);
    }
    if (!target) {
      const identifierWithNamespace = addNamespace(identifier, namespace);
      if (identifierWithNamespace !== identifier) {
        target = map.get(identifierWithNamespace);
      }
    }
  }
  return target;
}

/**
 *
 */
function findSObjectDescription(
  objectName: string,
  descriptions: Map<string, DescribeSObjectResult>,
  options: DescribeOptions,
) {
  return getTargetByIdentifierInNamespace(
    descriptions,
    objectName.toLowerCase(),
    options.defaultNamespace,
  );
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
      description.fields.map((field) => [field.name.toLowerCase(), field]),
    );
    return getTargetByIdentifierInNamespace(
      fields,
      fieldName.toLowerCase(),
      options.defaultNamespace,
    );
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
    ).map((described) => [described.name.toLowerCase(), described]),
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
