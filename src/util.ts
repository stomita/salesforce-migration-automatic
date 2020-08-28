import { Record as SFRecord } from 'jsforce';

/**
 *
 */
export function removeNamespace(identifier: string, namespace: string) {
  return identifier.indexOf(`${namespace}__`) === 0
    ? identifier.substring(namespace.length + 2)
    : identifier;
}

/**
 *
 */
export function addNamespace(identifier: string, namespace: string) {
  const parts = identifier.split('__');
  return parts.length === 2 &&
    (parts[1] === 'c' || parts[1] === 'r' || parts[1] === 'mdt') // should be custom field/object/relation
    ? `${namespace}__${identifier}`
    : identifier;
}

/**
 *
 */
export function getRecordFieldValue(
  record: SFRecord,
  field: string,
  namespace: string | undefined,
) {
  let value = record[field];
  if (typeof value === 'undefined' && namespace) {
    const fieldNoNamespace = removeNamespace(field, namespace);
    value = record[fieldNoNamespace];
    if (typeof value === 'undefined') {
      const fieldWithNamespace = addNamespace(field, namespace);
      value = record[fieldWithNamespace];
    }
  }
  return value;
}

/**
 *
 */
export function getMapValue<T>(
  map: Map<string, T>,
  identifier: string,
  namespace: string | undefined,
) {
  let target = map.get(identifier);
  if (target == null && namespace) {
    const identifierNoNamespace = removeNamespace(identifier, namespace);
    target = map.get(identifierNoNamespace);
    if (target == null) {
      const identifierWithNamespace = addNamespace(identifier, namespace);
      target = map.get(identifierWithNamespace);
    }
  }
  return target;
}

/**
 *
 */
export function includesInNamespace(
  arr: string[],
  identifier: string,
  namespace: string | undefined,
) {
  let ret = arr.includes(identifier);
  if (!ret && namespace) {
    const identifierNoNamespace = removeNamespace(identifier, namespace);
    ret = arr.includes(identifierNoNamespace);
    if (!ret) {
      const identifierWithNamespace = addNamespace(identifier, namespace);
      ret = arr.includes(identifierWithNamespace);
    }
  }
  return ret;
}
