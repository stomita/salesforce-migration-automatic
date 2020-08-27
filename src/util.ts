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
  return identifier.split('__').length > 2
    ? identifier
    : `${namespace}__${identifier}`;
}
