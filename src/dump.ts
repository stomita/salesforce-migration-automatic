import { Connection, Record as SFRecord } from 'jsforce';
import {
  DumpQuery,
  QueryTarget,
  RelatedTarget,
  DumpOptions,
  DumpProgress,
} from './types';
import { describeSObjects, Describer } from './describe';
import {
  getMapValue,
  includesInNamespace,
  getRecordFieldValue,
  removeNamespace,
  toStringList,
} from './util';
import { stringifyCSV } from './csv';

/**
 *
 */
type FetchedRecordsMap = Map<string, SFRecord[]>;
type FetchedIdsMap = Map<string, Set<string>>;

function getTargetFieldDefinitions(query: DumpQuery, describer: Describer) {
  let queryFields: Set<string> | null = null;
  if (query.fields) {
    queryFields = new Set(toStringList(query.fields));
  }
  let ignoreFields: Set<string> | null = null;
  if (query.ignoreFields) {
    ignoreFields = new Set(toStringList(query.ignoreFields));
  }
  const description = describer.findSObjectDescription(query.object);
  if (!description) {
    throw new Error(`No object description information found: ${query.object}`);
  }
  return queryFields
    ? description.fields.filter((f) => queryFields?.has(f.name))
    : ignoreFields
    ? description.fields.filter((f) => !ignoreFields?.has(f.name))
    : description.fields;
}

function getTargetFields(query: DumpQuery, describer: Describer) {
  const fieldDefs = getTargetFieldDefinitions(query, describer);
  return fieldDefs.map((f) => f.name);
}

async function executeQuery(
  conn: Connection,
  soql: string,
  options: DumpOptions,
) {
  const records = await new Promise<SFRecord[]>((resolve, reject) => {
    const records: SFRecord[] = [];
    conn
      .query(soql)
      .maxFetch(options.maxFetchSize ?? 10000)
      .on('data', (record) => records.push(record))
      .on('end', () => resolve(records))
      .on('error', (err) => reject(err));
  });
  return records;
}

async function queryRecords(
  conn: Connection,
  query: { object: string } & QueryTarget,
  describer: Describer,
  options: DumpOptions,
) {
  const fields = getTargetFields(query, describer);
  let soql = `SELECT ${fields.join(', ')} FROM ${query.object}`;
  soql += query.scope ? ` USING SCOPE ${query.scope}` : '';
  soql += query.condition ? ` WHERE ${query.condition}` : '';
  soql += query.orderby ? ` ORDER BY ${query.orderby}` : '';
  soql += query.limit ? ` LIMIT ${query.limit}` : '';
  soql += query.offset ? ` OFFSET ${query.offset}` : '';
  return executeQuery(conn, soql, options);
}

async function queryPrimaryRecords(
  conn: Connection,
  queries: DumpQuery[],
  describer: Describer,
  options: DumpOptions,
) {
  const fetchedRecordsMap: FetchedRecordsMap = new Map();
  const fetchedIdsMap: FetchedIdsMap = new Map();
  await Promise.all(
    queries
      .filter((query) => query.target === 'query')
      .map(async (query) => {
        if (query.target !== 'query') {
          throw new Error('cannot be reached here');
        }
        const records = await queryRecords(conn, query, describer, options);
        const ids = new Set([...records.map((record) => record.Id)]);
        fetchedRecordsMap.set(query.object, records);
        fetchedIdsMap.set(query.object, ids);
      }),
  );
  return { fetchedRecordsMap, fetchedIdsMap };
}

function getFetchingIds(
  object: string,
  fetchedRecordsMap: FetchedRecordsMap,
  fetchedIds: Set<string>,
  describer: Describer,
  options: DumpOptions,
) {
  const fetchingIds = new Set<string>();
  for (const objectKey of fetchedRecordsMap.keys()) {
    const description = describer.findSObjectDescription(objectKey);
    if (!description) {
      continue;
    }
    const { fields } = description;
    const fetchedRecords = fetchedRecordsMap.get(objectKey) ?? [];
    for (const field of fields) {
      if (
        field.createable &&
        field.type === 'reference' &&
        includesInNamespace(
          field.referenceTo ?? [],
          object,
          options.defaultNamespace,
        )
      ) {
        for (const record of fetchedRecords) {
          const refId: string | undefined = getRecordFieldValue(
            record,
            field.name,
            options.defaultNamespace,
          );
          if (refId && !fetchedIds.has(refId)) {
            fetchingIds.add(refId);
          }
        }
      }
    }
  }
  return fetchingIds;
}

async function fetchDependentRecords(
  conn: Connection,
  query: { object: string } & RelatedTarget,
  fetchedRecordsMap: FetchedRecordsMap,
  fetchedIds: Set<string>,
  describer: Describer,
  options: DumpOptions,
) {
  const fields = getTargetFields(query, describer);
  const fetchingIds = getFetchingIds(
    query.object,
    fetchedRecordsMap,
    fetchedIds,
    describer,
    options,
  );
  if (fetchingIds.size === 0) {
    return [];
  }
  const soql = `SELECT ${fields.join(', ')} FROM ${
    query.object
  } WHERE Id IN ('${Array.from(fetchingIds).join("','")}')`;
  return executeQuery(conn, soql, options);
}

async function fetchAllDependentRecords(
  conn: Connection,
  queries: DumpQuery[],
  fetchedRecordsMap: FetchedRecordsMap,
  fetchedIdsMap: FetchedIdsMap,
  describer: Describer,
  options: DumpOptions,
) {
  const newlyFetchedIdsMap: FetchedIdsMap = new Map();
  for (const query of queries) {
    if (query.target !== 'related') {
      continue;
    }
    const fetchedRecords = fetchedRecordsMap.get(query.object) ?? [];
    const fetchedIds = fetchedIdsMap.get(query.object) ?? new Set<string>();
    const newlyFetchedIds = new Set<string>();
    const records = await fetchDependentRecords(
      conn,
      query,
      fetchedRecordsMap,
      fetchedIds,
      describer,
      options,
    );
    for (const record of records) {
      const id = record.Id;
      fetchedRecords.push(record);
      fetchedIds.add(id);
      newlyFetchedIds.add(id);
    }
    fetchedRecordsMap.set(query.object, fetchedRecords);
    fetchedIdsMap.set(query.object, fetchedIds);
    newlyFetchedIdsMap.set(query.object, newlyFetchedIds);
  }
  return newlyFetchedIdsMap;
}

function getParentRelationsMap(
  object: string,
  newlyFetchedIdsMap: FetchedIdsMap,
  describer: Describer,
  options: DumpOptions,
) {
  const parentRelationsMap = new Map<string, Set<string>>();
  const description = describer.findSObjectDescription(object);
  if (!description) {
    throw new Error(`No object description found: ${object}`);
  }
  const { fields } = description;
  for (const field of fields) {
    if (field.createable && field.type === 'reference') {
      for (const refObject of field.referenceTo ?? []) {
        const newlyFetchedIds = getMapValue(
          newlyFetchedIdsMap,
          refObject,
          options.defaultNamespace,
        );
        if (!newlyFetchedIds || newlyFetchedIds.size === 0) {
          continue;
        }
        const refIds = new Set([
          ...(parentRelationsMap.get(field.name) ?? []),
          ...newlyFetchedIds,
        ]);
        parentRelationsMap.set(field.name, refIds);
      }
    }
  }
  return parentRelationsMap;
}

async function fetchRelatedRecords(
  conn: Connection,
  query: { object: string } & RelatedTarget,
  newlyFetchedIdsMap: FetchedIdsMap,
  describer: Describer,
  options: DumpOptions,
) {
  const fields = getTargetFields(query, describer);
  const parentRelationsMap = getParentRelationsMap(
    query.object,
    newlyFetchedIdsMap,
    describer,
    options,
  );
  const conditions = Array.from(parentRelationsMap.entries()).map(
    ([refField, refIds]) =>
      `${refField} IN ('${Array.from(refIds.values()).join("', '")}')`,
  );
  if (conditions.length === 0) {
    return [];
  }
  const soql = `SELECT ${fields.join(', ')} FROM ${
    query.object
  } WHERE ${conditions.join(' OR ')}`;
  return executeQuery(conn, soql, options);
}

async function fetchAllRelatedRecords(
  conn: Connection,
  queries: DumpQuery[],
  fetchedRecordsMap: FetchedRecordsMap,
  fetchedIdsMap: FetchedIdsMap,
  newlyFetchedIdsMap: FetchedIdsMap,
  describer: Describer,
  options: DumpOptions,
) {
  for (const query of queries) {
    if (query.target !== 'related') {
      continue;
    }
    const fetchedRecords = fetchedRecordsMap.get(query.object) ?? [];
    const fetchedIds = fetchedIdsMap.get(query.object) ?? new Set<string>();
    const newlyFetchedIds =
      newlyFetchedIdsMap.get(query.object) ?? new Set<string>();
    const records = await fetchRelatedRecords(
      conn,
      query,
      newlyFetchedIdsMap,
      describer,
      options,
    );
    for (const record of records) {
      const id = record.Id;
      if (!fetchedIds.has(id)) {
        fetchedRecords.push(record);
        fetchedIds.add(id);
        newlyFetchedIds.add(id);
      }
    }
    fetchedRecordsMap.set(query.object, fetchedRecords);
    fetchedIdsMap.set(query.object, fetchedIds);
    newlyFetchedIdsMap.set(query.object, newlyFetchedIds);
  }
  return newlyFetchedIdsMap;
}

function calcFetchedCount(fetchedIdsMap: FetchedIdsMap) {
  return Array.from(fetchedIdsMap.keys())
    .map(
      (object) =>
        [object, fetchedIdsMap.get(object)?.size ?? 0] as [string, number],
    )
    .reduce(
      ([fetchedCount, fetchedCountPerObject], [object, count]) =>
        [
          fetchedCount + count,
          {
            ...fetchedCountPerObject,
            [object]: count,
          },
        ] as [number, Record<string, number>],
      [0, {}] as [number, Record<string, number>],
    );
}

async function dumpRecordsAsCSV(
  queries: DumpQuery[],
  fetchedRecordsMap: FetchedRecordsMap,
  describer: Describer,
  options: DumpOptions,
) {
  const { idMap } = options;
  const origIdMap = idMap
    ? new Map(
        Array.from(idMap.entries()).map(([origId, newId]) => [newId, origId]),
      )
    : undefined;
  return Promise.all(
    queries.map(async (query) => {
      const fieldDefs = getTargetFieldDefinitions(query, describer);
      const columns = fieldDefs.map((f) => ({
        // as the records are fetched with no default-namespced field name
        key: options.defaultNamespace
          ? removeNamespace(f.name, options.defaultNamespace)
          : f.name,
        header: f.name,
      }));
      let records = fetchedRecordsMap.get(query.object) ?? [];
      if (origIdMap) {
        const idFields = fieldDefs
          .filter((f) => f.type === 'id' || f.type === 'reference')
          .map((f) => f.name);
        records = records.map((record) =>
          idFields.reduce(
            (rec, field) => ({
              ...rec,
              [field]: origIdMap.get(rec[field]) ?? rec[field],
            }),
            record,
          ),
        );
      }
      return stringifyCSV(records, columns);
    }),
  );
}

/**
 *
 */
export async function dumpAsCSVData(
  conn: Connection,
  queries: DumpQuery[],
  reportProgress: (progress: DumpProgress) => void,
  options: DumpOptions = {},
) {
  const queryObjects = queries.map((query) => query.object);
  const describer = await describeSObjects(conn, queryObjects, options);
  const { fetchedRecordsMap, fetchedIdsMap } = await queryPrimaryRecords(
    conn,
    queries,
    describer,
    options,
  );
  let prevCount = 0;
  let [fetchedCount, fetchedCountPerObject] = calcFetchedCount(fetchedIdsMap);
  reportProgress({ fetchedCount, fetchedCountPerObject });
  let newlyFetchedIdsMap = fetchedIdsMap;
  while (prevCount < fetchedCount) {
    prevCount = fetchedCount;
    await fetchAllRelatedRecords(
      conn,
      queries,
      fetchedRecordsMap,
      fetchedIdsMap,
      newlyFetchedIdsMap,
      describer,
      options,
    );
    [fetchedCount, fetchedCountPerObject] = calcFetchedCount(fetchedIdsMap);
    reportProgress({ fetchedCount, fetchedCountPerObject });
    newlyFetchedIdsMap = await fetchAllDependentRecords(
      conn,
      queries,
      fetchedRecordsMap,
      fetchedIdsMap,
      describer,
      options,
    );
    [fetchedCount, fetchedCountPerObject] = calcFetchedCount(fetchedIdsMap);
    reportProgress({ fetchedCount, fetchedCountPerObject });
  }
  return dumpRecordsAsCSV(queries, fetchedRecordsMap, describer, options);
}
