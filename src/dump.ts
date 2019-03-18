import { Connection, Record as SFRecord } from "jsforce";
import stringify from "csv-stringify";
import {
  DescribeSObjectResultMap,
  DumpQuery,
  QueryTarget,
  RelatedTarget
} from "./types";
import { describeSObjects } from "./describe";

/**
 *
 */
type FetchedRecordsMap = Record<string, SFRecord[]>;
type FetchedIdsMap = Record<string, Set<string>>;

function getTargetFields(
  query: DumpQuery,
  descriptions: DescribeSObjectResultMap
) {
  if (query.target === "query" && query.fields) {
    return query.fields;
  }
  return descriptions[query.object].fields.map(field => field.name);
}

async function executeQuery(conn: Connection, soql: string) {
  const records = await new Promise<SFRecord[]>((resolve, reject) => {
    const records: SFRecord[] = [];
    conn
      .query(soql)
      .maxFetch(10000)
      .on("data", record => records.push(record))
      .on("end", () => resolve(records))
      .on("error", err => reject(err));
  });
  return records;
}

async function queryRecords(
  conn: Connection,
  query: { object: string } & QueryTarget,
  descriptions: DescribeSObjectResultMap
) {
  const fields = getTargetFields(query, descriptions);
  const soql = `SELECT ${fields.join(", ")} FROM ${query.object}`;
  return executeQuery(conn, soql);
}

async function queryPrimaryRecords(
  conn: Connection,
  queries: DumpQuery[],
  descriptions: DescribeSObjectResultMap
) {
  const fetchedRecordsMap: FetchedRecordsMap = {};
  const fetchedIdsMap: FetchedIdsMap = {};
  await Promise.all(
    queries
      .filter(query => query.target === "query")
      .map(async query => {
        if (query.target !== "query") {
          throw new Error("cannot be reached here");
        }
        const records = await queryRecords(conn, query, descriptions);
        const ids = new Set([...records.map(record => record.Id)]);
        fetchedRecordsMap[query.object] = records;
        fetchedIdsMap[query.object] = ids;
      })
  );
  return { fetchedRecordsMap, fetchedIdsMap };
}

function getFetchingIds(
  object: string,
  fetchedRecordsMap: FetchedRecordsMap,
  fetchedIds: Set<string>,
  descriptions: DescribeSObjectResultMap
) {
  const fetchingIds = new Set<string>();
  for (const objectKey of Object.keys(fetchedRecordsMap)) {
    const description = descriptions[objectKey];
    if (!description) {
      continue;
    }
    const { fields } = description;
    const fetchedRecords = fetchedRecordsMap[objectKey] || [];
    for (const field of fields) {
      if (
        field.createable &&
        field.type === "reference" &&
        (field.referenceTo || []).indexOf(object) >= 0
      ) {
        for (const record of fetchedRecords) {
          const refId = record[field.name];
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
  descriptions: DescribeSObjectResultMap
) {
  const fields = getTargetFields(query, descriptions);
  const fetchingIds = getFetchingIds(
    query.object,
    fetchedRecordsMap,
    fetchedIds,
    descriptions
  );
  if (fetchingIds.size === 0) {
    return [];
  }
  const soql = `SELECT ${fields.join(", ")} FROM ${
    query.object
  } WHERE Id IN ('${Array.from(fetchingIds).join("','")}')`;
  return executeQuery(conn, soql);
}

async function fetchAllDependentRecords(
  conn: Connection,
  queries: DumpQuery[],
  fetchedRecordsMap: FetchedRecordsMap,
  fetchedIdsMap: FetchedIdsMap,
  descriptions: DescribeSObjectResultMap
) {
  const newlyFetchedIdsMap: FetchedIdsMap = {};
  for (const query of queries) {
    if (query.target !== "related") {
      continue;
    }
    const fetchedRecords = fetchedRecordsMap[query.object] || [];
    const fetchedIds = fetchedIdsMap[query.object] || new Set<string>();
    const newlyFetchedIds = new Set<string>();
    const records = await fetchDependentRecords(
      conn,
      query,
      fetchedRecordsMap,
      fetchedIds,
      descriptions
    );
    for (const record of records) {
      const id = record.Id;
      fetchedRecords.push(record);
      fetchedIds.add(id);
      newlyFetchedIds.add(id);
    }
    fetchedRecordsMap[query.object] = fetchedRecords;
    fetchedIdsMap[query.object] = fetchedIds;
    newlyFetchedIdsMap[query.object] = newlyFetchedIds;
  }
  return newlyFetchedIdsMap;
}

function getParentRelationsMap(
  object: string,
  newlyFetchedIdsMap: FetchedIdsMap,
  descriptions: DescribeSObjectResultMap
) {
  const parentRelationsMap: Record<string, Set<string>> = {};
  const description = descriptions[object];
  const { fields } = description;
  for (const field of fields) {
    if (field.createable && field.type === "reference") {
      for (const refObject of field.referenceTo || []) {
        const newlyFetchedIds = newlyFetchedIdsMap[refObject];
        if (!newlyFetchedIds || newlyFetchedIds.size === 0) {
          continue;
        }
        const refIds = parentRelationsMap[field.name] || new Set<string>();
        for (const newId of Array.from(newlyFetchedIds)) {
          refIds.add(newId);
        }
        parentRelationsMap[field.name] = refIds;
      }
    }
  }
  return parentRelationsMap;
}

async function fetchRelatedRecords(
  conn: Connection,
  query: { object: string } & RelatedTarget,
  newlyFetchedIdsMap: FetchedIdsMap,
  descriptions: DescribeSObjectResultMap
) {
  const fields = getTargetFields(query, descriptions);
  const parentRelationsMap = getParentRelationsMap(
    query.object,
    newlyFetchedIdsMap,
    descriptions
  );
  const conditions = Object.keys(parentRelationsMap).map(refField => {
    const refIds = parentRelationsMap[refField];
    return `${refField} IN ('${Array.from(refIds.values()).join("', '")}')`;
  });
  if (conditions.length === 0) {
    return [];
  }
  const soql = `SELECT ${fields.join(", ")} FROM ${
    query.object
  } WHERE ${conditions.join(" OR ")}`;
  return executeQuery(conn, soql);
}

async function fetchAllRelatedRecords(
  conn: Connection,
  queries: DumpQuery[],
  fetchedRecordsMap: FetchedRecordsMap,
  fetchedIdsMap: FetchedIdsMap,
  newlyFetchedIdsMap: FetchedIdsMap,
  descriptions: DescribeSObjectResultMap
) {
  for (const query of queries) {
    if (query.target !== "related") {
      continue;
    }
    const fetchedRecords = fetchedRecordsMap[query.object] || [];
    const fetchedIds = fetchedIdsMap[query.object] || new Set<string>();
    const newlyFetchedIds =
      newlyFetchedIdsMap[query.object] || new Set<string>();
    const records = await fetchRelatedRecords(
      conn,
      query,
      newlyFetchedIdsMap,
      descriptions
    );
    for (const record of records) {
      const id = record.Id;
      if (!fetchedIds.has(id)) {
        fetchedRecords.push(record);
        fetchedIds.add(id);
        newlyFetchedIds.add(id);
      }
    }
    fetchedIdsMap[query.object] = fetchedIds;
    newlyFetchedIdsMap[query.object] = newlyFetchedIds;
  }
  return newlyFetchedIdsMap;
}

function calcFetchedCount(fetchedIdsMap: FetchedIdsMap) {
  return Object.keys(fetchedIdsMap)
    .map(object => [object, fetchedIdsMap[object].size] as [string, number])
    .reduce(
      ([fetchedCount, fetchedCountPerObject], [object, count]) =>
        [
          fetchedCount + count,
          {
            ...fetchedCountPerObject,
            [object]: count
          }
        ] as [number, Record<string, number>],
      [0, {}] as [number, Record<string, number>]
    );
}

async function dumpRecordsAsCSV(
  queries: DumpQuery[],
  fetchedRecordsMap: FetchedRecordsMap,
  descriptions: DescribeSObjectResultMap
) {
  return Promise.all(
    queries.map(async query => {
      const fields = getTargetFields(query, descriptions);
      const records = fetchedRecordsMap[query.object];
      return new Promise<string>((resolve, reject) => {
        stringify(records, { columns: fields, header: true }, (err, ret) => {
          if (err) {
            reject(err);
          } else {
            resolve(ret);
          }
        });
      });
    })
  );
}

/**
 *
 */
export async function dumpAsCSVData(
  conn: Connection,
  queries: DumpQuery[],
  reportProgress: (params: any) => void
) {
  const queryObjects = queries.map(query => query.object);
  const descriptions = await describeSObjects(conn, queryObjects);
  const { fetchedRecordsMap, fetchedIdsMap } = await queryPrimaryRecords(
    conn,
    queries,
    descriptions
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
      descriptions
    );
    [fetchedCount, fetchedCountPerObject] = calcFetchedCount(fetchedIdsMap);
    reportProgress({ fetchedCount, fetchedCountPerObject });
    newlyFetchedIdsMap = await fetchAllDependentRecords(
      conn,
      queries,
      fetchedRecordsMap,
      fetchedIdsMap,
      descriptions
    );
    [fetchedCount, fetchedCountPerObject] = calcFetchedCount(fetchedIdsMap);
    reportProgress({ fetchedCount, fetchedCountPerObject });
  }
  return dumpRecordsAsCSV(queries, fetchedRecordsMap, descriptions);
}
