import { EventEmitter } from "events";
import { Connection, DescribeSObjectResult, Record as SFRecord } from "jsforce";
import parse from "csv-parse";
import stringify from "csv-stringify";

type SObjectFieldDescription = {
  name: string;
  type: string;
  label: string;
  createable: boolean;
  referenceTo?: string[] | null | undefined;
};

type SObjectDescription = {
  name: string;
  label: string;
  fields: SObjectFieldDescription[];
};

type LoadData = {
  headers: string[];
  rows: string[][];
};

type RecordIdPair = {
  id: string;
  record: Record<string, any>;
};

export type UploadResult = {
  totalCount: number;
  successes: Array<[string, string]>;
  failures: Array<[string, any]>;
};

type RelatedTarget = {
  target: "related";
};

type QueryTarget = {
  target: "query";
  condition?: string;
  orderby?: string;
  limit?: number;
  offset?: number;
  scope?: string;
};

type DumpTarget = QueryTarget | RelatedTarget;

export type DumpQuery = {
  object: string;
  fields?: string[];
} & DumpTarget;

/*
 * Import other org data, exported from salesforce.com (via DataLoader) This
 * class automatically resolves inter-record dependencies.
 */
export class AutoMigrator extends EventEmitter {
  private _described: Record<string, Promise<SObjectDescription>> = {};
  private _dataMap: Record<string, LoadData | undefined> = {};
  private _idMap: Record<string, any> = {};
  private _target: Record<string, boolean> = {};
  private _successes: Array<[string, string]> = [];
  private _failures: Array<[string, any]> = [];
  private _conn: Connection;

  constructor(conn: Connection) {
    super();
    this._conn = conn;
  }

  private async describe(table: string) {
    let described = this._described[table];
    if (!described) {
      described = this._conn.describe(table);
      this._described[table] = described;
    }
    return described;
  }

  private async getFieldDef(table: string, fname: string) {
    const { fields } = await this.describe(table);
    const fnameUpper = fname.toUpperCase();
    return fields.find(f => f.name.toUpperCase() === fnameUpper);
  }

  private async getType(table: string, fname: string) {
    var f = await this.getFieldDef(table, fname);
    return f ? f.type : undefined;
  }

  private setUploadingTarget(ids: string[]) {
    for (const id of ids) {
      this._target[id] = true;
    }
  }

  private isTargetedUpload() {
    return Object.keys(this._target).length > 0;
  }

  private async filterUploadableRecords(table: string) {
    const dataset = this._dataMap[table];
    if (!dataset) {
      throw new Error(`No dataset is loaded for: ${table}`);
    }
    const { headers, rows } = dataset;

    // search id and reference id column index
    let idIndex: number | undefined = undefined;
    const ridIndexes: number[] = [];
    await Promise.all(
      headers.map(async (h, i) => {
        const type = await this.getType(table, h);
        if (type === "id") {
          idIndex = i;
        } else if (type === "reference") {
          ridIndexes.push(i);
        }
      })
    );
    if (idIndex == null) {
      throw new Error(`No id type field is listed for: ${table}`);
    }
    const uploadables: string[][] = [];
    const waitings: string[][] = [];

    const isTargeted = this.isTargetedUpload();

    for (const row of rows) {
      const id = row[idIndex];
      let isUploadable = !isTargeted || this._target[id];
      for (const idx of ridIndexes) {
        const refId = row[idx];
        if (refId) {
          if (isTargeted) {
            if (this._target[refId]) {
              // if parent record is in targets
              this._target[id] = true; // child record should be in targets,
              // also.
            } else if (this._target[id]) {
              // if child record is in targets
              this._target[refId] = true; // parent record should be in targets,
              // also.
            }
          }
          if (!this._idMap[refId]) {
            // if parent record not uploaded
            isUploadable = false;
          }
        }
      }
      if (isUploadable) {
        uploadables.push(row);
      } else {
        waitings.push(row);
      }
    }
    dataset.rows = waitings;
    this._dataMap[table] = dataset;
    return uploadables;
  }

  private async convertToRecordIdPair(table: string, row: string[]) {
    let id: string | undefined;
    const record: Record<string, any> = {};
    const dataset = this._dataMap[table];
    if (!dataset) {
      throw new Error(`No dataset is loaded for: ${table}`);
    }
    const { headers } = dataset;
    await Promise.all(
      row.map(async (value, i) => {
        const field = await this.getFieldDef(table, headers[i]);
        if (field == null) {
          return;
        }
        const { name, type, createable } = field;
        switch (type) {
          case "id":
            id = value;
            break;
          case "int":
            {
              const num = parseInt(value);
              if (!isNaN(num) && createable) {
                record[name] = num;
              }
            }
            break;
          case "double":
          case "currency":
          case "percent":
            {
              const fnum = parseFloat(value);
              if (!isNaN(fnum) && createable) {
                record[name] = fnum;
              }
            }
            break;
          case "date":
          case "datetime":
            if (value && createable) {
              record[name] = value;
            }
            break;
          case "reference":
            if (createable) {
              record[name] = this._idMap[value];
            }
            break;
          default:
            if (createable) {
              record[name] = value;
            }
            break;
        }
      })
    );
    if (!id) {
      throw new Error(`No id type field is found: ${table}, ${row.join(", ")}`);
    }
    return { id, record };
  }

  private async uploadRecords(uploadings: Record<string, RecordIdPair[]>) {
    for (const [table, recordIdPairs] of Object.entries(uploadings)) {
      const records = recordIdPairs.map(({ record }) => record);
      const rets = await this._conn
        .sobject(table)
        .create(records, { allowRecursive: true } as any);
      if (Array.isArray(rets)) {
        rets.forEach((ret, i) => {
          const origId = recordIdPairs[i].id;
          if (ret.success) {
            // register map info of oldid -> newid
            this._idMap[origId] = ret.id;
            this._successes.push([origId, ret.id]);
          } else {
            this._failures.push([origId, ret.errors]);
          }
        });
      }
    }
  }

  private calcTotalUploadCount() {
    let totalCount = 0;
    for (const table of Object.keys(this._dataMap)) {
      const dataset = this._dataMap[table];
      if (dataset) {
        totalCount += dataset.rows.length;
      }
    }
    return totalCount;
  }

  private async uploadInternal(totalCount: number): Promise<UploadResult> {
    // array of sobj and recordId (old) pair
    const uploadings: Record<string, RecordIdPair[]> = {};
    for (const table of Object.keys(this._dataMap)) {
      const rows = await this.filterUploadableRecords(table);
      const recordIdPairs = await Promise.all(
        rows.map(row => this.convertToRecordIdPair(table, row))
      );
      if (recordIdPairs.length > 0) {
        uploadings[table] = recordIdPairs;
      }
    }
    if (Object.keys(uploadings).length > 0) {
      await this.uploadRecords(uploadings);
      // event notification;
      const successCount = this._successes.length;
      const failureCount = this._failures.length;
      this.emit("uploadProgress", { totalCount, successCount, failureCount });
      // recursive call
      return this.uploadInternal(totalCount);
    } else {
      const successes = this._successes;
      const failures = this._failures;
      this.emit("complete");
      return { totalCount, successes, failures };
    }
  }

  /**
   * Upload Current loaded content to
   */
  async upload() {
    const totalCount = this.calcTotalUploadCount();
    return this.uploadInternal(totalCount);
  }

  /**
   * Load CSV text data in memory in order to upload to Salesforce
   *
   * @param table
   * @param csvData
   * @param options
   */
  async loadCSVData(table: string, csvData: string, options: Object = {}) {
    const [headers, ...rows] = await new Promise<string[][]>(
      (resolve, reject) => {
        parse(csvData, options, (err: Error | undefined, rets: string[][]) => {
          if (err) {
            reject(err);
          } else {
            resolve(rets);
          }
        });
      }
    );
    this._dataMap[table] = { headers, rows };
  }

  /**
   * Dump the record data as CSV
   * @param queries
   */
  async dumpAsCSVData(queries: DumpQuery[]) {
    const conn = this._conn;
    const queryObjects = queries.map(query => query.object);
    console.log("describing sobjects", queryObjects);
    const descriptions = await describeSObjects(conn, queryObjects);
    console.log("querying primary records");
    const { fetchedRecordsMap, fetchedIdsMap } = await queryPrimaryRecords(
      conn,
      queries,
      descriptions
    );
    let prevCount = 0;
    let fetchedCount = calcFetchedCount(fetchedIdsMap);
    let newlyFetchedIdsMap = fetchedIdsMap;
    while (prevCount < fetchedCount) {
      prevCount = fetchedCount;
      console.log("fetch related records");
      await fetchAllRelatedRecords(
        conn,
        queries,
        fetchedRecordsMap,
        fetchedIdsMap,
        newlyFetchedIdsMap,
        descriptions
      );
      console.log("fetch dependent records");
      newlyFetchedIdsMap = await fetchAllDependentRecords(
        conn,
        queries,
        fetchedRecordsMap,
        fetchedIdsMap,
        descriptions
      );
      fetchedCount = calcFetchedCount(fetchedIdsMap);
      this.emit("dumpProgress", { fetchedCount });
    }
    return dumpRecordsAsCSV(queries, fetchedRecordsMap, descriptions);
  }
}

/**
 *
 */
type DescribeSObjectResultMap = Record<string, DescribeSObjectResult>;
type FetchedRecordsMap = Record<string, SFRecord[]>;
type FetchedIdsMap = Record<string, Set<string>>;

/**
 *
 * @param conn
 * @param objects
 */
async function describeSObjects(conn: Connection, objects: string[]) {
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
  console.log("execute query =>", soql);
  const records = await new Promise<SFRecord[]>((resolve, reject) => {
    const records: SFRecord[] = [];
    conn
      .query(soql)
      .maxFetch(10000)
      .on("data", record => records.push(record))
      .on("end", () => resolve(records))
      .on("error", err => reject(err));
  });
  console.log("execute query: done");
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
    .map(object => fetchedIdsMap[object].size)
    .reduce((cnt1, cnt2) => cnt1 + cnt2);
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
