import { Connection, Record as SFRecord } from "jsforce";
import parse from "csv-parse";
import {
  DescribeSObjectResultMap,
  UploadInput,
  UploadResult,
  UploadStatus,
  UploadProgress,
  RecordMappingPolicy
} from "./types";
import { describeSObjects } from "./describe";

type RecordIdPair = {
  id: string;
  record: Record<string, any>;
};

type LoadDataset = {
  object: string;
  headers: string[];
  rows: string[][];
};

function hasTargets(targetIds: Record<string, boolean>) {
  return Object.keys(targetIds).length > 0;
}

function removeNamespace(identifier: string) {
  return identifier.replace(/^[a-zA-Z][a-zA-Z0-9]+__/, "");
}

function findSObjectDescription(
  object: string,
  descriptions: DescribeSObjectResultMap
) {
  let description = descriptions[object];
  if (!description) {
    description = descriptions[removeNamespace(object)];
  }
  return description;
}

function findFieldDescription(
  object: string,
  fieldName: string,
  descriptions: DescribeSObjectResultMap
) {
  const description = findSObjectDescription(object, descriptions);
  if (description) {
    let field = description.fields.find(({ name }) => name === fieldName);
    if (!field) {
      const fieldNameNoNamespace = removeNamespace(fieldName);
      field = description.fields.find(
        ({ name }) => name === fieldNameNoNamespace
      );
    }
    return field;
  }
}

function filterUploadableRecords(
  { object, headers, rows }: LoadDataset,
  targetIds: Record<string, boolean>,
  idMap: Record<string, string>,
  descriptions: DescribeSObjectResultMap
) {
  // search id and reference id column index
  let idIndex: number | undefined = undefined;
  let ridIndexes: number[] = [];
  headers.forEach((header, i) => {
    const field = findFieldDescription(object, header, descriptions);
    if (field) {
      const { type } = field;
      if (type === "id") {
        idIndex = i;
      } else if (type === "reference") {
        const { referenceTo } = field;
        for (const refObject of referenceTo || []) {
          if (findSObjectDescription(refObject, descriptions)) {
            ridIndexes.push(i);
            break;
          }
        }
      }
    }
  });
  if (idIndex == null) {
    throw new Error(`No id type field is listed for: ${object}`);
  }
  const uploadables: string[][] = [];
  const waitings: string[][] = [];
  const notloadables: string[][] = [];

  for (const row of rows) {
    const id = row[idIndex];
    if (idMap[id]) {
      // already mapped
      notloadables.push(row);
      continue;
    }
    let isUploadable = !hasTargets(targetIds) || targetIds[id];
    for (const idx of ridIndexes) {
      const refId = row[idx];
      if (refId) {
        if (targetIds[refId]) {
          // if parent record is in targets
          targetIds[id] = true; // child record should be in targets, too.
        } else if (targetIds[id]) {
          // if child record is in targets
          targetIds[refId] = true; // parent record should be in targets, too.
        }
        if (!idMap[refId]) {
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
  return { uploadables, waitings, notloadables };
}

function convertToRecordIdPair(
  { object, headers }: LoadDataset,
  row: string[],
  idMap: Record<string, string>,
  descriptions: DescribeSObjectResultMap
) {
  let id: string | undefined;
  const record: Record<string, any> = {};
  row.forEach((value, i) => {
    const field = findFieldDescription(object, headers[i], descriptions);
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
      case "boolean":
        if (createable) {
          record[name] = !/^(|0|n|f|false)$/i.test(value);
        }
        break;
      case "reference":
        if (createable) {
          record[name] = idMap[value];
        }
        break;
      default:
        if (createable) {
          record[name] = value;
        }
        break;
    }
  });
  if (!id) {
    throw new Error(`No id type field is found: ${object}, ${row.join(", ")}`);
  }
  return { id, record };
}

async function uploadRecords(
  conn: Connection,
  uploadings: Record<string, RecordIdPair[]>,
  idMap: Record<string, string>
) {
  const successes: UploadStatus["successes"] = [];
  const failures: UploadStatus["failures"] = [];
  for (const [object, recordIdPairs] of Object.entries(uploadings)) {
    const records = recordIdPairs.map(({ record }) => record);
    const rets = await conn
      .sobject(object)
      .create(records, { allowRecursive: true } as any);
    if (Array.isArray(rets)) {
      rets.forEach((ret, i) => {
        const record = records[i];
        const origId = recordIdPairs[i].id;
        if (ret.success) {
          // register map info of oldid -> newid
          idMap[origId] = ret.id;
          successes.push({ object, origId, newId: ret.id, record });
        } else {
          failures.push({ object, origId, errors: ret.errors, record });
        }
      });
    }
  }
  return { successes, failures };
}

function calcTotalUploadCount(datasets: LoadDataset[]) {
  let totalCount = 0;
  for (const dataset of datasets) {
    if (dataset) {
      totalCount += dataset.rows.length;
    }
  }
  return totalCount;
}

async function uploadDatasets(
  conn: Connection,
  datasets: LoadDataset[],
  targetIds: Record<string, boolean>,
  idMap: Record<string, string>,
  descriptions: DescribeSObjectResultMap,
  uploadStatus: UploadStatus,
  reportProgress: (progress: UploadProgress) => void
): Promise<UploadResult> {
  // array of sobj and recordId (old) pair
  const uploadings: Record<string, RecordIdPair[]> = {};
  for (const dataset of datasets) {
    const { uploadables, waitings } = filterUploadableRecords(
      dataset,
      targetIds,
      idMap,
      descriptions
    );
    const uploadRecordIdPairs = uploadables.map(row =>
      convertToRecordIdPair(dataset, row, idMap, descriptions)
    );
    if (uploadRecordIdPairs.length > 0) {
      uploadings[dataset.object] = uploadRecordIdPairs;
    }
    dataset.rows = waitings;
  }
  if (Object.keys(uploadings).length > 0) {
    const { successes, failures } = await uploadRecords(
      conn,
      uploadings,
      idMap
    );
    const totalCount = uploadStatus.totalCount;
    // event notification;
    const newUploadStatus = {
      totalCount,
      successes: [...uploadStatus.successes, ...successes],
      failures: [...uploadStatus.failures, ...failures]
    };
    const successCount = newUploadStatus.successes.length;
    const failureCount = newUploadStatus.failures.length;
    reportProgress({ totalCount, successCount, failureCount });
    // recursive call
    return uploadDatasets(
      conn,
      datasets,
      targetIds,
      idMap,
      descriptions,
      newUploadStatus,
      reportProgress
    );
  } else {
    return uploadStatus;
  }
}

/**
 *
 */
async function getExistingIdMap(
  conn: Connection,
  dataset: LoadDataset,
  keyField: string,
  descriptions: DescribeSObjectResultMap
) {
  const { object, headers, rows } = dataset;
  let idIndex = -1;
  let keyIndex = -1;
  headers.forEach((header, i) => {
    if (header === keyField) {
      keyIndex = i;
      return;
    }
    const field = findFieldDescription(object, header, descriptions);
    if (field && field.type === "id") {
      idIndex = i;
    }
  });
  if (idIndex < 0 || keyIndex < 0) {
    return {};
  }
  const keyMap = rows.reduce(
    (keyMap, row) => {
      const id = row[idIndex];
      const keyValue = row[keyIndex];
      if (id == null || id === "" || keyValue == null || keyValue === "") {
        return keyMap;
      }
      return { ...keyMap, [keyValue]: id };
    },
    {} as Record<string, string>
  );
  const keyValues = Array.from(new Set(Object.keys(keyMap)));
  const records: SFRecord[] =
    keyValues.length === 0
      ? []
      : await conn.sobject(object).find(
          {
            [keyField]: keyValues
          },
          ["Id", keyField]
        );
  const newKeyMap = records.reduce(
    (newKeyMap, record) => {
      const keyValue: string = record[keyField];
      if (keyValue == null) {
        return newKeyMap;
      }
      return {
        ...newKeyMap,
        [keyValue]: record.Id as string
      };
    },
    {} as Record<string, string>
  );
  return Object.keys(keyMap).reduce(
    (idMap, keyValue) => {
      const id = keyMap[keyValue];
      const newId = newKeyMap[keyValue];
      if (id == null || newId == null) {
        return idMap;
      }
      return {
        ...idMap,
        [id]: newId
      };
    },
    {} as Record<string, string>
  );
}

/**
 *
 */
async function getAllExistingIdMap(
  conn: Connection,
  datasets: LoadDataset[],
  mappingPolicies: RecordMappingPolicy[],
  descriptions: DescribeSObjectResultMap
) {
  const datasetMap = datasets.reduce(
    (datasetMap, dataset) => ({
      ...datasetMap,
      [dataset.object]: dataset
    }),
    {} as Record<string, LoadDataset>
  );
  const idMap = (await Promise.all(
    mappingPolicies.map(({ object, keyField }) => {
      const dataset = datasetMap[object];
      if (!dataset) {
        throw new Error(`Input is not found for mapping object: ${object}`);
      }
      return getExistingIdMap(conn, dataset, keyField, descriptions);
    })
  )).reduce(
    (idMap, ids) => ({
      ...idMap,
      ...ids
    }),
    {} as Record<string, string>
  );

  return idMap;
}

/**
 *
 */
async function upload(
  conn: Connection,
  datasets: LoadDataset[],
  mappingPolicies: RecordMappingPolicy[],
  reportProgress: (progress: UploadProgress) => void
) {
  const totalCount = calcTotalUploadCount(datasets);
  const targetIds: Record<string, boolean> = {};
  const objects = datasets.map(({ object }) => object);
  const descriptions = await describeSObjects(conn, objects);
  const idMap = await getAllExistingIdMap(
    conn,
    datasets,
    mappingPolicies,
    descriptions
  );
  const uploadStatus = { totalCount, successes: [], failures: [] };
  return uploadDatasets(
    conn,
    datasets,
    targetIds,
    idMap,
    descriptions,
    uploadStatus,
    reportProgress
  );
}

async function parseCSVInputs(inputs: UploadInput[], options: Object) {
  return Promise.all(
    inputs.map(async input => {
      const { object, csvData } = input;
      const [headers, ...rows] = await new Promise<string[][]>(
        (resolve, reject) => {
          parse(
            csvData,
            options,
            (err: Error | undefined, rets: string[][]) => {
              if (err) {
                reject(err);
              } else {
                resolve(rets);
              }
            }
          );
        }
      );
      return { object, headers, rows };
    })
  );
}

/**
 * Load CSV text data in memory in order to upload to Salesforce
 */
export async function loadCSVData(
  conn: Connection,
  inputs: UploadInput[],
  mappingPolicies: RecordMappingPolicy[],
  reportUpload: (status: UploadProgress) => void,
  options: Object = {}
) {
  const datasets = await parseCSVInputs(inputs, options);
  return upload(conn, datasets, mappingPolicies, reportUpload);
}
