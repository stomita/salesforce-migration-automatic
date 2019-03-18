import { Connection } from "jsforce";
import parse from "csv-parse";
import {
  DescribeSObjectResultMap,
  UploadInput,
  UploadResult,
  UploadStatus,
  UploadProgress
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

function findFieldDescription(
  object: string,
  fieldName: string,
  descriptions: DescribeSObjectResultMap
) {
  const description = descriptions[object];
  return description.fields.find(field => field.name === fieldName);
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
        ridIndexes.push(i);
      }
    }
  });
  if (idIndex == null) {
    throw new Error(`No id type field is listed for: ${object}`);
  }
  const uploadables: string[][] = [];
  const waitings: string[][] = [];

  const hasTargets = Object.keys(targetIds).length > 0;

  for (const row of rows) {
    const id = row[idIndex];
    let isUploadable = !hasTargets || targetIds[id];
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
  return { uploadables, waitings };
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
  const successes: [string, string][] = [];
  const failures: [string, Error[]][] = [];
  for (const [table, recordIdPairs] of Object.entries(uploadings)) {
    const records = recordIdPairs.map(({ record }) => record);
    const rets = await conn
      .sobject(table)
      .create(records, { allowRecursive: true } as any);
    if (Array.isArray(rets)) {
      rets.forEach((ret, i) => {
        const origId = recordIdPairs[i].id;
        if (ret.success) {
          // register map info of oldid -> newid
          idMap[origId] = ret.id;
          successes.push([origId, ret.id]);
        } else {
          failures.push([origId, ret.errors]);
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
 * Upload Current loaded content to
 */
async function upload(
  conn: Connection,
  datasets: LoadDataset[],
  reportProgress: (progress: UploadProgress) => void
) {
  const totalCount = calcTotalUploadCount(datasets);
  const targetIds: Record<string, boolean> = {};
  const idMap: Record<string, string> = {};
  const objects = datasets.map(({ object }) => object);
  const descriptions = await describeSObjects(conn, objects);
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
  reportUpload: (status: UploadProgress) => void,
  options: Object = {}
) {
  const datasets = await parseCSVInputs(inputs, options);
  return upload(conn, datasets, reportUpload);
}
