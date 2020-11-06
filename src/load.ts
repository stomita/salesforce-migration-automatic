import { Connection, Record as SFRecord } from 'jsforce';
import parse from 'csv-parse';
import {
  UploadInput,
  UploadResult,
  UploadStatus,
  UploadProgress,
  RecordMappingPolicy,
  UploadOptions,
  RecordSpecifier,
} from './types';
import { describeSObjects, Describer } from './describe';

type RecordIdPair = {
  id: string;
  record: SFRecord;
};

type LoadDataset = {
  object: string;
  headers: string[];
  rows: string[][];
};

function filterUploadableRecords(
  { object, headers, rows }: LoadDataset,
  targetIds: Set<string>,
  idMap: Map<string, string>,
  describer: Describer,
) {
  // search id and reference id column index
  let idIndex: number | undefined = undefined;
  let refFields: Array<{ field: string; index: number }> = [];
  for (const [i, header] of headers.entries()) {
    const field = describer.findFieldDescription(object, header);
    if (field) {
      const { type } = field;
      if (type === 'id') {
        idIndex = i;
      } else if (type === 'reference') {
        const { name: fieldName, referenceTo } = field;
        for (const refObject of referenceTo || []) {
          if (describer.findSObjectDescription(refObject)) {
            refFields.push({ field: fieldName, index: i });
            break;
          }
        }
      }
    }
  }
  if (idIndex == null) {
    throw new Error(`No id type field is listed for: ${object}`);
  }
  const uploadables: string[][] = [];
  const waitings: Array<{
    row: string[];
    id: string;
    blockingField: string | undefined;
    blockingId: string | undefined;
  }> = [];
  const notloadables: string[][] = [];

  for (const row of rows) {
    const id = row[idIndex];
    if (idMap.has(id)) {
      // already mapped
      notloadables.push(row);
      continue;
    }
    let isUploadable = targetIds.size === 0 || targetIds.has(id);
    let blockingField: string | undefined = undefined;
    let blockingId: string | undefined = undefined;
    for (const refField of refFields) {
      const { index: refIdx, field } = refField;
      const refId = row[refIdx];
      if (refId) {
        if (targetIds.has(refId)) {
          // if parent record is in targets
          targetIds.add(id); // child record should be in targets, too.
        } else if (targetIds.has(id)) {
          // if child record is in targets
          targetIds.add(refId); // parent record should be in targets, too.
        }
        if (!idMap.has(refId)) {
          // if parent record not uploaded
          isUploadable = false;
          blockingField = field;
          blockingId = refId;
        }
      }
    }
    if (isUploadable) {
      uploadables.push(row);
    } else {
      waitings.push({ row, id, blockingField, blockingId });
    }
  }
  return { uploadables, waitings, notloadables };
}

function convertToRecordIdPair(
  { object, headers }: LoadDataset,
  row: string[],
  idMap: Map<string, string>,
  describer: Describer,
) {
  let id: string | undefined;
  const record: SFRecord = {};
  for (const [i, value] of row.entries()) {
    const field = describer.findFieldDescription(object, headers[i]);
    if (field == null) {
      continue;
    }
    const { name, type, createable } = field;
    switch (type) {
      case 'id':
        id = value;
        break;
      case 'int':
        {
          const num = parseInt(value);
          if (!isNaN(num) && createable) {
            record[name] = num;
          }
        }
        break;
      case 'double':
      case 'currency':
      case 'percent':
        {
          const fnum = parseFloat(value);
          if (!isNaN(fnum) && createable) {
            record[name] = fnum;
          }
        }
        break;
      case 'date':
      case 'datetime':
        if (value && createable) {
          record[name] = value;
        }
        break;
      case 'boolean':
        if (createable) {
          record[name] = !/^(|0|n|f|false)$/i.test(value);
        }
        break;
      case 'reference':
        if (createable) {
          record[name] = idMap.get(value);
        }
        break;
      default:
        if (createable) {
          record[name] = value;
        }
        break;
    }
  }
  if (!id) {
    throw new Error(`No id type field is found: ${object}, ${row.join(', ')}`);
  }
  return { id, record };
}

async function uploadRecords(
  conn: Connection,
  uploadings: Map<string, RecordIdPair[]>,
  idMap: Map<string, string>,
  describer: Describer,
) {
  const successes: UploadStatus['successes'] = [];
  const failures: UploadStatus['failures'] = [];
  for (const [object, recordIdPairs] of uploadings) {
    const description = describer.findSObjectDescription(object);
    if (!description) {
      throw new Error(`No object description found: ${object}`);
    }
    const records = recordIdPairs.map(({ record }) => record);
    const rets = await conn
      .sobject(description.name)
      .create(records, { allowRecursive: true } as any);
    if (Array.isArray(rets)) {
      rets.forEach((ret, i) => {
        const record = records[i];
        const origId = recordIdPairs[i].id;
        if (ret.success) {
          // register map info of oldid -> newid
          idMap.set(origId, ret.id);
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
  targetIds: Set<string>,
  idMap: Map<string, string>,
  describer: Describer,
  uploadStatus: UploadStatus,
  reportProgress: (progress: UploadProgress) => void,
): Promise<UploadResult> {
  // array of sobj and recordId (old) pair
  const uploadings = new Map<string, RecordIdPair[]>();
  const blocked: UploadStatus['blocked'] = [];
  for (const dataset of datasets) {
    const { uploadables, waitings } = filterUploadableRecords(
      dataset,
      targetIds,
      idMap,
      describer,
    );
    const uploadRecordIdPairs = uploadables.map((row) =>
      convertToRecordIdPair(dataset, row, idMap, describer),
    );
    if (uploadRecordIdPairs.length > 0) {
      uploadings.set(dataset.object, uploadRecordIdPairs);
    }
    dataset.rows = waitings.map(({ row }) => row);
    blocked.push(
      ...waitings.map(({ id, blockingField, blockingId }) => ({
        object: dataset.object,
        origId: id,
        blockingField,
        blockingId,
      })),
    );
  }
  if (uploadings.size > 0) {
    const { successes, failures } = await uploadRecords(
      conn,
      uploadings,
      idMap,
      describer,
    );
    const totalCount = uploadStatus.totalCount;
    // event notification;
    const newUploadStatus = {
      totalCount,
      successes: [...uploadStatus.successes, ...successes],
      failures: [...uploadStatus.failures, ...failures],
      blocked,
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
      describer,
      newUploadStatus,
      reportProgress,
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
  keyFields: string[],
  defaultMapping: RecordSpecifier | undefined,
  describer: Describer,
) {
  const idMap = new Map<string, string>();
  const { object, headers, rows } = dataset;
  let idIndex = -1;
  const keyIndexMap = new Map<string, number>();
  const keyFieldSet = new Set(keyFields);
  for (const [i, header] of headers.entries()) {
    if (keyFieldSet.has(header)) {
      keyIndexMap.set(header, i);
      continue;
    }
    const field = describer.findFieldDescription(object, header);
    if (field && field.type === 'id') {
      idIndex = i;
    }
  }
  if (idIndex < 0) {
    return idMap;
  }
  if (keyIndexMap.size > 0) {
    const keyMap = new Map<string, string>();
    const keyFieldConditionValues = new Map<string, Set<any>>();
    for (const row of rows) {
      const id = row[idIndex];
      if (id != null && id !== '') {
        const keyValueTpl = [];
        for (const keyField of keyFields) {
          const keyIndex = keyIndexMap.get(keyField);
          const keyFieldValue = keyIndex != null ? row[keyIndex] : null;
          const keyFieldValues =
            keyFieldConditionValues.get(keyField) ?? new Set<any>();
          keyFieldValues.add(keyFieldValue ?? null);
          keyFieldConditionValues.set(keyField, keyFieldValues);
          keyValueTpl.push(keyFieldValue ?? '');
        }
        const keyValue = keyValueTpl.join('\t').trim();
        keyMap.set(keyValue, id);
      }
    }
    const condition = Array.from(keyFieldConditionValues.keys()).reduce(
      (cond, keyField) => {
        const fieldValues = keyFieldConditionValues.get(keyField);
        return fieldValues
          ? { ...cond, [keyField]: Array.from(fieldValues) }
          : cond;
      },
      {},
    );
    const records: SFRecord[] =
      keyMap.size === 0
        ? []
        : await conn.sobject(object).find(condition, ['Id', ...keyFields]);
    const newKeyMap = new Map<string, string>();
    for (const record of records) {
      const keyValue = keyFields
        .map((keyField) => record[keyField])
        .join('\t')
        .trim();
      if (keyValue != null) {
        newKeyMap.set(keyValue, record.Id as string);
      }
    }
    for (const keyValue of keyMap.keys()) {
      const id = keyMap.get(keyValue);
      const newId = newKeyMap.get(keyValue);
      if (id != null && newId != null) {
        idMap.set(id, newId);
      }
    }
  }
  if (defaultMapping) {
    let defaultId: string | undefined = undefined;
    if (typeof defaultMapping === 'string') {
      defaultId = defaultMapping;
    } else {
      const { condition, orderby, offset } = defaultMapping;
      let soql = `SELECT Id FROM ${object}`;
      soql += condition ? ` WHERE ${condition}` : '';
      soql += orderby ? ` ORDER BY ${orderby}` : '';
      soql += ` LIMIT 1`;
      soql += offset ? ` OFFSET ${offset}` : '';
      const { records } = await conn.query<{ Id: string }>(soql);
      defaultId = records?.[0]?.Id;
    }
    if (defaultId) {
      for (const row of rows) {
        const id = row[idIndex];
        if (id && !idMap.has(id)) {
          idMap.set(id, defaultId);
        }
      }
    }
  }
  return idMap;
}

/**
 *
 */
async function getAllExistingIdMap(
  conn: Connection,
  datasets: LoadDataset[],
  mappingPolicies: RecordMappingPolicy[],
  describer: Describer,
) {
  const datasetMap = new Map(
    datasets.map((dataset) => [dataset.object, dataset]),
  );
  const idMap = (
    await Promise.all(
      mappingPolicies.map((policy) => {
        const {
          object,
          keyField,
          keyFields: keyFields_,
          defaultMapping,
        } = policy;
        const dataset = datasetMap.get(object);
        if (!dataset) {
          throw new Error(`Input is not found for mapping object: ${object}`);
        }
        const keyFields = keyFields_
          ? typeof keyFields_ === 'string'
            ? keyFields_.split(/\s*,\s*/)
            : keyFields_
          : keyField
          ? [keyField]
          : [];
        return getExistingIdMap(
          conn,
          dataset,
          keyFields,
          defaultMapping,
          describer,
        );
      }),
    )
  ).reduce(
    (idMap, ids) => new Map([...idMap, ...ids]),
    new Map<string, string>(),
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
  reportProgress: (progress: UploadProgress) => void,
  options: UploadOptions,
) {
  const totalCount = calcTotalUploadCount(datasets);
  const targetIds = new Set<string>();
  const objects = datasets.map(({ object }) => object);
  const describer = await describeSObjects(conn, objects, options);
  const idMap = await getAllExistingIdMap(
    conn,
    datasets,
    mappingPolicies,
    describer,
  );
  const uploadStatus = {
    totalCount,
    successes: [],
    failures: [],
    blocked: [],
  };
  return uploadDatasets(
    conn,
    datasets,
    targetIds,
    idMap,
    describer,
    uploadStatus,
    reportProgress,
  );
}

async function parseCSVInputs(inputs: UploadInput[], options: Object) {
  return Promise.all(
    inputs.map(async (input) => {
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
            },
          );
        },
      );
      return { object, headers, rows };
    }),
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
  options: UploadOptions = {},
) {
  const datasets = await parseCSVInputs(inputs, options);
  return upload(conn, datasets, mappingPolicies, reportUpload, options);
}
