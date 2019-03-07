import EventEmitter from "events";
import { Connection } from "jsforce";

type SObjectFieldDescription = {
  name: string;
  type: string;
  label: string;
  createable: boolean;
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

type UploadResult = {
  successes: Array<[string, string]>;
  failures: Array<[string, any]>;
};

/*
 * Import other org data, exported from salesforce.com (via DataLoader) This
 * class automatically resolves inter-record dependencies.
 */
export class SerializedUploader extends EventEmitter {
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

  async describe(table: string) {
    return new Promise<SObjectDescription>((resolve, reject) => {
      this._conn.describe$(table, (err: Error, ret: SObjectDescription) => {
        if (err) {
          reject(err);
        } else {
          resolve(ret);
        }
      });
    });
  }

  async getFieldDef(table: string, fname: string) {
    const { fields } = await this.describe(table);
    const fnameUpper = fname.toUpperCase();
    return fields.find(f => f.name.toUpperCase() === fnameUpper);
  }

  async getType(table: string, fname: string) {
    var f = await this.getFieldDef(table, fname);
    return f ? f.type : undefined;
  }

  setUploadingTarget(ids: string[]) {
    for (const id of ids) {
      this._target[id] = true;
    }
  }

  isTargetedUpload() {
    return Object.keys(this._target).length > 0;
  }

  async filterUploadableRecords(table: string) {
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

  async convertToRecordIdPair(table: string, row: string[]) {
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

  async uploadRecords(uploadings: Record<string, RecordIdPair[]>) {
    for (const [table, recordIdPairs] of Object.entries(uploadings)) {
      const records = recordIdPairs.map(({ record }) => record);
      const rets = await this._conn.sobject(table).create(records);
      if (Array.isArray(rets)) {
        rets.forEach((ret, i) => {
          const origId = recordIdPairs[i].id;
          if (ret.success) {
            // register map info of oldid -> newid
            this._idMap[origId] = ret.id;
            this._successes.push([origId, ret.id]);
          } else {
            this._failures.push([origId, ret]);
          }
        });
      }
    }
  }

  async upload(): Promise<UploadResult> {
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
      const successes = this._successes;
      const failures = this._failures;
      this.emit("UploadProgress", { successes, failures });
      // recursive call
      return this.upload();
    } else {
      const successes = this._successes;
      const failures = this._failures;
      this.emit("Complete");
      return { successes, failures };
    }
  }
}
