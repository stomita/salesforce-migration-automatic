import { DescribeSObjectResult, Record as SFRecord } from 'jsforce';

export type DescribeSObjectResultMap = Map<string, DescribeSObjectResult>;

export type UploadInput = {
  object: string;
  csvData: string;
};

export type RecordMappingPolicy = {
  object: string;
  keyField?: string;
  keyFields?: string | string[];
  defaultMapping?: RecordSpecifier;
};

export type RecordSpecifier =
  | string
  | {
      condition?: string;
      orderby?: string;
      offset?: number;
      scope?: string;
    };

export type UploadStatus = {
  totalCount: number;
  successes: Array<{
    object: string;
    record: SFRecord;
    origId: string;
    newId: string;
  }>;
  failures: Array<{
    object: string;
    record: SFRecord;
    origId: string;
    errors: Array<{
      message: string;
    }>;
  }>;
  blocked: Array<{
    object: string;
    origId: string;
    blockingField: string | undefined;
    blockingId: string | undefined;
  }>;
  idMap: Map<string, string>;
};

export type UploadResult = UploadStatus;

export type UploadProgress = {
  totalCount: number;
  successCount: number;
  failureCount: number;
};

export type UploadOptions = {
  csvParseOptions?: Object;
  defaultNamespace?: string;
  idMap?: Map<string, string>;
};

export type RelatedTarget = {
  target: 'related';
};

export type QueryTarget = {
  target: 'query';
  condition?: string;
  orderby?: string;
  limit?: number;
  offset?: number;
  scope?: string;
};

export type DumpTarget = QueryTarget | RelatedTarget;

export type DumpQuery = {
  object: string;
  fields?: string | string[];
  ignoreFields?: string | string[];
} & DumpTarget;

export type DumpOptions = {
  defaultNamespace?: string;
  maxFetchSize?: number;
  idMap?: Map<string, string>;
};

export type DumpProgress = {
  fetchedCount: number;
  fetchedCountPerObject: { [name: string]: number };
};
