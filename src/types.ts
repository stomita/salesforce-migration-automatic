import { DescribeSObjectResult } from "jsforce";

export type ArrayValue<T> = T extends Array<infer V> ? V : never;

export type DescribeFieldResult = ArrayValue<DescribeSObjectResult["fields"]>;

export type DescribeSObjectResultMap = Record<string, DescribeSObjectResult>;

export type LoadData = {
  headers: string[];
  rows: string[][];
};

export type RecordIdPair = {
  id: string;
  record: Record<string, any>;
};

export type UploadResult = {
  totalCount: number;
  successes: Array<[string, string]>;
  failures: Array<[string, any]>;
};

export type RelatedTarget = {
  target: "related";
};

export type QueryTarget = {
  target: "query";
  condition?: string;
  orderby?: string;
  limit?: number;
  offset?: number;
  scope?: string;
};

export type DumpTarget = QueryTarget | RelatedTarget;

export type DumpQuery = {
  object: string;
  fields?: string[];
} & DumpTarget;
