import { DescribeSObjectResult } from "jsforce";

export type DescribeSObjectResultMap = Record<string, DescribeSObjectResult>;

export type UploadInput = {
  object: string;
  csvData: string;
};

export type UploadStatus = {
  totalCount: number;
  successes: Array<{
    object: string;
    origId: string;
    newId: string;
  }>;
  failures: Array<{
    object: string;
    origId: string;
    errors: Array<{
      message: string;
    }>;
  }>;
};

export type UploadResult = UploadStatus;

export type UploadProgress = {
  totalCount: number;
  successCount: number;
  failureCount: number;
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
