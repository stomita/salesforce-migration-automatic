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

type DumpTarget = QueryTarget | RelatedTarget;

export type DumpQuery = {
  object: string;
  fields?: string[];
} & DumpTarget;
