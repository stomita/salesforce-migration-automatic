import parse from 'csv-parse';
import stringify from 'csv-stringify';

/**
 *
 */
export async function parseCSV<T = string[]>(csvData: string, options: Object) {
  return new Promise<T[]>((resolve, reject) => {
    parse(csvData, options, (err: Error | undefined, rets: T[]) => {
      if (err) {
        reject(err);
      } else {
        resolve(rets);
      }
    });
  });
}

/**
 *
 */
export async function stringifyCSV<T = {}>(
  records: T[],
  columns: Array<{ key: string; header: string }>,
) {
  return new Promise<string>((resolve, reject) => {
    stringify(records, { columns, header: true }, (err, ret) => {
      if (err) {
        reject(err);
      } else {
        resolve(ret);
      }
    });
  });
}
