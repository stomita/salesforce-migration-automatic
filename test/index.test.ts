import path from 'path';
import fs from 'fs-extra';
import { AutoMigrator, DumpQuery } from '../src';
import { Connection } from 'jsforce';
import { getConnection } from './util/getConnection';

let conn: Connection;

beforeAll(async () => {
  conn = await getConnection();
  conn.bulk.pollInterval = 50000;
  conn.bulk.pollTimeout = 300000;
});

/**
 *
 */
const ACCOUNT_IDS = ['0012800000k4FHkAAM'];
const CONTACT_IDS = ['0032v00003J3g3PAAR', '0032v00003VNQLtAAP'];
const USER_IDS = ['00528000002J6BkAAK', '0052v00000g3SEkAAM'];

/**
 *
 */
describe('AutoMigrator', () => {
  jest.setTimeout(300000);

  it('should upload empty data', async () => {
    const am = new AutoMigrator(conn);
    const {
      totalCount,
      successes,
      failures,
      blocked,
      idMap,
    } = await am.loadCSVData([]);
    expect(totalCount).toBe(0);
    expect(successes).toBeDefined();
    expect(successes.length).toBe(0);
    expect(failures).toBeDefined();
    expect(failures.length).toBe(0);
    expect(blocked).toBeDefined();
    expect(blocked.length).toBe(0);
    expect(idMap).toBeDefined();
    expect(idMap.size).toBe(0);
  });

  it('should block record upload when dependent id record is not given', async () => {
    const am = new AutoMigrator(conn);
    const {
      totalCount,
      successes,
      failures,
      blocked,
      idMap,
    } = await am.loadCSVData([
      {
        object: 'Account',
        csvData: `
Id,Name,OwnerId
${ACCOUNT_IDS[0]},Account 01,${USER_IDS[0]}
        `.trim(),
      },
      {
        object: 'User',
        csvData: 'Id,Name',
      },
    ]);
    expect(totalCount).toBe(1);
    expect(successes).toBeDefined();
    expect(successes.length).toBe(0);
    expect(failures).toBeDefined();
    expect(failures.length).toBe(0);
    expect(blocked).toBeDefined();
    expect(blocked.length).toBe(1);
    expect(blocked[0].object).toBe('Account');
    expect(blocked[0].origId).toBe(ACCOUNT_IDS[0]);
    expect(blocked[0].blockingField).toBe('OwnerId');
    expect(blocked[0].blockingId).toBe(USER_IDS[0]);
    expect(idMap).toBeDefined();
    expect(idMap.size).toBe(0);
  });

  it('should block record upload when dependent id record fails', async () => {
    const am = new AutoMigrator(conn);
    const {
      totalCount,
      successes,
      failures,
      blocked,
      idMap,
    } = await am.loadCSVData([
      {
        // account with no name field (will fail to load)
        object: 'Account',
        csvData: `
Id,Name,Type
${ACCOUNT_IDS[0]},,Partner
        `.trim(),
      },
      {
        object: 'Contact',
        csvData: `
Id,FirstName,LastName,AccountId
${CONTACT_IDS[0]},Sarah,Connor,${ACCOUNT_IDS[0]}
        `.trim(),
      },
    ]);
    expect(totalCount).toBe(2);
    expect(successes).toBeDefined();
    expect(successes.length).toBe(0);
    expect(failures).toBeDefined();
    expect(failures.length).toBe(1);
    expect(failures[0].object).toBe('Account');
    expect(failures[0].origId).toBe(ACCOUNT_IDS[0]);
    expect(blocked).toBeDefined();
    expect(blocked.length).toBe(1);
    expect(blocked[0].object).toBe('Contact');
    expect(blocked[0].origId).toBe(CONTACT_IDS[0]);
    expect(blocked[0].blockingField).toBe('AccountId');
    expect(blocked[0].blockingId).toBe(ACCOUNT_IDS[0]);
    expect(idMap).toBeDefined();
    expect(idMap.size).toBe(0);
  });

  it('should upload data from csv', async () => {
    const accCnt = await conn.sobject('Account').count();
    const oppCnt = await conn.sobject('Opportunity').count();
    const userCnt = await conn.sobject('User').count();
    const am = new AutoMigrator(conn);
    const dataDir = path.join(__dirname, 'fixtures', 'csv');
    const filenames = await fs.readdir(dataDir);
    const inputs = await Promise.all(
      filenames.map(async (filename) => {
        const object = filename.split('.')[0];
        const csvData = await fs.readFile(path.join(dataDir, filename), 'utf8');
        return { object, csvData };
      }),
    );
    const mappingPolicies = [
      {
        object: 'User',
        keyFields: ['FederationIdentifier'],
      },
    ];
    am.on('loadProgress', ({ totalCount, successCount, failureCount }) => {
      console.log(
        'total: ',
        totalCount,
        'successes: ',
        successCount,
        'failures: ',
        failureCount,
      );
    });
    const { successes, failures, blocked } = await am.loadCSVData(
      inputs,
      mappingPolicies,
    );
    expect(successes).toBeDefined();
    expect(successes.length).toBeGreaterThan(0);
    expect(failures).toBeDefined();
    failures.forEach((failure) => console.log(failure));
    expect(failures.length).toBe(0);
    expect(blocked).toBeDefined();
    expect(blocked.length).toBe(0);
    const newAccCnt = await conn.sobject('Account').count();
    expect(newAccCnt).toBeGreaterThan(accCnt);
    const newOppCnt = await conn.sobject('Opportunity').count();
    expect(newOppCnt).toBeGreaterThan(oppCnt);
    const newUserCnt = await conn.sobject('User').count();
    expect(newUserCnt).toBe(userCnt);
  });

  it('should download data as csv', async () => {
    const am = new AutoMigrator(conn);
    const queries: DumpQuery[] = [
      {
        object: 'Account',
        target: 'query',
        condition: 'CreatedDate = TODAY',
        orderby: 'Name ASC',
        scope: 'Everything',
        limit: 1000,
      },
      { object: 'Contact', target: 'related' },
      { object: 'Opportunity', target: 'related' },
      { object: 'Case', target: 'related' },
      { object: 'User', target: 'related' },
    ];
    am.on('dumpProgress', ({ fetchedCount, fetchedCountPerObject }) => {
      console.log('fetched: ', fetchedCount, fetchedCountPerObject);
    });
    const csvs = await am.dumpAsCSVData(queries);
    expect(csvs).toBeDefined();
    expect(csvs.length).toBe(queries.length);
    for (const csv of csvs) {
      expect(typeof csv).toBe('string');
      expect(csv.trim().split(/\n/).length).toBeGreaterThan(1);
    }
  });
});

afterAll(async () => {
  for (let i = 0; i < 3; i++) {
    await Promise.all(
      ['Task', 'Lead', 'Case', 'Opportunity', 'Contact', 'Account'].map(
        async (sobject) => await conn.sobject(sobject).find({}, 'Id').destroy(),
      ),
    );
  }
});
