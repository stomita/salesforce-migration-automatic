import { execSync } from 'child_process';
import jsforce, { Connection } from 'jsforce';

/**
 *
 */
export async function getConnection(): Promise<Connection> {
  if (process.env.SFDX_USERNAME) {
    const infoJSON = execSync(
      `sfdx force:org:display -u ${process.env.SFDX_USERNAME} --json`,
    ).toString();
    const info = JSON.parse(infoJSON);
    const { accessToken, instanceUrl } = info.result;
    return new Connection({ instanceUrl, accessToken });
  } else {
    return (jsforce as any).registry.getConnection(process.env.SF_USERNAME);
  }
}
