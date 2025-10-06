import fs from 'fs-extra';
import * as path from 'path';
import pl from 'nodejs-polars';
import { Reader } from './reader.js';
import { toSinger, SingerHeaderMap } from './singer.js';

export interface ExportOptions {
  keys?: string[];
  exportFormat?: 'csv' | 'json' | 'jsonl' | 'parquet' | 'singer';
  outputFilePrefix?: string;
  stringifyObjects?: boolean;
  reservedVariables?: Record<string, string>;
  allowObjects?: boolean;
  schema?: SingerHeaderMap;
}

export function toExport(
  data: pl.DataFrame,
  name: string,
  outputDir: string,
  options: ExportOptions = {}
): void {
  const {
    exportFormat = process.env.DEFAULT_EXPORT_FORMAT || 'singer',
    outputFilePrefix = process.env.OUTPUT_FILE_PREFIX,
    reservedVariables = {}
  } = options;

  // Handle unified output override
  const unifiedOutputKey = `HG_UNIFIED_OUTPUT_${name.toUpperCase()}`;
  let finalName = process.env[unifiedOutputKey] || name;

  if (outputFilePrefix) {
    const formatVariables = buildStringFormatVariables(reservedVariables);
    const formattedPrefix = formatStrSafely(outputFilePrefix, formatVariables);
    finalName = `${formattedPrefix}${finalName}`;
  }

  // Ensure output directory exists
  fs.ensureDirSync(outputDir);

  const outputPath = path.join(outputDir, finalName);

  switch (exportFormat) {
    case 'parquet':
      data.writeParquet(`${outputPath}.parquet`);
      break;
    case 'singer':
      // Get primary key from reader
      const reader = new Reader();
      const keys = options.keys || reader.getPk(name);
      // Export data as singer format
      toSinger(data, finalName, outputDir, {
        keys,
        allowObjects: options.allowObjects ?? true,
        schema: options.schema
      });
      break;
    case 'json':
      // Convert to JSON array format
      const jsonData = data.toRecords();
      fs.writeFileSync(`${outputPath}.json`, JSON.stringify(jsonData, null, 2));
      break;
    case 'jsonl':
      // Convert to JSONL format (one JSON object per line)
      const jsonlData = data.toRecords()
        .map(record => JSON.stringify(record))
        .join('\n');
      fs.writeFileSync(`${outputPath}.jsonl`, jsonlData);
      break;
    default: // csv
      // Stringify struct columns for CSV export
      let csvData = data;
      const schema = data.schema;

      for (const [colName, dataType] of Object.entries(schema)) {
        // Check if the column is a struct type
        if (dataType.variant === 'Struct') {
          // Convert struct column to JSON string
          csvData = csvData.withColumn(
            csvData.getColumn(colName).cast(pl.Utf8).alias(colName)
          );
        }
      }

      csvData.writeCSV(`${outputPath}.csv`);
      break;
  }
}

export function buildStringFormatVariables(
  defaultKwargs: Record<string, string> = {},
  useTenantMetadata: boolean = true,
  subtenantDelimiter: string = '_'
): Record<string, string> {
  const reservedKeys = Object.keys(defaultKwargs);
  const finalKwargs = { ...defaultKwargs };

  // Build tenant metadata variables
  if (useTenantMetadata) {
    const tenantMetadataPath = path.join(
      process.env.ROOT || '.',
      'snapshots',
      'tenant-config.json'
    );

    if (fs.existsSync(tenantMetadataPath)) {
      try {
        const tenantConfig = fs.readJsonSync(tenantMetadataPath);
        const tenantMetadata = tenantConfig?.hotglue_metadata?.metadata || {};

        // Add tenant metadata that doesn't conflict with reserved keys
        for (const [k, v] of Object.entries(tenantMetadata)) {
          if (!reservedKeys.includes(k)) {
            finalKwargs[k] = String(v);
          }
        }
      } catch (error) {
        console.warn(`Failed to read tenant metadata: ${error}`);
      }
    }
  }

  // Add environment variables
  const flowId = process.env.FLOW || '';
  const jobId = process.env.JOB_ID || '';
  const tap = process.env.TAP || '';
  const connector = process.env.CONNECTOR_ID || '';
  const tenantId = process.env.TENANT || '';
  const envId = process.env.ENV_ID || '';

  const splittedTenantId = tenantId.split(subtenantDelimiter);
  const rootTenantId = splittedTenantId[0] || '';
  const subTenantId = splittedTenantId[1] || '';

  Object.assign(finalKwargs, {
    tenant: tenantId,
    tenant_id: tenantId,
    root_tenant_id: rootTenantId,
    sub_tenant_id: subTenantId,
    env_id: envId,
    flow_id: flowId,
    job_id: jobId,
    tap,
    connector,
  });

  return finalKwargs;
}

export function formatStrSafely(
  strToFormat: string,
  formatVariables: Record<string, string>
): string {
  let strOutput = strToFormat;

  for (const [k, v] of Object.entries(formatVariables)) {
    if (v) {
      const regex = new RegExp(`\\{${k}\\}`, 'g');
      strOutput = strOutput.replace(regex, v);
    }
  }

  return strOutput;
}

export function getIndexSafely<T>(arr: T[], index: number): T | null {
  try {
    return arr[index] ?? null;
  } catch {
    return null;
  }
}
