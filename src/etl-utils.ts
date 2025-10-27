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
      const schema = data.schema;
      const structColumns: string[] = [];

      // Identify struct columns by checking the constructor name
      for (const [colName, dataType] of Object.entries(schema)) {
        if (dataType && typeof dataType === 'object' && dataType.constructor?.name === 'Struct') {
          structColumns.push(colName);
        }
      }

      // If there are struct columns, we need to stringify them properly
      if (structColumns.length > 0) {
        // Convert struct columns to JSON strings using struct.jsonEncode()
        let csvData = data;
        for (const col of structColumns) {
          csvData = csvData.withColumn(
            pl.col(col).struct.jsonEncode().alias(col)
          );
        }
        csvData.writeCSV(`${outputPath}.csv`);
      } else {
        // No struct columns, write directly
        data.writeCSV(`${outputPath}.csv`);
      }
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

/**
 * Converts a DataFrame column to localized datetime format
 * @param df - Input Polars DataFrame
 * @param columnName - Name of the column to convert
 * @returns A Polars Series containing the localized datetime values
 */
export function localizeDatetime(df: pl.DataFrame, columnName: string): pl.Series {
  let col = df.getColumn(columnName);

  // Convert column to datetime if it’s not already
  if (col.dtype !== pl.DataType.Datetime("ms", "UTC") && col.dtype !== pl.DataType.Datetime("ms")) {
    try {
      col = col.cast(pl.DataType.Datetime("ms"));
    } catch (e) {
      // Try parsing from string if cast fails
      col = pl.Series(columnName, col.toArray().map(v => (v ? new Date(v) : null)));
      col = col.cast(pl.DataType.Datetime("ms"));
    }
  }

  // Ensure timezone is UTC
  // Polars handles timezone as a parameter on Datetime type
  if (col.dtype !== pl.DataType.Datetime("ms", "UTC")) {
    col = col.cast(pl.DataType.Datetime("ms", "UTC"));
  }

  return col;
}

/**
 * Reads snapshot data for a stream from either Parquet or CSV files
 * @param stream - Name of the stream to read snapshots for
 * @param snapshotDir - Directory containing the snapshot files
 * @param options - Optional CSV read options
 * @returns A Polars DataFrame containing the snapshot data, or null if no snapshot exists
 */
export function readSnapshots(
  stream: string,
  snapshotDir: string,
  options: pl.CsvReadOptions = {}
): pl.DataFrame | null {
  const parquetPath = path.join(snapshotDir, `${stream}.snapshot.parquet`);
  const csvPath = path.join(snapshotDir, `${stream}.snapshot.csv`);

  if (fs.existsSync(parquetPath)) {
    // Read Parquet file
    const df = pl.readParquet(parquetPath);
    return df;
  } else if (fs.existsSync(csvPath)) {
    // Read CSV file with provided options
    const df = pl.readCSV(csvPath, options);
    return df;
  } else {
    // No snapshot found
    return null;
  }
}

/**
 * Creates or updates snapshot records for a data stream
 * @param streamData - Polars DataFrame containing the stream data to snapshot
 * @param stream - Name of the stream
 * @param snapshotDir - Directory to store the snapshot files
 * @param pk - Primary key used for deduplication (default: "id")
 * @param justNew - If true, return only new data instead of the full snapshot
 * @param useCsv - Whether to write snapshots as CSV instead of Parquet
 * @param coerceTypes - If true, coerce merged data columns to match streamData dtypes
 * @param localizeDatetimeTypes - If true, convert datetime columns to UTC
 * @param overwrite - If true, overwrite snapshot entirely instead of merging
 * @param options - Additional options (for CSV reading)
 * @returns Updated Polars DataFrame (snapshot)
 * @throws Error if snapshot fails while trying to convert field during type coercion
 */
export function snapshotRecords(
  streamData: pl.DataFrame | null,
  stream: string,
  snapshotDir: string,
  pk: string = "id",
  justNew: boolean = false,
  useCsv: boolean = false,
  coerceTypes: boolean = false,
  localizeDatetimeTypes: boolean = false,
  overwrite: boolean = false,
  options: pl.CsvReadOptions = {}
): Promise<pl.DataFrame | null> {
  // Load existing snapshot if available
  const snapshot = readSnapshots(stream, snapshotDir, options);

  // If snapshot exists and we're not overwriting
  if (!overwrite && streamData && snapshot) {
    let updatedSnapshot = snapshot.clone();

    // Localize datetime columns to UTC if requested
    if (localizeDatetimeTypes) {
      const schema = updatedSnapshot.schema;
      for (const [colName, dtype] of Object.entries(schema)) {
        if (dtype === pl.DataType.Datetime("ms")) {
          const localized = localizeDatetime(updatedSnapshot, colName);
          updatedSnapshot = updatedSnapshot.withColumn(localized);
        }
      }
    }

    // Merge snapshot + new data
    let mergedData = pl.concat([updatedSnapshot, streamData]);

    // Coerce types if needed
    if (coerceTypes && !streamData.isEmpty() && !updatedSnapshot.isEmpty()) {
      const schema = streamData.schema;
      try {
        for (const [colName, dtype] of Object.entries(schema)) {
          let newType = dtype;

          // Map pandas-like coercions
          if (dtype === pl.DataType.Boolean) {
            newType = pl.DataType.Boolean;
          } else if (
            [pl.DataType.Int64, pl.DataType.Int32].includes(dtype as any)
          ) {
            newType = pl.DataType.Int64;
          }

          mergedData = mergedData.withColumn(
            mergedData.getColumn(colName).cast(newType)
          );
        }
      } catch (err: any) {
        throw new Error(
          `Snapshot failed while trying to convert field during type coercion: ${err.message}`
        );
      }
    }

    // Drop duplicates based on PK (keep last)
    mergedData = mergedData.unique({ maintainOrder: false, subset: [pk], keep: "last" });

    // Write snapshot to file
    const filePath = path.join(
      snapshotDir,
      `${stream}.snapshot.${useCsv ? "csv" : "parquet"}`
    );
    if (useCsv) {
      mergedData.writeCSV(filePath);
    } else {
      mergedData.writeParquet(filePath);
    }

    return justNew ? streamData : mergedData;
  }

  // If there’s no existing snapshot, save new data
  if (streamData) {
    const filePath = path.join(
      snapshotDir,
      `${stream}.snapshot.${useCsv ? "csv" : "parquet"}`
    );
    if (useCsv) {
      streamData.writeCSV(filePath);
    } else {
      streamData.writeParquet(filePath);
    }
    return streamData;
  }

  // If just_new or overwrite is true but no data
  if (justNew || overwrite) {
    return streamData;
  } else {
    return snapshot;
  }
}
