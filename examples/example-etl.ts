import * as gs from '../dist/index.js';
import pl from 'nodejs-polars';

// Standard directory for hotglue
const ROOT_DIR = './examples';
const INPUT_DIR = `${ROOT_DIR}/sync-output`;
const OUTPUT_DIR = `${ROOT_DIR}/etl-output`;

// Get tenant id via environment variables
const TENANT_ID = process.env.USER_ID || process.env.TENANT || 'default';

async function runEtl(): Promise<void> {
  console.log('Starting ETL process...');

  // Create Reader instance
  const input = new gs.Reader(INPUT_DIR, ROOT_DIR);

  // Get all available streams
  const availableStreams = input.keys();
  console.log('Available streams:', availableStreams);

  // Process each stream
  for (const key of availableStreams) {
    try {
      console.log(`Processing stream: ${key}`);

      // Read the raw input data with catalog types
      const inputDf = input.get(key, { catalogTypes: true });

      if (!inputDf) {
        console.log(`Failed to read data for ${key}, skipping`);
        continue;
      } else {
        console.log(inputDf)
      }

      // Inject the tenant id as a column in the output
      const outputDf = inputDf.withColumn(
        pl.lit(TENANT_ID).alias('tenant')
      );

      // Export the data to the format defined by environment variables
      gs.toExport(outputDf, key, OUTPUT_DIR);

      console.log(`Successfully processed ${key}`);

    } catch (error) {
      console.error(`Failed to process data for ${key}:`, error);
      continue;
    }
  }

  console.log('ETL process completed');
}

// Run the ETL if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  runEtl().catch(error => {
    console.error('ETL process failed:', error);
    process.exit(1);
  });
}

export { runEtl };
