import * as gs from '../../dist/index.js';

const ROOT_DIR = './examples/example-parquet';
const INPUT_DIR = `${ROOT_DIR}/sync-output`;
const OUTPUT_DIR = `${ROOT_DIR}/etl-output`;

async function runEtl(): Promise<void> {
  console.log('Starting ETL process...');

  const input = new gs.Reader(INPUT_DIR, ROOT_DIR);

  console.log('Available streams:', input.keys());

  // Try to read the parquet file
  const parquetStream = 'campaign_performance';

  try {
    console.log(`\nTesting ${parquetStream} (parquet file):`);
    const df = input.get(parquetStream);

    if (df) {
      console.log('✓ Successfully read parquet file');
      console.log('Shape:', df.shape);
      console.log('Columns:', df.columns);
      console.log('First few rows:');
      console.log(df.head(3).toString());

      // Test with catalog types
      console.log('\nTesting with catalog types:');
      const dfWithTypes = input.get(parquetStream, { catalogTypes: true });
      if (dfWithTypes) {
        console.log('✓ Successfully read with catalog types');
        console.log('Shape:', dfWithTypes.shape);
      }

      // Test getPk
      const pk = input.getPk(parquetStream);
      console.log('Primary keys:', pk);

      // Test metadata
      const metadata = input.getMetadata(parquetStream);
      console.log('Metadata:', metadata);


      // Export the data to the format defined by environment variables
      gs.toExport(dfWithTypes, parquetStream, OUTPUT_DIR);

    } else {
      console.log('✗ Failed to read parquet file');
    }
  } catch (error) {
    console.error('Error reading parquet file:', error);
  }
}

// Run the ETL if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  runEtl().catch(error => {
    console.error('ETL process failed:', error);
    process.exit(1);
  });
}

export { runEtl };
