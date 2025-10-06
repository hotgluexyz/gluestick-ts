import * as gs from '../../dist/index.js';
import pl from 'nodejs-polars';

const ROOT_DIR = './examples/example-nested-csv';
const OUTPUT_DIR = `${ROOT_DIR}/etl-output`;

async function runEtl(): Promise<void> {
  console.log('Starting ETL process with nested data...');

  try {
    // Create a DataFrame with a struct column (nested data)
    const df = pl.DataFrame({
      id: [1, 2, 3],
      name: ['Alice', 'Bob', 'Charlie'],
      address: [
        { street: '123 Main St', city: 'NYC', zip: '10001' },
        { street: '456 Oak Ave', city: 'LA', zip: '90001' },
        { street: '789 Pine Rd', city: 'SF', zip: '94102' }
      ]
    });

    console.log('DataFrame with nested struct column:');
    console.log(df);
    console.log('\nSchema:', df.schema);

    // Attempt to export to CSV (this should fail with "CSV format does not support nested data")
    console.log('\nAttempting to export to CSV format...');
    gs.toExport(df, 'nested_data', OUTPUT_DIR, {
      exportFormat: 'csv'
    });

    console.log('Successfully exported (unexpected!)');

  } catch (error) {
    console.error('Failed to process nested data:', error);
    throw error;
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
