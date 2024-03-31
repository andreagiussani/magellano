# Magellano

## Get Data

To get the dataset, please use the following GCS bucket:

```
gsutil -m cp -r "gs://gcp-public-data-arco-era5/raw/date-variable-single_level/2022/05" .
```

Make sure you have installed the GCP SDK.

## How to Use

Examples can be found in the `examples` directory. Please refer to `demo.ipynb` for a general introduction to the tool.

To load all May 2022 data related to total_precipitations, and filter by the
H3 index `'8a1e80433107fff'`, one can run the following script:

```python
from pipeline.orchestrator import DataOrchestrator
from pipeline.loader import DataLoader

h3_indexes_df = DataLoader.compute_h3_index_from_file (
    nc_file_path="05/01/total_precipitation/surface.nc", 
    resolution=10
)

data_pipeline = DataOrchestrator(
    h3_indexes_df, 
    start_day=1, 
    end_day=31, 
    batch_size=5, 
    datetime_to_filter=None, 
    h3_index_filter='8a1e80433107fff', 
    filter_by_date=False, 
    process_data=False, 
    output_file = 'era5_may2022_tp_dataset_8a1e80433107fff.parquet'
)
data_pipeline.pipeline()
```
Please note that the H3 index `'8a1e80433107fff'` is the one closest to the following coordinates:
- Latitude: 41.75
- Longitude: 12.5

which can be identified as Italy.

## Unit Tests

To run unit tests:

```
python -m unittest discover
```

To perform coverage of the base code:

```
coverage run -m unittest discover
```

Then view the coverage report:

```
coverage report -m
```

## Remarks

- Not all classes have been tested. For a production perspective, a PR should be opened and reviewed.
- In general, all new contributions must be tested.
- No plotting utilities have been generated at this stage. It would be great to have a sort of visual validation of the generated data as well.
- A pipeline using Apache Beam can be generated to be more flexible. In such a case, an improvement of the classes can be done.
- A library can be created. In this way, we can keep track of changes in the code, using a CHANGELOG.md file. For this stage, this step has been omitted.
