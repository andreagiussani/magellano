# Magellano

## Get Data

To get the dataset, please use the following GCS bucket:

```
gsutil -m cp -r "gs://gcp-public-data-arco-era5/raw/date-variable-single_level/2022/05" .
```

Make sure you have installed the GCP SDK.

## How to Use

Examples can be found in the `examples` directory. Please refer to `demo.html` for a general introduction to the tool.

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
