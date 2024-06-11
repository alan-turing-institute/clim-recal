
This is a crude collection of scripts that picks out the key parts of the repo, required to prepare the CPM and HADs data prior to applying the debiasing methods.

It builds heavily on the work already done by Stuart and Griff.

# What is and isn't included

## Included
* CPM temporally resampled to a 365/366 day year.
* CPM reprojected to British National Grid.
* HADs data reprojected and aligned to match an output CPM file.

## Not included
* Cropping to the three cities.
* Any ergonomic or user-friendly features.

The scripts need a little tweaking before running. The steps are outlined below.


# Setup

* These scripts rely on the conda environment defined in the repo being activated.
* They do not rely on any other part of the `clim-recal` package as far as I am aware.


# Processing CPM data

This has to be done before the HADs data.

TODO:

- [ ] In `process_cpm_all.sh` update the `default_path` based on your system.
- [ ] In `process_cpm_all.sh` confirm whether to use the `parallel` command or just use a loop
- [ ] In `process_single_cpm_wrapper.sh` update the `file_new` variable with your choice of output directory.
- [ ] In `process_single_cpm_wrapper.sh` remove the `--dry-run` flag when your ready.

# Processing HADs data

This has to be done after the CPM data, as it requires a reference to a processed CPM file.

TODO:

- [ ] In `process_hads_all.sh` update the `default_path` based on your system.
- [ ] In `process_hads_all.sh` confirm whether to use the `parallel` command or just use a loop.
- [ ] In `process_single_hads_wrapper.sh` update the `file_new` variable with your choice of output directory.
- [ ] In `process_single_hads_wrapper.sh` update the `reference_file` variable with the path to the CPM file you want to use as a reference. (This will be opened multiple times, so it may be worth copying it to a local directory first).
- [ ] In `process_single_hads_wrapper.sh` remove the `--dry-run` flag when your ready.


# Example usage

```bash
python3 process_single_cpm_file.py --dry-run --cpm-file ../tests/data/cpm/tasmax_rcp85_land-cpm_uk_2.2km_05_day_19811201-19821130_cpm_example.nc --output-dir ../output
```

```bash
python3 process_hads_single_file.py --hads-file ../tests/data/hads/tasmax_hadukgrid_uk_1km_day_19940101-19940131_hads_example.nc --output-dir ../output --reference-file ../output/tasmax_rcp85_land-cpm_uk_2.2km_05_day_19811201-19821130_cpm_example.nc
```
