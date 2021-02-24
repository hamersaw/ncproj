# ncproj
## OVERVIEW
Reproject gridded netCDF data using shapefiles

## USAGE
    # compile
    ./gradlew build

    # compute grid index
    java -cp "app/build/libs/*" org.sustain.etl.ncproj.Main index sustaindb region_county

    # dump data

## BENCHMARKS
    hamersaw@nightcrawler:~/development/ncproj$ time java -cp "app/build/libs/*" org.sustain.etl.ncproj.Main index sustaindb region_county ~/downloads/sustain-data/macav2/2005/macav2metdata_tasmax_NorESM1-M_r1i1p1_histstorical_2005_2005_CONUS_daily.nc > ~/downloads/county-index.txt

    real	8m19.179s
    user	9m50.758s
    sys     0m24.437s

## TODO
- everything
