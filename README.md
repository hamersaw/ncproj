# ncproj
## OVERVIEW
Reproject gridded netCDF data using shapefiles

## USAGE
    # compile
    ./gradlew build

    # compute grid index
    java -cp "app/build/libs/*" org.sustain.etl.ncproj.Main index sustaindb region_county ~/downloads/sustain-data/macav2/2005/macav2metdata_tasmax_NorESM1-M_r1i1p1_histstorical_2005_2005_CONUS_daily.nc

    # dump data
    java -cp "app/build/libs/*" org.sustain.etl.ncproj.Main dump ~/downloads/county-index.txt ~/downloads/sustain-data/macav2/2005/* > ~/downloads/macav2-2005-ncproj5.csv

## BENCHMARKS
    hamersaw@nightcrawler:~/development/ncproj$ time java -cp "app/build/libs/*" org.sustain.etl.ncproj.Main index sustaindb region_county ~/downloads/sustain-data/macav2/2005/macav2metdata_tasmax_NorESM1-M_r1i1p1_histstorical_2005_2005_CONUS_daily.nc > ~/downloads/county-index.txt

    real	8m19.179s
    user	9m50.758s
    sys     0m24.437s

    hamersaw@nightcrawler:~/development/ncproj$ time java -cp "app/build/libs/*" org.sustain.etl.ncproj.Main dump ~/downloads/county-index.txt ~/downloads/sustain-data/macav2/2005/* > ~/downloads/macav2-2005-ncproj5.csv

    real	1m46.131s
    user	4m27.804s
    sys	    0m13.379s
## TODO
- everything
