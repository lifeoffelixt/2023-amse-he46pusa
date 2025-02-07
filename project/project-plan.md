# Project Plan

## Summary

<!-- Describe your data science project in max. 5 sentences. -->
Analysis of weather events on German highways and accidents in 2018-19. 

## Rationale

<!-- Outline the impact of the analysis, e.g. which pains it solves. -->
It analyses whether highway segments that are particularly exposed to extreme weather events result in more car crashes than usual.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: Highway Weather Data
* Metadata URL: https://mobilithek.info/offers/-3534538293975156153
* Data URL: https://www.mcloud.de/downloads/mcloud/96EA9CD1-0695-4461-90B1-BC6F6B0E0729/Resultat_HotSpot_Analyse_neu.csv
* Data Type: CSV

Weather events on specific routes were studied using reanalysis data from all of Germany from Dec. 1, 2017-Nov. 30, 2019. The weather values of 3160 points with 1 km distance were read from the data and averaged or summed up, depending on the parameter. The values were normalized and the highest was given the value 100, the lowest the value 0.

### Datasource2: CrashData 
* Metadata URL: https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/ 
* Data URL:
* 2017: https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte2017_EPSG25832_CSV.zip
* 2018: https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte2018_EPSG25832_CSV.zip
* 2019: https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte2019_EPSG25832_CSV.zip
* Data Type: CSV/Zip

Road traffic accident data of 2017 to 2019 of Germany.