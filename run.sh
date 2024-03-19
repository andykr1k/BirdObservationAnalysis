mvn clean package

# Part One - Done
bin/beast --master "local[*]" --class edu.ucr.cs.cs167.CS167_Project_Group25.DataPrep --conf spark.executor.memory=32g --conf spark.driver.memory=32g target/CS167_Project_Group25-1.0-SNAPSHOT.jar preprocess eBird_10k.csv.bz2

# Part Two - Done
bin/beast --master "local[*]" --class edu.ucr.cs.cs167.CS167_Project_Group25.SpatialAnalysis --conf spark.executor.memory=32g --conf spark.driver.memory=32g ./target/CS167_Project_Group25-1.0-SNAPSHOT.jar ./eBird_ZIP Mallard

# Part Three - Done
spark-submit --master "local[*]" --class edu.ucr.cs.cs167.CS167_Project_Group25.TemporalAnalysis ./target/CS167_Project_Group25-1.0-SNAPSHOT.jar part3 ./eBird_ZIP 02/21/2015 05/22/2015

# Part Four - Done
spark-submit --master "local[*]"  --class edu.ucr.cs.cs167.CS167_Project_Group25.Prediction target/CS167_Project_Group25-1.0-SNAPSHOT.jar ./eBird_ZIP