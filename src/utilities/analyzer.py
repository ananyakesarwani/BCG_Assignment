# import logging
from dataclasses import dataclass

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from . import utils


@dataclass
class USVehicleAccidentAnalysis:

    # Configurations
    config_file_path: str = "config.yaml"
    input_file_paths: dict = None
    header_name: dict = None
    output_file_path: dict = None

    # Dataframes
    charges = None
    damages = None
    endorse = None
    primary_person = None
    units = None
    restrict = None
    data = []

    def configure(self,spark):
        self.input_file_paths = utils.read_yaml(
            self.config_file_path).get("INPUT_FILENAME")
        self.header_name = utils.read_yaml(
            self.config_file_path).get("HEADER_NAMES")
        self.output_file_path = utils.read_yaml(
            self.config_file_path).get("OUTPUT_PATH")
        self.sparkInstance = spark

    def retreive_data(self, spark):

        self.charges = utils.load_csv_data_to_df(
            spark, self.input_file_paths.get("Charges"))
        self.damages = utils.load_csv_data_to_df(
            spark, self.input_file_paths.get("Damages"))
        self.endorse = utils.load_csv_data_to_df(
            spark, self.input_file_paths.get("Endorse"))
        self.primary_person = utils.load_csv_data_to_df(
            spark, self.input_file_paths.get("Primary_Person"))
        self.units = utils.load_csv_data_to_df(
            spark, self.input_file_paths.get("Units"))
        self.restrict = utils.load_csv_data_to_df(
            spark, self.input_file_paths.get("Restrict"))

    def male_accidents_count(self):
        """
        Find the number of crashes (accidents) in which number of persons killed are male
        """
        schema = StructType([
                    StructField(self.header_name.get(1), IntegerType(), True),
                ])

        result = self.primary_person.filter((self.primary_person.PRSN_INJRY_SEV_ID == "KILLED")&
                (self.primary_person.PRSN_GNDR_ID == "MALE")).distinct().count()
        resultDF = self.sparkInstance.createDataFrame([(result,)], schema)
        utils.write_output(resultDF, 1)
        print("1. Fatal male accidents: ")
        resultDF.show(truncate = False)

    def booked_two_wheeler_accidents(self):
        """
        How many two wheelers are booked for crashes
        """
        schema = StructType([
                    StructField(self.header_name.get(2), IntegerType(), True),
                ])

        result = self.units.select("CRASH_ID").filter(self.units.VEH_BODY_STYL_ID.contains("MOTORCYCLE")). \
            distinct().count()
        resultDF = self.sparkInstance.createDataFrame([(result,)], schema)

        utils.write_output(resultDF, 2)

        print("2. Number of two wheelers booked for crashed: ")
        resultDF.show(truncate = False)

    def max_female_accidents_state(self):
        """
        Which state has highest number of accidents in which females are involved
        """
        schema = StructType([
                    StructField(self.header_name.get(3), StringType(), True),
                ])

        result = self.primary_person.select("*").filter(col("PRSN_GNDR_ID") == "FEMALE"). \
            groupby("DRVR_LIC_STATE_ID").count().orderBy(col("count").desc()).first().DRVR_LIC_STATE_ID
        resultDF = self.sparkInstance.createDataFrame([(result,)], schema)

        utils.write_output(resultDF, 3)

        print("3. States with the highest number of female accidents: ")
        resultDF.show(truncate = False)

    def largest_injury_vehicles(self):
        """
        Finds Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        """
        resultDf = self.primary_person.join(self.units, on=["CRASH_ID"], how='inner'). \
            filter((self.units.VEH_MAKE_ID != "NA") &((self.primary_person.PRSN_INJRY_SEV_ID.contains("INJURY"))|
            (self.primary_person.PRSN_INJRY_SEV_ID == "KILLED"))). \
            groupby(self.units.VEH_MAKE_ID).count().withColumnRenamed("count", "total").orderBy("total")
        
        top15DF = resultDf.select("VEH_MAKE_ID").orderBy(resultDf.total.desc()).limit(15)
        top5to15DF = top15DF.subtract(top15DF.limit(5))

        utils.write_output(top5to15DF, 4)
        
        print("4. Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death: ")
        top5to15DF.show(truncate=False)

    def top_ethnic_userGroup_crash_for_every_body_style(self):
        """
        Finds and show top ethnic user group of each unique body style that was involved in crashes
        """
        windowFunction = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        resultDF = self.units.join(self.primary_person, on=['CRASH_ID'], how='inner'). \
            filter(~self.units.VEH_BODY_STYL_ID.isin(["NA", "UNKNOWN", "NOT REPORTED",
                                                         "OTHER  (EXPLAIN IN NARRATIVE)"])). \
            filter(~self.primary_person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"])). \
            groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count(). \
            withColumn("row", row_number().over(windowFunction)).filter(col("row") == 1).drop("row", "count")
    
        utils.write_output(resultDF, 5)
        print("5. For all the body styles involved in crashes, mentioned the top ethnic user group of each unique body style :")
        resultDF.show(truncate=False)

    def top_5_zip_codes_with_alcohols_as_factor_for_car_crash(self):
        """
        Finds top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash
        """
        resultDF = self.units.join(self.primary_person, on=['CRASH_ID'], how='inner'). \
            dropna(subset=["DRVR_ZIP"]). \
            filter(
                (
                    col("VEH_BODY_STYL_ID").contains("CAR") 
                ) & ( 
                    col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | 
                    col("CONTRIB_FACTR_2_ID").contains("ALCOHOL"))
                ). \
            groupby("DRVR_ZIP").count().orderBy(col("count").desc()).limit(5)
        zipCodeResults = resultDF.select("DRVR_ZIP")

        utils.write_output(zipCodeResults, 6)
        print("6. Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash :")
        zipCodeResults.show(truncate=False)
    
    def get_crash_ids_with_given_criterias(self):
        """
        Counts Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
        and car avails Insurance.
        """

        joinedDF = self.damages.join(self.units, on=['CRASH_ID'], how='inner')

        noDamagesDF = joinedDF. \
            filter(col("DAMAGED_PROPERTY").isin(["NO DAMAGE","NONE"]))

        damageLevelAbove4DF = joinedDF. \
            filter((
                (col("VEH_DMAG_SCL_1_ID") > "DAMAGED 4") &
                (~col("VEH_DMAG_SCL_1_ID").isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
            ) | (
                (col("VEH_DMAG_SCL_2_ID") > "DAMAGED 4") &
                (~col("VEH_DMAG_SCL_2_ID").isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
            ))
        
        insuranceCarsDF = joinedDF. \
            filter((
                col("VEH_BODY_STYL_ID").contains("CAR")
                ) & (
                col("FIN_RESP_TYPE_ID") != "NA")
            )
        
        allConditionsDF = insuranceCarsDF.join(damageLevelAbove4DF, on = ['CRASH_ID'], how ='inner'). \
            join(noDamagesDF, on = ['CRASH_ID'], how ='inner')

        resultDF = self.sparkInstance.createDataFrame([("No Damage In Distinct Crash", noDamagesDF.select("CRASH_ID").distinct().count()), 
                    ("Damage Level Above 4", damageLevelAbove4DF.select("CRASH_ID").distinct().count()), 
                    ("Cars with Insurance", insuranceCarsDF.select("CRASH_ID").distinct().count()),
                    ("Satisfying All Above Conditions", allConditionsDF.select("CRASH_ID").distinct().count())], 
                    ["GIVEN_FACTORS", "COUNT_OF_MATCHING_CRITERIA"])
    
        utils.write_output(resultDF, 7)

        print("7. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance :")
        resultDF.show(truncate = False)

    def top_5_vehicle_brand(self):
        """
        Determines the Top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed
        Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
        offences
        """
        top_25_state_list = [row[0] for row in self.units.filter(col("VEH_LIC_STATE_ID").cast("int").isNull()).
            groupby("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(25).collect()]
        top_10_used_vehicle_colors = [row[0] for row in self.units.filter(self.units.VEH_COLOR_ID != "NA").
            groupby("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10).collect()]

        resultDF = self.charges.join(self.primary_person, on=['CRASH_ID'], how='inner'). \
            join(self.units, on=['CRASH_ID'], how='inner'). \
            filter(self.charges.CHARGE.contains("SPEED")). \
            filter(self.primary_person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])). \
            filter(self.units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors)). \
            filter(self.units.VEH_LIC_STATE_ID.isin(top_25_state_list)). \
            groupby("VEH_MAKE_ID").count(). \
            orderBy(col("count").desc()).limit(5)

        utils.write_output(resultDF.select("VEH_MAKE_ID"), 8)
        
        print("7. Determines the Top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences,\n has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states \n with highest number of offences : ")
        resultDF.select("VEH_MAKE_ID").show(truncate = False)



    def __call__(self):
        print("*******************-Analysis Started-*******************")
        print('\n')
        self.male_accidents_count()
        print('\n')
        self.booked_two_wheeler_accidents()
        print('\n')
        self.max_female_accidents_state()
        print('\n')
        self.largest_injury_vehicles()
        print('\n')
        self.top_ethnic_userGroup_crash_for_every_body_style()
        print('\n')
        self.top_5_zip_codes_with_alcohols_as_factor_for_car_crash()
        print('\n')
        self.get_crash_ids_with_given_criterias()
        print('\n')
        self.top_5_vehicle_brand()
        print('\n')
        print("*******************-Analysis End-*******************")
