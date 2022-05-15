from Db_Operation.PredictionConfig import DBOperation
from Application_logger.Logger import App_Logger
from Data_Transformation_predict.prediction_transformation import  DataTransformation
from Prediction_Raw_files_validation.Prediction_Data_validation import Prediction_Data_validation
class pred_validation:
    def __init__(self,path):
        self.raw_data = Prediction_Data_validation(path)
        self.dataTransform = DataTransformation()
        self.dBOperation = DBOperation()
        self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer = App_Logger()

    def prediction_validation(self):
        try:
            self.log_writer.log(self.file_object, 'Start of Validation on files for prediction!!')
            # extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.fetch_value_schema()
            # getting the regex defined to validate filename
            regex = self.raw_data.FileNameRegex()
            # validating filename of prediction files
            self.raw_data.validateFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            # validating column length in the file
            self.raw_data.ValidateColumnLength(noofcolumns)
            # validating if any column has all values missing
            #self.raw_data.validateMissingValuesInWholeColumn()
            #self.log_writer.log(self.file_object, "Raw Data Validation Complete!!")

            #self.log_writer.log(self.file_object, ("Starting Data Transforamtion!!"))
            # replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.TransformPredictionCategoricalData()

            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dBOperation.createTableDb('Prediction', column_names)

            # insert csv files in the table
            self.dBOperation.insertIntoTableGoodData('Prediction')

            # Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDataPredictionFolder()

            # Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchivebad()

            # export data in table to csvfile
            self.dBOperation.selectingDatafromtableintocsv('Prediction')

        except Exception as e:            raise e
