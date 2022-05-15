from Application_logger.Logger import App_Logger
from Validation_Raw_Training.raw_validation import  RawValidation
from Data_Transformation_Training.Data_Tranformation import DataTransformation
from Db_Operation.ConfigDb import dbOperation
class train_validation:
    def __init__(self,path):
        self.raw_data = RawValidation(path)
        self.file_object = open("Training_Logs/Training_Main_Log.txt",'a+')
        self.logger = App_Logger()
        self.datatransform = DataTransformation()
        self.dBOperation = dbOperation()


    def train_validation(self):
        try:
            self.logger.log(self.file_object,'Start file validation for Training')
            #extracting the values of Schema_training from raw_validation
            LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,NumberofColumns = self.raw_data.fetch_value_schema()

            #obtaining the regex for file name validation
            regex = self.raw_data.FileNameRegex()

            #validating filename
            self.raw_data.validateFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)
            self.file_object.close()
            #validate column length
            self.raw_data.ValidateColumnLength(NumberofColumns)
            self.datatransform.TransformTrainingCategoricalData()

            self.dBOperation.CreateTableDb('Training', column_names)
            self.dBOperation.insertIntoTableGoodData('Training')
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.raw_data.moveBadFilesToArchivebad()
            self.dBOperation.selectingDatafromtableintocsv('Training')
            self.file_object.close()

        except Exception as e:
             raise e



