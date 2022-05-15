import os
import shutil
from os import listdir
import  re

import pandas

from Application_logger.Logger import App_Logger
import  json
from datetime import  datetime
class Prediction_Data_validation:
    def __init__(self,path):
        self.Batch_Directory = path
        self.schema_path = 'schema_prediction.json'
        self.logger = App_Logger()

    def fetch_value_schema(self):
        '''
        this method will fetch the necessary requirement from #prediction_schema.json
        to check the file contained is satisfied.
        '''
        try:
            with open(self.schema_path, 'r') as f:
                myfile = json.load(
                    f)  # since it is a mixed content file so using json.load if only string then json.loads
                f.close()
            fileSampleName = myfile['SampleFileName']
            LengthOfDateStampInFile = myfile['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = myfile['LengthOfTimeStampInFile']
            column_names = myfile['ColName']
            NumberofColumns = myfile['NumberofColumns']

            file = open("Prediction_Logs/ValuesFromSchemaValidation.txt", 'a+')
            message = "LengthOfDateStampInFile:: %s" % LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile + "\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log(file, message)
        except ValueError:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "ValueError:Value not found inside schema_prediction.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns

    def FileNameRegex(self):
        '''

        Returns the regex format to check the name of the file
        -------

        '''
        regex = "['insurance']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def CreateBadGoodFolderDirectory(self):
        '''

        create folder to store Good data and Bad data
        -------

        '''
        try:
            path = os.path.join("Prediction_Raw_files_Validated", 'Good_Raw')
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Prediction_Raw_files_Validated", 'Bad_Raw')
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as e:
            file = open("Prediction_Logs/GeneralError.txt", 'a+')
            self.logger.log(file, "Error while creating Directory %s:" % e)
            file.close()
            raise OSError

    def deleteExistingGoodDataPredictionFolder(self):
        '''

        ths method will delete the existing Good Data training folder
        -------

        '''
        try:
            path = "Prediction_Raw_files_Validated"
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file = open('Prediction_Logs/GeneralLog.txt', 'a+')
                self.logger.log(file, "Good raw directory has been deleted successfully")
                file.close()
        except OSError as s:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s" % s)
            file.close()
            raise OSError

    def deleteExistingBadDataPredictionFolder(self):
        '''

        ths method will delete the existing Bad Data training folder
        -------

        '''
        try:
            path = "Prediction_Raw_files_Validated"
            if os.path.isdir(path + 'Bad_raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open('Prediction_Logs/GeneralLog.txt', 'a+')
                self.logger.log(file, "Good raw directory has been deleted successfully")
                file.close()
        except OSError as s:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s" % s)
            file.close()
            raise OSError

    def moveBadFilesToArchivebad(self):
        '''
        this method will move the Bad Raw folder data to Archive
        and delete it from Bad_Raw,where client can verify data from
        ArchieveBad

        -------

        '''
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            bad_raw = 'Prediction_Raw_files_Validated/Bad_Raw/'
            if os.path.isdir(bad_raw):
                path = "PredictionArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'Prediction_Raw_files_Validated/BadData_' + str(date) + "_" + str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(bad_raw)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(bad_raw + f, dest)
                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "bad files moved to arcive")
                path = 'Prediction_Raw_files_Validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file, "Bad Raw Data Folder Deleted successfully!!")
                file.close()
        except Exception as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise e

    def validateFileNameRaw(self, regex, LengthOfDateStampInFile, LengthOfTimeStampInFile):
        '''
        this method will validate if our Training_Batch_files satisfied
        required condition or not
        Parameters
        ----------
        regex
        LengthOfDateStampInFile
        LengthOfTimeStampInFile


        -------

        '''
        # delete the existing good Raw and bad Raw data files
        self.deleteExistingBadDataPredictionFolder()
        self.deleteExistingGoodDataPredictionFolder()
        # create the new drectories
        self.CreateBadGoodFolderDirectory()
        myfiles = [f for f in listdir(self.Batch_Directory)]
        try:
            f = open('Prediction_Logs/nameValidation.txt', 'a+')
            for filename in myfiles:
                if (re.match(regex, filename)):
                    splitAtDot = (re.split('_', os.path.splitext(filename)[0]))
                    # splitAtDot = re.split('.csv',filename)
                    # splitAtDot = (re.split('_',splitAtDot[0]))
                    if len(splitAtDot[1]) == 9:  # LengthOfDateStampInFile
                        if len(splitAtDot[2]) == 6:  # LengthOfTimeStampInFile
                            shutil.copy("Prediction_Batch_Files/" + filename, "Prediction_Raw_files_Validated/Good_Raw")
                            self.logger.log(f, "Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("Prediction_Batch_Files/" + filename, "Prediction_Raw_files_Validated/Bad_Raw")
                            self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("Prediction_Batch_Files/" + filename, "Prediction_Raw_files_Validated/Bad_Raw")
                        self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("Prediction_Batch_Files/" + filename, "Prediction_Raw_files_Validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            f.close()
        except Exception as e:
            f = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e

    def deletePredictionFile(self):

        if os.path.exists('Prediction_Output_File/Predictions.csv'):
            os.remove('Prediction_Output_File/Predictions.csv')

    def ValidateColumnLength(self, NumberofColumns):
        '''

        Parameters
        ----------
        NumberofColumns

        Returns
        validation of total length present in csv or not
        -------

        '''
        try:
            f = open("Prediction_Logs/ColumnValidation.txt", 'a+')
            self.logger.log(f, "Column length validation begins")
            for file in listdir("Training_Raw_files_validated/Good_Raw/"):
                df = pandas.read_csv("Prediction_Raw_files_Validated/Good_Raw/" + file)
                if df.shape[1] == NumberofColumns:
                    pass
                else:
                    shutil.move("Prediction_Raw_files_Validated/Good_Raw/" + file, "Prediction_Raw_files_Validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()
