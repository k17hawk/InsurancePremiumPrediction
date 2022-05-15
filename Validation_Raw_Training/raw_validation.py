'''
here we are going to valdidate the raw trining data ,and also keep them in goodRawFolder if no error
else send them to BadRawFolder
'''
from datetime import datetime
import re
import shutil
from importlib.resources import path
import json
import os
from os import listdir
import pandas


from Application_logger.Logger import App_Logger



class RawValidation:

    def __init__(self,path):
        self.Batch_file = path
        self.schema_file = 'training_schema.json'
        self.logger = App_Logger()
        
    def fetch_value_schema(self):
        '''
        this method will fetch the necessary requirement from #training_schema.json 
        to check the file contained is satisfied.
        '''    
        try:
            with open(self.schema_file,'r')as f:
                myfile = json.load(f)  #since it is a mixed content file so using json.load if only string then json.loads
                f.close()
            fileSampleName = myfile['SampleFileName']
            LengthOfDateStampInFile = myfile['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = myfile['LengthOfTimeStampInFile']
            column_names = myfile['ColName']
            NumberofColumns = myfile['NumberofColumns'] 
            
            file = open("Training_Logs/ValuesFromSchemaValidation.txt",'a+')
            message = "LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"   
            self.logger.log(file,message)    
        except ValueError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
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
            path =  os.path.join("Training_Raw_files_validated",'Good_Raw')
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Training_Raw_files_validated",'Bad_Raw')
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as e:
            file = open("Training_Logs/GeneralError.txt",'a+')
            self.logger.log(file,"Error while creating Directory %s:" % e)
            file.close()
            raise  OSError


    def deleteExistingGoodDataTrainingFolder(self):
        '''

        ths method will delete the existing Good Data training folder
        -------

        '''
        try:
            path = "Training_Raw_files_validated"
            if os.path.isdir(path +'Good_Raw/'):
                shutil.rmtree(path +'Good_Raw/')
                file = open('Training_Logs/GeneralLog.txt','a+')
                self.logger.log(file,"Good raw directory has been deleted successfully")
                file.close()
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s" % s)
            file.close()
            raise OSError

    def deleteExistingBadDataTrainingFolder(self):
        '''

        ths method will delete the existing Bad Data training folder
        -------

        '''
        try:
            path = "Training_Raw_files_validated"
            if os.path.isdir(path + 'Bad_raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open('Training_Logs/GeneralLog.txt', 'a+')
                self.logger.log(file, "Good raw directory has been deleted successfully")
                file.close()
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
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
            bad_raw = 'Training_Raw_files_validated/Bad_Raw/'
            if os.path.isdir(bad_raw):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'Training_Raw_files_validated/BadData_'+str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(bad_raw)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(bad_raw + f,dest)
                file = open("Training_Logs/GeneralLog.txt",'a+')
                self.logger.log(file,"bad files moved to arcive")
                path = 'Training_Raw_files_validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file, "Bad Raw Data Folder Deleted successfully!!")
                file.close()
        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise e



    def validateFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
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
        #delete the existing good Raw and bad Raw data files
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        #create the new drectories
        self.CreateBadGoodFolderDirectory()
        myfiles =  [f for f in listdir(self.Batch_file)]
        try:
            f = open('Training_Logs/nameValidation.txt','a+')
            for filename in myfiles:
                if (re.match(regex,filename)):
                    splitAtDot = (re.split('_', os.path.splitext(filename)[0]))
                    #splitAtDot = re.split('.csv',filename)
                    #splitAtDot = (re.split('_',splitAtDot[0]))
                    if len(splitAtDot[1]) == 9:  #LengthOfDateStampInFile
                        if len(splitAtDot[2])==6: #LengthOfTimeStampInFile
                                shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Good_Raw")
                                self.logger.log(f, "Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            f.close()
        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e

    def ValidateColumnLength(self,NumberofColumns):
        '''

        Parameters
        ----------
        NumberofColumns

        Returns
        validation of total length present in csv or not
        -------

        '''
        try:
            f = open("Training_Logs/ColumnValidation.txt",'a+')
            self.logger.log(f,"Column length validation begins")
            for file in listdir("Training_Raw_files_validated/Good_Raw/"):
                df = pandas.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                if df.shape[1]==NumberofColumns:
                    pass
                else:
                    shutil.move("Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()
