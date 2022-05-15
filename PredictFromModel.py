import pandas
import os
from Application_logger.Logger import App_Logger
from Data_preprocessing.preprocessing import Preprocessor
from Prediction_Raw_files_validation.Prediction_Data_validation import Prediction_Data_validation
from ObtainedDataFromCsvTraining.predictionDataLoader import Data_Getter
from file_operations import file_methods


class prediction:
    def __init__(self,path):
        self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer = App_Logger()
        self.pred_data_val = Prediction_Data_validation(path)

    def predictionFromModel(self):

        try:
            self.pred_data_val.deletePredictionFile()  # deletes the existing prediction file from last run!
            self.log_writer.log(self.file_object, 'Start of Prediction')
            data_getter = Data_Getter(self.file_object, self.log_writer)
            data = data_getter.get_data()

            preprocessor = Preprocessor(self.file_object, self.log_writer)
            # encode categorical data
            data = preprocessor.encode_categorical_columns(data)
            data = preprocessor.scale_numerical_columns(data)
            file_loader=file_methods.File_Operation(self.file_object,self.log_writer)
            kmeans=file_loader.load_model('KMeans')

            ##Code changed

            clusters=kmeans.predict(data)
            data['clusters']=clusters
            clusters=data['clusters'].unique()
            predictions=[]
            for i in clusters:
                cluster_data= data[data['clusters']==i]
                cluster_data = cluster_data.drop(['clusters'],axis=1)
                model_name = file_loader.find_correct_model_file(i)
                model = file_loader.load_model(model_name)
                result = (model.predict(cluster_data))
                for res in result:
                     predictions.append(res)

            final= pandas.DataFrame(list(zip(predictions)),columns=['Predictions'])
            outputpath="Prediction_Output_File/Predictions.csv"
            final.to_csv(outputpath,header=True,mode='a+') #appends result to prediction file
            self.log_writer.log(self.file_object,'End of Prediction')
        except Exception as ex:
            self.log_writer.log(self.file_object, 'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex
        return  outputpath

