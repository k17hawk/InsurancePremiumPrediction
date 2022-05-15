import pandas as pd
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import RandomOverSampler

class Preprocessor:
    """
    this clasa will perform EDA on data
    """
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object

    def remove_unwanted_spaces(self,data):
        """

        Parameters :
        ----------
        data

        Returns the data where spaces are removed
        -------

        """
        self.logger_object.log(self.file_object,"Entered the remove unwanred spaced steps in Prepocessing.py")
        self.data = data

        try:
            self.df_without_spaces = self.data.apply(lambda x:x.str.strip() if x.dtype=="O" else  x) #strip the spaces from object dtypes
            self.logger_object.log(self.file_object,
                               'Unwanted spaces removal Successful.Exited the remove_unwanted_spaces method of the Preprocessor class')
            return self.df_without_spaces
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in remove_unwanted_spaces method of the Preprocessor class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'unwanted space removal Unsuccessful. Exited the remove_unwanted_spaces method of the Preprocessor class')
            raise Exception()


    def seperate_label_features(self,data,label_column_name):
        """

        Parameters
        ----------
        data
        label_column_name

        Returns thhe seperate labels and features dataframes
        -------

        """
        self.logger_object.log(self.file_object,"Entered the seperate_label_features in preprocessing ")
        try:
            self.X = data.drop(labels = label_column_name,axis= 1) #drop the label column and others as X feature column
            self.Y = data[label_column_name] #only label column
            self.logger_object.log(self.file_object,
                               'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class')
            return self.X, self.Y

        except Exception as e:
                  self.logger_object.log(self.file_object,
                           'Exception occured in separate_label_feature method of the Preprocessor class. Exception message:  ' + str(
                               e))
                  self.logger_object.log(self.file_object,
                           'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
                  raise Exception()


    def scale_numerical_columns(self,data):
        """

        Parameters
        ----------
        data

        Returns the scaled numerical features
        -------

        """

        self.logger_object.log(self.file_object,
                               'Entered the scale_numerical_columns method of the Preprocessor class')

        self.data=data
        self.num_df = self.data[['age','bmi','children']]

        try:

            self.scaler = StandardScaler()
            self.scaled_data = self.scaler.fit_transform(self.num_df)
            self.scaled_num_df = pd.DataFrame(data=self.scaled_data, columns=self.num_df.columns,index=self.data.index)
            self.data.drop(columns=self.scaled_num_df.columns, inplace=True)
            self.data = pd.concat([self.scaled_num_df, self.data], axis=1)

            self.logger_object.log(self.file_object, 'scaling for numerical values successful. Exited the scale_numerical_columns method of the Preprocessor class')
            return self.data

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in scale_numerical_columns method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'scaling for numerical columns Failed. Exited the scale_numerical_columns method of the Preprocessor class')
            raise Exception()


    def encode_categorical_columns(self,data):
        """

        Parameters
        ----------
        data

        Returns the encoded categorical columns
        -------

        """
        self.logger_object.log(self.file_object,"Entered the Encoded categorical columns steps in preprocessing")

        self.data = data
        try:
            self.cat_df = self.data.select_dtypes(include=['object']).copy()
            self.cat_df['sex'] = self.cat_df['sex'].map({'female': 0, 'male': 1})
            self.cat_df['smoker'] = self.cat_df['smoker'].map({'yes': 0, 'no': 1})
            self.cat_df['region'] = self.cat_df['region'].map({'southwest':0,'northwest':1,'northeast':2,'southeast':3})
            # self.labels_ordered = data.groupby(['region'])['expenses'].mean().sort_values().index
            # self.labels_ordered = {k: i for i, k in enumerate(self.labels_ordered, 0)}
            # self.cat_df['region'] = data['region'].map(self.labels_ordered)
            self.data.drop(columns=self.data.select_dtypes(include=['object']).columns, inplace=True)
            self.data = pd.concat([self.cat_df, self.data], axis=1)
            self.logger_object.log(self.file_object,
                                   'encoding for categorical values successful. Exited the encode_categorical_columns method of the Preprocessor class')
            return self.data

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in encode_categorical_columns method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'encoding for categorical columns Failed. Exited the encode_categorical_columns method of the Preprocessor class')
            raise Exception()

    def handle_imbalanced_dataset(self,x,y):
        """

        Parameters
        ----------
        x
        y

        Returns balanced features of target column
        -------

        """

        self.logger_object.log(self.file_object,'Entered the handle imbalanced dataset in preprocessing.py')
        try:
            self.rdsample = RandomOverSampler()
            self.x_sampled, self.y_sampled = self.rdsample.fit_sample(x, y)
            self.logger_object.log(self.file_object,
                                   'dataset balancing successful. Exited the handle_imbalanced_dataset method of the Preprocessor class')
            return self.x_sampled, self.y_sampled
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in handle_imbalanced_dataset method of the Preprocessor class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'dataset balancing Failed. Exited the handle_imbalanced_dataset method of the Preprocessor class')
            raise Exception()

