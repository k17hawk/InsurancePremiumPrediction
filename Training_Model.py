from sklearn.model_selection import train_test_split

from Application_logger.Logger import App_Logger
from ObtainedDataFromCsvTraining.data_loader import Data_Getter
from Data_preprocessing import preprocessing,clustering
from best_model_finder import tuner
from file_operations import file_methods


class trainModel:
    def __init__(self):
        self.log_writer = App_Logger()
        self.file_object = open("Training_Logs/ModelTraining.txt",'a+')

    def trainingModel(self):
        self.log_writer.log(self.file_object,'File training started!')
        try:
            #Getting the data from source
            getdata = Data_Getter(self.file_object,self.log_writer)
            obtained_data = getdata.get_data()

            """applying the data preprocessing"""
            preprocessor = preprocessing.Preprocessor(self.file_object,self.log_writer)
            #encoding the categorical columns
            data = preprocessor.encode_categorical_columns(obtained_data)
            # create separate features and labels
            X, Y = preprocessor.seperate_label_features(data, label_column_name='expenses')
            """ Applying the clustering approach"""

            kmeans = clustering.KMeansClustering(self.file_object, self.log_writer)  # object initialization.
            number_of_clusters = kmeans.elbow_plot(X)  # using the elbow plot to find the number of optimum clusters

            # Divide the data into clusters
            X = kmeans.create_clusters(X, number_of_clusters)

            # create a new column in the dataset consisting of the corresponding cluster assignments.
            X['Labels'] = Y

            # getting the unique clusters from our dataset
            list_of_clusters = X['Cluster'].unique()

            """parsing all the clusters and looking for the best ML algorithm to fit on individual cluster"""

            for i in list_of_clusters:
                cluster_data = X[X['Cluster'] == i]  # filter the data for one cluster

                # Prepare the feature and Label columns
                cluster_features = cluster_data.drop(['Labels', 'Cluster'], axis=1)
                cluster_label = cluster_data['Labels']

                # splitting the data into training and test set for each cluster one by one
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1 / 3,
                                                                    random_state=355)
                # Proceeding with more data pre-processing steps
                x_train = preprocessor.scale_numerical_columns(x_train)
                x_test = preprocessor.scale_numerical_columns(x_test)

                model_finder = tuner.Model_Finder(self.file_object, self.log_writer)  # object initialization

                # getting the best model for each of the clusters
                best_model_name, best_model = model_finder.get_best_model(x_train, y_train, x_test, y_test)

                # saving the best model to the directory.
                file_op = file_methods.File_Operation(self.file_object, self.log_writer)
                save_model = file_op.save_model(best_model, best_model_name + str(i))

            # logging the successful Training
            self.log_writer.log(self.file_object, 'Successful End of Training')
            self.file_object.close()

        except Exception as e:
            # logging the unsuccessful Training
            self.log_writer.log(self.file_object, 'Unsuccessful End of Training')
            self.file_object.close()
            raise Exception


