from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import GridSearchCV
from sklearn.metrics  import r2_score,accuracy_score
from sklearn.model_selection import RandomizedSearchCV
import numpy as np
class Model_Finder:
    """
    used to find the model with best accuracy and AUC score.
    """
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.Rd=RandomForestRegressor()
        self.gb = GradientBoostingRegressor()

    def get_best_params_for_rdf(self,train_x,train_y):
        """

        Parameters
        ----------
        train_x
        train_y

        Returns the model with best parameters
        -------

        """
        self.logger_object.log(self.file_object, 'Entered the get_best_params_for_svm method of the Model_Finder class')
        try:
            # initializing with different combination of parameters
            n_estimators = [5, 20, 50, 100]  # number of trees in the random forest
            max_depth = [int(x) for x in
            np.linspace(10, 120, num=12)]  # maximum number of levels allowed in each decision tree
            min_samples_split = [2, 6, 10]  # minimum sample number to split a node
            min_samples_leaf = [1, 3, 4]  # minimum sample number that can be stored in a leaf node
            bootstrap = [True, False]  # method used to sample data points



            self.param_grid = {"n_estimators": n_estimators,
                               "max_depth": max_depth,
                               "min_samples_split":min_samples_split,
                               "min_samples_leaf": min_samples_leaf,
                               "bootstrap":bootstrap
                               }

            # Creating an object of the Grid Search class
            self.grid = RandomizedSearchCV(estimator=self.Rd,param_distributions = self.param_grid, cv=5,random_state=35, verbose=2, n_jobs=-1)

            # finding the best parameters
            self.grid.fit(train_x, train_y)
            # self.grid.best_estimator_
            # self.grid.best_params_

            # extracting the best parameters
            self.n_estimators = self.grid.best_params_['n_estimators']
            self.max_depth = self.grid.best_params_['max_depth']
            self.min_samples_split = self.grid.best_params_['min_samples_split']
            self.min_samples_leaf = self.grid.best_params_['min_samples_leaf']
            self.bootstrap = self.grid.best_params_['bootstrap']


            # creating a new model with the best parameters
            self.randomforest = RandomForestRegressor(n_estimators=self.n_estimators, max_depth=self.max_depth,
                                                      min_samples_split=self.min_samples_split,min_samples_leaf=self.min_samples_leaf,bootstrap=self.bootstrap)
            # training the mew model

            self.randomforest.fit(train_x, train_y)

            self.logger_object.log(self.file_object,
                                   'Random Foresr best params: ' + str(
                                       self.grid.best_params_) + '. Exited the get_best_params_for_rdf method of the Model_Finder class')

            return self.grid
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in get_best_params_for_rdf method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'Random forest training  failed. Exited the get_best_params_for_rdf method of the Model_Finder class')
            raise Exception()


    def get_best_params_for_gb(self,train_x,train_y):
        """

        Parameters
        ----------
        train_x
        train_y

        Returns the best peremeter for gradient boosting algorithm
        -------

        """

        self.logger_object.log(self.file_object,
                               'Entered the get_best_params_for_gb method of the Model_Finder class')
        try:
            # initializing with different combination of parameters
            self.param_grid_xb = {
                "n_estimators": [100, 130],
                "min_samples_leaf": range(9, 10, 1),
                "max_depth": range(8, 10, 1),
                "max_leaf_nodes":range(3,9,1)

            }
            # Creating an object of the Grid Search class
            self.grid= GridSearchCV(estimator=self.gb, param_grid = self.param_grid_xb, cv = 2,verbose=3, n_jobs=-1)
            # finding the best parameters
            self.grid.fit(train_x, train_y)

            # extracting the best parameters
            self.n_estimators = self.grid.best_params_['n_estimators']
            self.max_depth = self.grid.best_params_['max_depth']
            self.min_samples_leaf = self.grid.best_params_['min_samples_leaf']
            self.max_leaf_nodes = self.grid.best_params_['max_leaf_nodes']

            # creating a new model with the best parameters
            self.xb = GradientBoostingRegressor(n_estimators=self.n_estimators, max_depth=self.max_depth,min_samples_leaf= self.min_samples_leaf,max_leaf_nodes= self.max_leaf_nodes )
            # training the mew model
            self.xb.fit(train_x, train_y)
            self.logger_object.log(self.file_object,
                                   'gradient boosting: ' + str(
                                       self.grid.best_params_) + '. Exited the get_best_params_for_gb method of the Model_Finder class')
            return self.xb
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in get_best_params_for_gb method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'XB Parameter tuning  failed. Exited the get_best_params_for_gb method of the Model_Finder class')
            raise Exception()

    def get_best_model(self,train_x,train_y,test_x,test_y):
        """

        Parameters
        ----------
        train_x
        train_y
        test_x
        test_y

        Returns the model that has highest accuracy score.
        -------

        """
        self.logger_object.log(self.file_object,
                               'Entered the get_best_model method of the Model_Finder class')
        # create best model for gradient boosting
        try:
            self.gb = self.get_best_params_for_gb(train_x, train_y)
            self.prediction_gb = self.gb.predict(test_x)  # Predictions for the gradient boosting

            if len(test_y.unique()) == 1:  # if there is only one label in y, then r2_score returns error. We will use accuracy in that case
                self.gb_score = accuracy_score(test_y, self.prediction_gb)
                self.logger_object.log(self.file_object, 'Accuracy for GBoostingRegressor:' + str(self.gb_score))  #
            else:
                self.gb_score = r2_score(test_y, self.prediction_gb)  # r2 for GBoosting
                self.logger_object.log(self.file_object, 'r2_score for gradient boosting:' + str(self.gb_score))  # Log AUC

            # create best model for Random Forest
            self.rd = self.get_best_params_for_rdf(train_x, train_y)
            self.prediction_rf = self.rd.predict(test_x)  # prediction using the RF Algorithm

            if len(test_y.unique()) == 1:  # if there is only one label in y, then r2_socre returns error. We will use accuracy in that case
                self.rd_score = accuracy_score(test_y, self.prediction_rf)
                self.logger_object.log(self.file_object, 'Accuracy for Random Forest:' + str(self.rd_score))
            else:
                self.rd_score = r2_score(test_y, self.prediction_rf)  # r2 for Random Forest
                self.logger_object.log(self.file_object, 'R2 for Random Forest:' + str(self.rd_score))

            # comparing the two models
            if (self.rd_score < self.gb_score):
                return 'gradientBoosting', self.xb
            else:
                return 'Random forest', self.randomforest

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in get_best_model method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'Model Selection Failed. Exited the get_best_model method of the Model_Finder class')
            raise Exception()
