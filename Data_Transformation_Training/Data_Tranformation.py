from os import listdir

import pandas

from Application_logger.Logger import App_Logger


class DataTransformation:
    """
    Methods this class used for transforming the Good  Raw Training data
    before storing in database
    """
    def __init__(self):
        self.goodDataPath = "Training_Raw_files_validated/Good_Raw/"
        self.logger = App_Logger()

    def TransformTrainingCategoricalData(self):
        """
        Methods will use substrings in the first column to keep only "integer"
        data for ease up the loading and will be removed during training
        Returns
        -------

        """
        log_file = open("Training_Logs/dataTranformLog.txt",'a+')
        try:
            onlyfiles = [f for f in listdir(self.goodDataPath)]
            for file in onlyfiles:
                data = pandas.read_csv(self.goodDataPath + "/" + file)
                # list of columns with string  variables
                columns = ["sex","region","smoker"]

                for col in columns:
                    data[col] = data[col].apply(lambda x: "'" + str(x) + "'")

                data.to_csv(self.goodDataPath + "/" + file, index=None, header=True)
                self.logger.log(log_file, " %s: Quotes added successfully!!" % file)

        except Exception as e:
            self.logger.log(log_file, "Data Transformation failed because:: %s" % e)

            log_file.close()
        log_file.close()
