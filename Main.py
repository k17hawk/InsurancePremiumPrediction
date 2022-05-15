from wsgiref import simple_server
from flask import Flask, request, render_template, jsonify
from flask import Response
import os

from flask_cors import CORS, cross_origin
import flask_monitoringdashboard as dashboard

from PredictFromModel import prediction
from Training_Model import trainModel
from train_validation_insertion import train_validation
from PredictionValidationInsertion import pred_validation
from flask import send_file
from werkzeug.utils import secure_filename


app = Flask(__name__)
dashboard.bind(app)
CORS(app)

UPLOAD_FOLDER = 'static/uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

app.secret_key = "secret-key-of-insurancePremiumPrediction"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

ALLOWED_EXTENSIONS = set(['csv'])

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')

@app.route("/predict", methods=['POST'])
@cross_origin()
def predictRouteClient():
    try:
        if request.json is not None:
            path = request.json['filepath']
            pred_val = pred_validation(path)  # object initialization

            pred_val.prediction_validation()  # calling the prediction_validation function

            pred = prediction(path)  # object initialization

            # predicting for dataset present in database
            path = pred.predictionFromModel()
            return Response("Prediction File created at %s!!!" % path)

        elif request.form is not None:
            path = request.form['filepath']

            pred_val = pred_validation(path)  # object initialization

            pred_val.prediction_validation()  # calling the prediction_validation function

            pred = prediction(path)  # object initialization

            # predicting for dataset present in database
            path = pred.predictionFromModel()

            return Response(path)

    except ValueError:
        return Response("Error Occurred! %s" % ValueError)
    except KeyError:
        return Response("Error Occurred! %s" % KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" % e)

# @app.route('/download',methods=["GET","POST"])
# def downloadFile(): #In your case fname is your filename
#     try:
#        path = f'./Prediction_Output_File/Predictions.csv'
#        return send_file(path,mimetype='text/csv', attachment_filename='Predictions.csv', as_attachment=True)
#     except Exception as e:
#         return str(e)
#
#
# @app.route('/genexcel', methods=["GET", "POST"])
# def createExcel():
#     if request.method == 'POST':
#         data = request.json
#         print(data)
#         # process json data
#
#     file_path = '/Prediction_Output_File/Predictions.csv'
#     return send_file(file_path, attachment_filename='Predictions.csv', as_attachment=True)
@app.route("/train", methods=['POST'])
@cross_origin()
def trainRouteClient():

    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath']
            train_valObj = train_validation(path)  # object initialization

            train_valObj.train_validation()

            trainModelObj = trainModel()
            trainModelObj.trainingModel()

           

    except ValueError:

        return Response("Error Occurred! %s" % ValueError)

    except KeyError:

        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:

        return Response("Error Occurred! %s" % e)
    return Response("Training successfull!!")

port = int(os.getenv("PORT",5001))
if __name__ == "__main__":
    app.run(port=port,debug=True)
