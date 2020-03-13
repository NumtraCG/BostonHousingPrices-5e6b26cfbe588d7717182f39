import traceback
import sys
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e6b26cfbe588d7717182f3a','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
	BostonHousingPrices_DBFS = DBFSConnector.DBFSConnector.fetch([], {}, "5e6b26cfbe588d7717182f3a", spark, "{'url': '/Demo/BostonTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi0ef076722999cf4cd8859e9aafdb7b76', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e6b26cfbe588d7717182f3a','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e6b26cfbe588d7717182f3a','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e6b26cfbe588d7717182f3b','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
	BostonHousingPrices_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e6b26cfbe588d7717182f3a"],{"5e6b26cfbe588d7717182f3a": BostonHousingPrices_DBFS}, "5e6b26cfbe588d7717182f3b", spark,json.dumps( {"FE": [{"transformationsData": {}, "feature": "CRIM", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "506", "mean": "3.61", "stddev": "8.6", "min": "0.00632", "max": "88.9762", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "ZN", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "506", "mean": "11.36", "stddev": "23.32", "min": "0.0", "max": "100.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "INDUS", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "506", "mean": "11.14", "stddev": "6.86", "min": "0.46", "max": "27.74", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "CHAS", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "506", "mean": "0.07", "stddev": "0.25", "min": "0.0", "max": "1.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "NOX", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "506", "mean": "0.55", "stddev": "0.12", "min": "0.385", "max": "0.871", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "RM", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "506", "mean": "6.28", "stddev": "0.7", "min": "3.561", "max": "8.78", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "AGE", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "506", "mean": "68.57", "stddev": "28.15", "min": "2.9", "max": "100.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "DIS", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "506", "mean": "3.8", "stddev": "2.11", "min": "1.1296", "max": "12.1265", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "RAD", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "506", "mean": "9.55", "stddev": "8.71", "min": "1.0", "max": "24.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "TAX", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "506", "mean": "408.24", "stddev": "168.54", "min": "187.0", "max": "711.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "PTRATIO", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "506", "mean": "18.46", "stddev": "2.16", "min": "12.6", "max": "22.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "B", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "506", "mean": "356.67", "stddev": "91.29", "min": "0.32", "max": "396.9", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "LSTAT", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "506", "mean": "12.65", "stddev": "7.14", "min": "1.73", "max": "37.97", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "MEDV", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "506", "mean": "22.53", "stddev": "9.2", "min": "5.0", "max": "50.0", "missing": "0"}, "transformation": ""}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e6b26cfbe588d7717182f3b','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e6b26cfbe588d7717182f3b','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e6b26cfbe588d7717182f3c','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
	BostonHousingPrices_AutoML = tpot_execution.Tpot_execution.run(["5e6b26cfbe588d7717182f3b"],{"5e6b26cfbe588d7717182f3b": BostonHousingPrices_AutoFE}, "5e6b26cfbe588d7717182f3c", spark,json.dumps( {"model_type": "regression", "label": "MEDV", "features": ["CRIM", "ZN", "INDUS", "CHAS", "NOX", "RM", "AGE", "DIS", "RAD", "TAX", "PTRATIO", "B", "LSTAT"], "percentage": "100", "executionTime": "5", "sampling": "0", "sampling_value": "", "run_id": "be5ff2abed9e4cceaa3c9bce4785194a", "model_id": "5e6b5571be588d7717183197", "ProjectName": "ML Sample Problems", "PipelineName": "BostonHousingPrices", "pipelineId": "5e6b26cfbe588d7717182f39", "userid": "5df78f4be2f2eff24740bbd7", "runid": "", "url_ResultView": "http://13.68.212.36:3200", "experiment_id": "480623611921769"}))

	PipelineNotification.PipelineNotification().completed_notification('5e6b26cfbe588d7717182f3c','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e6b26cfbe588d7717182f3c','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)

