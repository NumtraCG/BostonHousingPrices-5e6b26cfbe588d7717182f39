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
	BostonHousingPrices_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e6b26cfbe588d7717182f3a"],{"5e6b26cfbe588d7717182f3a": BostonHousingPrices_DBFS}, "5e6b26cfbe588d7717182f3b", spark,json.dumps( {"FE": [{"feature": "CRIM", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "358", "mean": "3.21", "stddev": "8.17", "min": "0.00632", "max": "88.9762", "missing": "0"}, "transformation": ""}, {"feature": "ZN", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "358", "mean": "11.3", "stddev": "22.81", "min": "0.0", "max": "100.0", "missing": "0"}, "transformation": ""}, {"feature": "INDUS", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "358", "mean": "10.84", "stddev": "6.77", "min": "0.46", "max": "27.74", "missing": "0"}, "transformation": ""}, {"feature": "CHAS", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "358", "mean": "0.07", "stddev": "0.26", "min": "0.0", "max": "1.0", "missing": "0"}, "transformation": ""}, {"feature": "NOX", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "358", "mean": "0.55", "stddev": "0.12", "min": "0.385", "max": "0.871", "missing": "0"}, "transformation": ""}, {"feature": "RM", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "358", "mean": "6.3", "stddev": "0.72", "min": "3.561", "max": "8.78", "missing": "0"}, "transformation": ""}, {"feature": "AGE", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "358", "mean": "68.13", "stddev": "28.57", "min": "2.9", "max": "100.0", "missing": "0"}, "transformation": ""}, {"feature": "DIS", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "358", "mean": "3.9", "stddev": "2.17", "min": "1.137", "max": "12.1265", "missing": "0"}, "transformation": ""}, {"feature": "RAD", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "358", "mean": "9.12", "stddev": "8.44", "min": "1.0", "max": "24.0", "missing": "0"}, "transformation": ""}, {"feature": "TAX", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "358", "mean": "403.46", "stddev": "163.18", "min": "187.0", "max": "711.0", "missing": "0"}, "transformation": ""}, {"feature": "PTRATIO", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "358", "mean": "18.34", "stddev": "2.19", "min": "12.6", "max": "22.0", "missing": "0"}, "transformation": ""}, {"feature": "B", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "358", "mean": "359.35", "stddev": "87.25", "min": "0.32", "max": "396.9", "missing": "0"}, "transformation": ""}, {"feature": "LSTAT", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "358", "mean": "12.6", "stddev": "7.26", "min": "1.92", "max": "37.97", "missing": "0"}, "transformation": ""}, {"feature": "MEDV", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "358", "mean": "22.73", "stddev": "9.29", "min": "5.0", "max": "50.0", "missing": "0"}, "transformation": ""}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e6b26cfbe588d7717182f3b','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e6b26cfbe588d7717182f3b','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e6b26cfbe588d7717182f3c','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
	BostonHousingPrices_AutoML = tpot_execution.Tpot_execution.run(["5e6b26cfbe588d7717182f3b"],{"5e6b26cfbe588d7717182f3b": BostonHousingPrices_AutoFE}, "5e6b26cfbe588d7717182f3c", spark,json.dumps( {"model_type": "regression", "label": "MEDV", "features": ["CRIM", "ZN", "INDUS", "CHAS", "NOX", "RM", "AGE", "DIS", "RAD", "TAX", "PTRATIO", "B", "LSTAT"], "percentage": "90", "executionTime": "5", "sampling": "0", "sampling_value": "", "run_id": "", "model_id": "5e6b5957be588d77171831e1", "ProjectName": "ML Sample Problems", "PipelineName": "BostonHousingPrices", "pipelineId": "5e6b26cfbe588d7717182f39", "userid": "5df78f4be2f2eff24740bbd7", "runid": "", "url_ResultView": "http://13.68.212.36:3200", "experiment_id": "480623611921769"}))

	PipelineNotification.PipelineNotification().completed_notification('5e6b26cfbe588d7717182f3c','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e6b26cfbe588d7717182f3c','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)

