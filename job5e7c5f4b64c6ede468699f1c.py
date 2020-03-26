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
from clustering.ClusteringMain import Clustering
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e7c5f4b64c6ede468699f1d','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify')
	visualTest_DBFS = DBFSConnector.DBFSConnector.fetch([], {}, "5e7c5f4b64c6ede468699f1d", spark, "{'url': '/clustering/IrisTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapid40af94a6c7d8d818acf548df4c773f8', 'dbfs_domain': 'eastus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e7c5f4b64c6ede468699f1d','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e7c5f4b64c6ede468699f1d','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify','http://137.116.116.173:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e7c5f4b64c6ede468699f1e','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify')
	visualTest_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e7c5f4b64c6ede468699f1d"],{"5e7c5f4b64c6ede468699f1d": visualTest_DBFS}, "5e7c5f4b64c6ede468699f1e", spark,json.dumps( {"FE": [{"transformationsData": {}, "feature": "SepalLengthCm", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "16", "mean": "5.72", "stddev": "0.79", "min": "4.6", "max": "7.1", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "SepalWidthCm", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "16", "mean": "3.15", "stddev": "0.39", "min": "2.4", "max": "3.8", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "PetalLengthCm", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "16", "mean": "3.43", "stddev": "1.87", "min": "1.3", "max": "5.9", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "PetalWidthCm", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "16", "mean": "1.09", "stddev": "0.8", "min": "0.2", "max": "2.3", "missing": "0"}, "transformation": ""}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e7c5f4b64c6ede468699f1e','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e7c5f4b64c6ede468699f1e','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify','http://137.116.116.173:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e7c5f7564c6ede468699f20','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify')
	visualTest_Cluster = Clustering.run(["5e7c5f4b64c6ede468699f1e"],{"5e7c5f4b64c6ede468699f1e": visualTest_AutoFE}, "5e7c5f7564c6ede468699f20", spark,json.dumps( {"autoClustering": 1, "defaultclusters": 1, "model": "Bisecting KMeans", "run_id": "492b1ad402c449cab1951c3234385673", "ProjectName": "ClusteringTest", "PipelineName": "visualTest", "pipelineId": "5e7c5f4b64c6ede468699f1c", "userid": "5e1eb97a7d1a8956f654a15f", "runid": "", "url_ResultView": "http://137.116.116.173:3200", "experiment_id": "2162989224512177"}))

	PipelineNotification.PipelineNotification().completed_notification('5e7c5f7564c6ede468699f20','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e7c5f7564c6ede468699f20','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify','http://137.116.116.173:3200/logs/getProductLogs')
	sys.exit(1)

