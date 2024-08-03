from healthcare_pipeline.pipelines.data_processing import pipeline as data_processing_pipeline
from healthcare_pipeline.pipelines.analysis import pipeline as analysis_pipeline

def register_pipelines():
    return {
        "data_processing": data_processing_pipeline.create_pipeline(),
        "data_analysis": analysis_pipeline.create_pipeline(),
        "__default__": data_processing_pipeline.create_pipeline() + analysis_pipeline.create_pipeline(),
    }
