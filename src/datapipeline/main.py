# import os
import sys
import re
from src.datapipeline.pipelines.animals_events.animal_events import (
    main as animal_events_pipeline,
)

pipeline_params = sys.argv


def main():
    pipeline_name: str = None
    if len(pipeline_params) > 0:
        for param in pipeline_params:
            if (pipeline_name := re.search(r"(?<=--name=).*", param)) is not None:
                pipeline_name = pipeline_name.group(0)
                break
        if pipeline_name == "load_animals_events_pipelines":
            print(f"Starting Data Pipeline {pipeline_name}")
            animal_events_pipeline()
