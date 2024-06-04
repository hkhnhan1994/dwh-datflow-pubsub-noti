
from apache_beam.transforms.sql import SqlTransform
import apache_beam as beam
from .functions import load_to_BQ

class transformation(beam.PTransform):
    def __init__(self, transformation_info:dict):
        self.transformation_info=transformation_info
    def expand(self,pcoll):
        # read content from yaml file
        transform=(
            pcoll
            |f"transformation for {self.transformation_info['TABLE']}">>SqlTransform(self.transformation_info['QUERY'])
        )
        to_BQ = (transform
                |f"load {self.transformation_info['TABLE']} to BQ"
                    >> load_to_BQ(self.transformation_info)
                )
        return transform